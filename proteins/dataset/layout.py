"""The protein dataset: the Parquet layout, the rule for which files make up a day, and atomic writes.

This module is the contract of the protein dataset files. It does not import Django, so that code
outside emgapi-v2 (a protein release, the legacy import) can read and write the same layout.
"""

import os
import re
import tempfile
from datetime import date
from pathlib import Path
from typing import Iterable, Iterator

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

HASH = pa.binary(32)


def _schema(*fields):
    return pa.schema(
        [pa.field(name, type, nullable=nullable) for name, type, nullable in fields]
    )


SCHEMAS = {
    "protein": _schema(
        ("id", pa.int64(), False),
        ("hash", HASH, False),
        ("sequence", pa.string(), False),
    ),
    "occurrence": _schema(
        ("id", pa.int64(), False),
        ("protein_id", pa.int64(), False),
        ("contig_id", pa.int64(), False),
        ("assembly_proteins_id", pa.int32(), False),
        ("gene_caller_id", pa.int16(), False),
        ("start_position", pa.int32(), False),
        ("end_position", pa.int32(), False),
        ("strand", pa.int8(), False),
        ("truncation", pa.string(), True),
    ),
    "contig": _schema(
        ("id", pa.int64(), False),
        ("assembly_proteins_id", pa.int32(), False),
        ("name", pa.string(), False),
        ("original_name", pa.string(), True),
        ("length", pa.int32(), False),
        ("hash", HASH, False),
        ("kmer_coverage", pa.float64(), True),
    ),
    # What emgapi-v2 knows of each assembly proteins record, through its MGYA.
    "metadata/assembly_proteins": _schema(
        ("assembly_proteins_id", pa.int32(), False),
        ("assembly_accession", pa.string(), False),
        ("source", pa.string(), False),  # assembly_analysis or gene_calling
        ("pipeline_version", pa.string(), False),
        ("analysis_accession", pa.string(), False),
        ("study_accession", pa.string(), False),
        ("biome_lineage", pa.string(), True),  # NULL when the sample has no biome
        ("is_private", pa.bool_(), False),
        ("is_suppressed", pa.bool_(), False),
    ),
    "metadata/gene_callers": _schema(
        ("id", pa.int16(), False),
        ("name", pa.string(), False),
        ("version", pa.string(), False),
    ),
}

SORT_ORDER = {
    "protein": ["hash"],
    "occurrence": ["assembly_proteins_id", "contig_id", "start_position"],
    "contig": ["assembly_proteins_id", "id"],
    "metadata/assembly_proteins": ["assembly_proteins_id"],
    "metadata/gene_callers": ["id"],
}

ROW_GROUP_SIZE = 122_880
DATED = re.compile(r"(base|day)-(\d{4}-\d{2}-\d{2})\.parquet")


class DatasetError(Exception):
    """The protein dataset cannot be read as of the requested day."""


def prefix(hash: bytes) -> str:
    """The partition of a hash: its top 6 bits as two hex digits, as in the MGYP database.

    :param hash: A protein's 32-byte hash.
    """
    return f"{hash[0] >> 2:02x}"


def complete_days(root: Path) -> list[date]:
    """The days whose metadata snapshot is in place, the last write of a dataset update.

    :param root: The protein dataset's directory.
    """
    return sorted(
        date.fromisoformat(path.name.removeprefix("day="))
        for path in (root / "metadata").glob("day=*")
    )


def days_with_files(root: Path) -> set[date]:
    """The days that have any file in the protein dataset, complete or not.

    :param root: The protein dataset's directory.
    """
    names = [
        *(p.name.removeprefix("day-")[:10] for p in root.glob("protein/*/day-*")),
        *(p.name.removeprefix("day=") for p in root.glob("*/day=*")),
        *(p.name.removeprefix(".tmp-day=") for p in root.glob("metadata/.tmp-day=*")),
    ]
    return {date.fromisoformat(name) for name in names}


def files(root: Path, table: str, as_of: date | None = None) -> list[Path]:
    """The files that make up `table` as of a complete day, by default the latest one.

    :param root: The protein dataset's directory.
    :param table: The table, e.g. protein or metadata/assembly_proteins.
    :param as_of: A complete day, by default the latest.
    """
    if table not in SCHEMAS:
        raise ValueError(f"unknown protein dataset table {table!r}")
    complete_days_ = complete_days(root)
    if as_of is None:
        if not complete_days_:
            raise DatasetError("The protein dataset has no complete day")
        as_of = complete_days_[-1]
    elif as_of not in complete_days_:
        raise DatasetError(f"{as_of} is not a complete day of the protein dataset")
    complete = set(complete_days_)

    if table.startswith("metadata/"):
        return [
            root
            / "metadata"
            / f"day={as_of}"
            / f"{table.removeprefix('metadata/')}.parquet"
        ]

    if table != "protein":
        return [
            file
            for directory in sorted((root / table).glob("day=*"))
            if (day := date.fromisoformat(directory.name.removeprefix("day="))) <= as_of
            and day in complete
            for file in sorted(directory.glob("*.parquet"))
        ]

    selected = []
    for directory in sorted((root / "protein").glob("prefix=*")):
        bases, days = {}, {}
        for path in directory.glob("*.parquet"):
            if match := DATED.fullmatch(path.name):
                kind, day = match[1], date.fromisoformat(match[2])
                (bases if kind == "base" else days)[day] = path
        base = max((day for day in bases if day <= as_of), default=None)
        if base is None and bases:
            raise DatasetError(
                f"{as_of} is older than the oldest base of {directory.name}, "
                "and the merge has deleted the files it needs"
            )
        if base is not None:
            selected.append(bases[base])
        selected += [
            days[day]
            for day in sorted(days)
            if (base is None or base < day) and day <= as_of and day in complete
        ]
    return selected


def write(path: Path, table: str, data: pa.Table) -> None:
    """Writes `data` to `path` in the table's schema and sort order, all or nothing.

    :param path: The file to write.
    :param table: The table whose schema and sort order the file has.
    :param data: The rows.
    """
    schema = SCHEMAS[table]
    data = (
        data.select(schema.names)
        .cast(schema)
        .sort_by([(column, "ascending") for column in SORT_ORDER[table]])
        .combine_chunks()
    )
    write_sorted(path, table, data.to_batches(max_chunksize=ROW_GROUP_SIZE))


def write_sorted(path: Path, table: str, batches: Iterable[pa.RecordBatch]) -> None:
    """Writes batches already in the table's sort order, each one a row group, all or nothing.

    :param path: The file to write.
    :param table: The table whose schema the file has.
    :param batches: The rows, in the table's sort order.
    """
    schema = SCHEMAS[table]
    batches = (
        pa.Table.from_batches([batch]).select(schema.names).cast(schema)
        for batch in batches
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "wb") as f:
        with pq.ParquetWriter(
            f,
            schema,
            compression="zstd",
            compression_level=1,
            write_statistics=True,
        ) as writer:
            for batch in batches:
                writer.write_table(batch, row_group_size=ROW_GROUP_SIZE)
        f.flush()
        os.fsync(f.fileno())
    tmp.rename(path)


def merge(path: Path, table: str, files: list[Path], memory_limit: str) -> None:
    """Writes the rows of `files` to `path` in the table's sort order, spilling to disk, all or nothing.

    :param path: The file to write.
    :param table: The table whose schema and sort order the file has.
    :param files: The files to merge.
    :param memory_limit: The memory limit of DuckDB, for example "32GB".
    """
    write_sorted(path, table, sorted_batches(table, files, memory_limit))


def sorted_batches(
    table: str, files: list[Path], memory_limit: str
) -> Iterator[pa.RecordBatch]:
    """The rows of `files` in the table's sort order, sorted by DuckDB, which spills to disk.

    :param table: The table whose columns and sort order the rows have.
    :param files: The files to read.
    :param memory_limit: The memory limit of DuckDB, for example "32GB". DuckDB goes over it
        while it sorts, so use about half of the job memory.
    """
    spill = Path(tempfile.gettempdir()) / "protein-dataset-sort"
    config = {"temp_directory": str(spill), "memory_limit": memory_limit}
    with duckdb.connect(config=config) as con:
        yield from con.execute(
            f"SELECT {', '.join(SCHEMAS[table].names)}"
            " FROM read_parquet($files, hive_partitioning = false)"
            f" ORDER BY {', '.join(SORT_ORDER[table])}",
            {"files": [str(f) for f in files]},
        ).to_arrow_reader(ROW_GROUP_SIZE)
