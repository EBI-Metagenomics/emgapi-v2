"""Tier 2: the Parquet layout, the rule for which files make up a day, and atomic writes."""

import os
import re
from datetime import date
from pathlib import Path

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
        ("assembly_id", pa.int32(), False),
        ("gene_caller_id", pa.int16(), False),
        ("start_position", pa.int32(), False),
        ("end_position", pa.int32(), False),
        ("strand", pa.int8(), False),
        ("truncation", pa.string(), True),
    ),
    "contig": _schema(
        ("id", pa.int64(), False),
        ("assembly_id", pa.int32(), False),
        ("name", pa.string(), False),
        ("original_name", pa.string(), True),
        ("length", pa.int32(), False),
        ("hash", HASH, False),
        ("kmer_coverage", pa.float64(), True),
    ),
    # study, biome and the flags are NULL when emgapi-v2 does not know the accession.
    "dims/assembly": _schema(
        ("id", pa.int32(), False),
        ("accession", pa.string(), False),
        ("pipeline_version", pa.string(), False),
        ("study_id", pa.int32(), True),
        ("biome_id", pa.int16(), True),
        ("private", pa.bool_(), True),
        ("suppressed", pa.bool_(), True),
    ),
    "dims/study": _schema(
        ("id", pa.int32(), False),
        ("accession", pa.string(), False),
        ("private", pa.bool_(), False),
        ("suppressed", pa.bool_(), False),
    ),
    "dims/biome": _schema(("id", pa.int16(), False), ("lineage", pa.string(), False)),
    "dims/gene_caller": _schema(
        ("id", pa.int16(), False),
        ("name", pa.string(), False),
        ("version", pa.string(), False),
    ),
}

SORT_ORDER = {
    "protein": ["hash"],
    "occurrence": ["assembly_id", "contig_id", "start_position"],
    "contig": ["assembly_id", "id"],
    "dims/assembly": ["id"],
    "dims/study": ["id"],
    "dims/biome": ["id"],
    "dims/gene_caller": ["id"],
}

ROW_GROUP_SIZE = 122_880
DATED = re.compile(r"(base|part)-(\d{4}-\d{2}-\d{2})\.parquet")


class Tier2Error(Exception): ...


def prefix(hash: bytes) -> str:
    """The partition of a hash: its top 6 bits as two hex digits, as in Tier 1."""
    return f"{hash[0] >> 2:02x}"


def complete_days(root: Path) -> list[date]:
    """The days whose snapshot of the dimensions has been published, the last write of a load."""
    return sorted(
        date.fromisoformat(path.name.removeprefix("snapshot="))
        for path in (root / "dims").glob("snapshot=*")
    )


def files(root: Path, table: str, as_of: date | None = None) -> list[Path]:
    """The files that make up `table` as of a complete day, by default the latest one."""
    if table not in SCHEMAS:
        raise ValueError(f"unknown Tier 2 table {table!r}")
    days = complete_days(root)
    if as_of is None:
        if not days:
            raise Tier2Error("Tier 2 has no complete day")
        as_of = days[-1]
    elif as_of not in days:
        raise Tier2Error(f"{as_of} is not a complete day of Tier 2")
    complete = set(days)

    if table.startswith("dims/"):
        return [
            root
            / "dims"
            / f"snapshot={as_of}"
            / f"{table.removeprefix('dims/')}.parquet"
        ]

    if table != "protein":
        return [
            file
            for directory in sorted((root / table).glob("ingest_date=*"))
            if (day := date.fromisoformat(directory.name.removeprefix("ingest_date=")))
            <= as_of
            and day in complete
            for file in sorted(directory.glob("*.parquet"))
        ]

    selected = []
    for directory in sorted((root / "protein").glob("prefix=*")):
        bases, parts = {}, {}
        for path in directory.glob("*.parquet"):
            if match := DATED.fullmatch(path.name):
                kind, day = match[1], date.fromisoformat(match[2])
                (bases if kind == "base" else parts)[day] = path
        base = max((day for day in bases if day <= as_of), default=None)
        if base is None and bases:
            raise Tier2Error(
                f"{as_of} is older than the oldest base of {directory.name}, "
                "and compaction has deleted the files it needs"
            )
        if base is not None:
            selected.append(bases[base])
        selected += [
            parts[day]
            for day in sorted(parts)
            if (base is None or base < day) and day <= as_of and day in complete
        ]
    return selected


def write(path: Path, table: str, data: pa.Table) -> None:
    """Writes `data` to `path` in the table's schema and sort order, all or nothing."""
    schema = SCHEMAS[table]
    data = (
        data.select(schema.names)
        .cast(schema)
        .sort_by([(column, "ascending") for column in SORT_ORDER[table]])
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "wb") as f:
        pq.write_table(
            data,
            f,
            compression="zstd",
            compression_level=1,
            row_group_size=ROW_GROUP_SIZE,
            write_statistics=True,
        )
        f.flush()
        os.fsync(f.fileno())
    tmp.rename(path)
