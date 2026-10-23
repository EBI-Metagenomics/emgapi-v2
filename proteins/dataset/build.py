"""Building the MGYP database from the protein dataset: once after the migration, and to recover a lost MGYP database."""

import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from time import monotonic

import duckdb
import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from proteins.dataset import layout

logger = logging.getLogger(__name__)

# Every table but dataset_owner, which always has its one row.
TABLES = (
    "protein",
    "assembly_proteins",
    "gene_caller",
    "staged_protein",
    "staged_contig",
    "staged_occurrence",
    "dataset_update",
)
REGISTRIES = {
    "assembly_proteins": (
        "metadata/assembly_proteins_registry",
        {
            "id": "id",
            "assembly_accession": "assembly_accession",
            "source": "source",
            "pipeline_version": "pipeline_version",
            "analysis_accession": "analysis_accession",
        },
    ),
    "gene_caller": (
        "metadata/gene_callers",
        {"id": "id", "name": "name", "version": "version"},
    ),
}
SEQUENCES = {
    "protein": "protein_id_seq",
    "contig": "contig_id_seq",
    "occurrence": "occurrence_id_seq",
}
# MGYPs that a lost MGYP database gave are in no file, but could have been seen. By default a rebuild
# skips this many ids past the protein dataset's highest: about 6 days at the heaviest projected rate.
LOST_MGYP_MARGIN = 10**8
# PostgreSQL's binary COPY format, big-endian: a row is its field count, then each field's length and value.
COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + bytes(8)
COPY_TRAILER = b"\xff\xff"
COPY_ROW = np.dtype(
    [
        ("fields", ">i2"),
        ("hash_length", ">i4"),
        ("hash", "S32"),
        ("id_length", ">i4"),
        ("id", ">i8"),
    ]
)


class BuildError(Exception):
    """The MGYP database or the protein dataset does not allow the build."""


def build_mgyp_database(
    dsn: str, root: Path, rebuild: bool = False, protein_id_start: int | None = None
) -> date:
    """Builds the MGYP database from the latest complete day of the protein dataset, into a schema just created by mgyp_schema.sql.

    Without `rebuild`, this follows the migration, and the protein dataset must hold its day only.
    Returns the day built from.

    :param dsn: libpq connection string of the MGYP database, as proteindb_owner.
    :param root: The protein dataset's directory.
    :param rebuild: To recover a lost MGYP database.
    :param protein_id_start: The first protein id to allocate. By default, the protein dataset's
        highest + 1, or for a rebuild + LOST_MGYP_MARGIN.
    """
    days = layout.complete_days(root)
    if not rebuild and len(days) > 1:
        raise BuildError(
            f"The protein dataset has {len(days)} complete days, so this is a rebuild, which needs --rebuild"
        )
    day = days[-1]
    files = {table: layout.files(root, table, day) for table in SEQUENCES}
    top = {table: max_id(files[table]) for table in SEQUENCES}
    if protein_id_start is None:
        protein_id_start = top["protein"] + (LOST_MGYP_MARGIN if rebuild else 1)
    elif protein_id_start <= top["protein"]:
        raise BuildError(
            f"--protein-id-start {protein_id_start} is not above the protein dataset's highest protein id, {top['protein']}"
        )
    logger.info(f"protein ids start at {protein_id_start}")

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET search_path = proteindb")
        check_empty(conn)
        # Before the hours of the partitions, so that their unique constraints fail early.
        with conn.transaction():
            for name, (table, columns) in REGISTRIES.items():
                (path,) = layout.files(root, table, day)
                rows = pq.read_table(path).select(list(columns.values()))
                with conn.cursor().copy(
                    f"COPY {name} ({', '.join(columns)}) FROM STDIN"
                ) as copy:
                    for row in zip(*rows.to_pydict().values()):
                        copy.write_row(row)
        by_prefix = defaultdict(list)
        for path in files["protein"]:
            by_prefix[int(path.parent.name.removeprefix("prefix="), 16)].append(path)
        for p in range(64):
            build_partition(conn, p, by_prefix[p])
        for name in REGISTRIES:
            (start,) = conn.execute(
                f"SELECT coalesce(max(id), 0) + 1 FROM {name}"
            ).fetchone()
            conn.execute(f"ALTER TABLE {name} ALTER COLUMN id RESTART WITH {start}")
        for table, sequence in SEQUENCES.items():
            start = protein_id_start if table == "protein" else top[table] + 1
            conn.execute("SELECT setval(%s, %s, false)", [sequence, start])

        logger.info("vacuuming protein")
        conn.execute("VACUUM (ANALYZE) protein")
        # The dataset update refuses to run while a complete day is not done.
        conn.cursor().executemany(
            "INSERT INTO dataset_update (day, status, finished_at, message)"
            " VALUES (%s, 'done', now(), 'built from the protein dataset')",
            [[d] for d in days],
        )
    return day


def max_id(files: list[Path]) -> int:
    """The highest id in the files, or 0 if there are none.

    :param files: Parquet files with an id column.
    """
    if not files:
        return 0
    with duckdb.connect() as con:
        (top,) = con.execute(
            "SELECT coalesce(max(id), 0) FROM read_parquet($files)",
            {"files": [str(f) for f in files]},
        ).fetchone()
    return top


def check_empty(conn: psycopg.Connection) -> None:
    """An interrupted build is not resumed, so it must start from the schema mgyp_schema.sql creates.

    :param conn: A connection to the MGYP database, as proteindb_owner.
    """
    (partitions,) = conn.execute(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'protein'::regclass"
    ).fetchone()
    if partitions != 64:
        raise BuildError(
            f"protein has {partitions} partitions: drop the schema and apply mgyp_schema.sql again"
        )
    filled = [
        table
        for table in TABLES
        if conn.execute(f"SELECT EXISTS (SELECT FROM {table})").fetchone()[0]
    ]
    if filled:
        raise BuildError(
            f"{', '.join(filled)} not empty: drop the schema and apply mgyp_schema.sql again"
        )


def copy_rows(batch: pa.RecordBatch) -> bytes:
    """A batch of (hash, id) in the binary COPY format. Every row has the same width, so numpy lays them out.

    :param batch: Rows with a 32-byte hash and an id, neither NULL.
    """
    hashes = batch.column("hash")
    rows = np.empty(len(batch), COPY_ROW)
    rows["fields"], rows["hash_length"], rows["id_length"] = 2, 32, 8
    rows["hash"] = np.frombuffer(
        hashes.buffers()[1], "S32", len(batch), hashes.offset * 32
    )
    rows["id"] = batch.column("id").to_numpy()
    return rows.tobytes()


def build_partition(conn: psycopg.Connection, p: int, files: list[Path]) -> None:
    """Replaces an empty partition with one loaded before its index is built, which is faster.

    :param conn: A connection to the MGYP database, as proteindb_owner.
    :param p: The partition, 0 to 63.
    :param files: The prefix's protein files.
    """
    started = monotonic()
    name = f"protein_{p:02x}"
    low = "MINVALUE" if p == 0 else f"'\\x{4 * p:02x}'::bytea"
    high = "MAXVALUE" if p == 63 else f"'\\x{4 * (p + 1):02x}'::bytea"
    in_range = " AND ".join(
        [
            *([f"hash >= {low}"] if p > 0 else []),
            *([f"hash < {high}"] if p < 63 else []),
        ]
    )
    conn.execute(f"DROP TABLE {name}")
    # The partition's CHECK constraints are required to attach it, and the range one spares ATTACH its scan.
    conn.execute(
        f"""CREATE TABLE {name} (LIKE protein INCLUDING CONSTRAINTS,
                CONSTRAINT {name}_range CHECK ({in_range}))
            WITH (autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 100000)"""
    )
    rows = 0
    with conn.cursor().copy(
        f"COPY {name} (hash, id) FROM STDIN (FORMAT BINARY)"
    ) as copy:
        copy.write(COPY_HEADER)
        for path in files:
            for batch in pq.ParquetFile(path).iter_batches(columns=["hash", "id"]):
                copy.write(copy_rows(batch))
                rows += len(batch)
        copy.write(COPY_TRAILER)
    conn.execute(f"ALTER TABLE {name} ADD PRIMARY KEY (hash) INCLUDE (id)")
    conn.execute(
        f"ALTER TABLE protein ATTACH PARTITION {name} FOR VALUES FROM ({low}) TO ({high})"
    )
    conn.execute(f"ALTER TABLE {name} DROP CONSTRAINT {name}_range")
    conn.execute(f"GRANT SELECT ON {name} TO proteindb_accession, proteindb_read")
    logger.info("%s: %d rows in %.0f s", name, rows, monotonic() - started)
