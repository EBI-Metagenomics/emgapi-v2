"""The migration's Tier 1 build, and the rebuild of Tier 1 from Tier 2 after it is lost."""

import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from time import monotonic

import adbc_driver_postgresql.dbapi as adbc
import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from proteins import tier2

logger = logging.getLogger(__name__)

# Everything but tier2_owner, which always has its one row.
TABLES = (
    "protein_key",
    "assembly",
    "study",
    "biome",
    "gene_caller",
    "staging_protein",
    "staging_contig",
    "staging_occurrence",
    "load_log",
)
REGISTRIES = {
    "assembly": ["id", "accession", "pipeline_version"],
    "study": ["id", "accession"],
    "biome": ["id", "lineage"],
    "gene_caller": ["id", "name", "version"],
}
SEQUENCES = {
    "protein": "protein_id_seq",
    "contig": "contig_id_seq",
    "occurrence": "occurrence_id_seq",
}


class MigrateError(Exception): ...


def build_tier1(dsn: str, root: Path, protein_id_start: int | None = None) -> date:
    """Builds Tier 1 from the latest complete day of Tier 2, into a schema just created by tier1.sql.

    Without `protein_id_start`, this is the migration, and Tier 2 must hold its day only. With it,
    it is a rebuild, and protein ids start there. Returns the day built from.
    """
    days = tier2.complete_days(root)
    if not days:
        raise MigrateError("Tier 2 has no complete day")
    if protein_id_start is None and len(days) > 1:
        raise MigrateError(
            f"Tier 2 has {len(days)} complete days, so this is a rebuild, which needs --protein-id-start"
        )
    day = days[-1]
    files = {table: tier2.files(root, table, day) for table in SEQUENCES}
    top = {table: max_id(files[table]) for table in SEQUENCES}
    if protein_id_start is not None and protein_id_start <= top["protein"]:
        raise MigrateError(
            f"--protein-id-start {protein_id_start} is not above Tier 2's highest protein id, {top['protein']}"
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET search_path = proteindb")
        check_empty(conn)
        by_prefix = defaultdict(list)
        for path in files["protein"]:
            by_prefix[int(path.parent.name.removeprefix("prefix="), 16)].append(path)
        for p in range(64):
            build_partition(conn, dsn, p, by_prefix[p])

        with adbc.connect(dsn) as registries, registries.cursor() as cursor:
            for name, columns in REGISTRIES.items():
                rows = pq.read_table(
                    tier2.files(root, f"dims/{name}", day)[0], columns=columns
                )
                cursor.adbc_ingest(
                    name, rows, mode="append", db_schema_name="proteindb"
                )
            registries.commit()
        for name in REGISTRIES:
            (start,) = conn.execute(
                f"SELECT coalesce(max(id), 0) + 1 FROM {name}"
            ).fetchone()
            conn.execute(f"ALTER TABLE {name} ALTER COLUMN id RESTART WITH {start}")
        for table, sequence in SEQUENCES.items():
            start = top[table] + 1
            if table == "protein" and protein_id_start is not None:
                start = protein_id_start
            conn.execute("SELECT setval(%s, %s, false)", [sequence, start])

        logger.info("vacuuming protein_key")
        conn.execute("VACUUM (ANALYZE) protein_key")
        conn.execute(
            "INSERT INTO load_log (ingest_date, status, finished_at, message)"
            " VALUES (%s, 'done', now(), 'built by proteindb_migrate tier1')",
            [day],
        )
    return day


def max_id(files: list[Path]) -> int:
    if not files:
        return 0
    with duckdb.connect() as con:
        (top,) = con.execute(
            "SELECT coalesce(max(id), 0) FROM read_parquet($files)",
            {"files": [str(f) for f in files]},
        ).fetchone()
    return top


def check_empty(conn: psycopg.Connection) -> None:
    """An interrupted build is not resumed, so it must start from the schema tier1.sql creates."""
    (partitions,) = conn.execute(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'protein_key'::regclass"
    ).fetchone()
    if partitions != 64:
        raise MigrateError(
            f"protein_key has {partitions} partitions: drop the schema and apply tier1.sql again"
        )
    filled = [
        table
        for table in TABLES
        if conn.execute(f"SELECT EXISTS (SELECT FROM {table})").fetchone()[0]
    ]
    if filled:
        raise MigrateError(
            f"{', '.join(filled)} not empty: drop the schema and apply tier1.sql again"
        )


def build_partition(
    conn: psycopg.Connection, dsn: str, p: int, files: list[Path]
) -> None:
    """Replaces an empty partition with one loaded before its index is built, which is faster."""
    started = monotonic()
    name = f"protein_key_{p:02x}"
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
        f"""CREATE TABLE {name} (LIKE protein_key INCLUDING CONSTRAINTS,
                CONSTRAINT {name}_range CHECK ({in_range}))
            WITH (autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 100000)"""
    )
    rows = 0
    if files:
        batches = (
            batch.select(["hash", "id"])
            for path in files
            for batch in pq.ParquetFile(path).iter_batches(columns=["hash", "id"])
        )
        schema = pa.schema([tier2.SCHEMAS["protein"].field(c) for c in ("hash", "id")])
        with adbc.connect(dsn) as load, load.cursor() as cursor:
            rows = cursor.adbc_ingest(
                name,
                pa.RecordBatchReader.from_batches(schema, batches),
                mode="append",
                db_schema_name="proteindb",
            )
            load.commit()
    conn.execute(f"ALTER TABLE {name} ADD PRIMARY KEY (hash) INCLUDE (id)")
    conn.execute(
        f"ALTER TABLE protein_key ATTACH PARTITION {name} FOR VALUES FROM ({low}) TO ({high})"
    )
    conn.execute(f"ALTER TABLE {name} DROP CONSTRAINT {name}_range")
    conn.execute(f"GRANT SELECT ON {name} TO proteindb_accession, proteindb_read")
    logger.info("%s: %d rows in %.0f s", name, rows, monotonic() - started)
