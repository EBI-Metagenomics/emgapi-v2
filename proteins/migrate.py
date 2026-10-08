"""The migration from the current database: its export, the Tier 2 build, the Tier 1 build, also used to rebuild Tier 1 from Tier 2, and their verification, with the check of its assemblies against emgapi-v2."""

import logging
import shutil
import tempfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from time import monotonic

import adbc_driver_postgresql.dbapi as adbc
import duckdb
import psycopg
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from proteins import dims, tier2

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


# The frozen current database. Its three large tables are split into 64 children, <table>_hash_<n>.
SOURCE = "mgnprotein_ingestion"
EXPORTS = {
    "protein": (
        "protein",
        "SELECT id, h AS hash, sequence, lpad(to_hex(get_byte(h, 0) >> 2), 2, '0') AS prefix"
        " FROM {child}, LATERAL decode(sequence_sha256sum::text, 'hex') h",
    ),
    "occurrence": (
        "protein_contig_occurrence",
        "SELECT id, protein_id, contig_id, assembly_id, gene_caller_id, start_position,"
        " end_position, strand, truncation::text AS truncation FROM {child}",
    ),
    "contig": (
        "contig",
        "SELECT id, assembly_id, contig_name AS name, NULL::text AS original_name,"
        " contig_length AS length, decode(sequence_sha256sum::text, 'hex') AS hash,"
        " kmer_coverage FROM {child}",
    ),
}
DIMS = {
    "assembly": "SELECT id, accession, pipeline_version::text AS pipeline_version, study_id,"
    " biome_id, private, suppressed FROM {source}.assembly",
    "study": "SELECT id, accession, private, suppressed FROM {source}.study",
    "biome": "SELECT id, lineage FROM {source}.biome",
    "gene_caller": "SELECT id, gene_caller AS name, version FROM {source}.gene_caller",
}


class MigrateError(Exception):
    """A migration step's inputs are missing, invalid or already used."""


def export(source: str, out: Path, table: str, child: int) -> int:
    """Exports one child of a current table to out/<table>/child=<child>/ in Tier 2's types. Returns its rows.

    Proteins are split by prefix into prefix=XX/, because the current database partitions them
    on another function of the hash.

    :param source: libpq URI of the current database.
    :param out: The export directory.
    :param table: protein, occurrence or contig.
    :param child: The child table, 0 to 63.
    """
    name, select = EXPORTS[table]
    final = out / table / f"child={child}"
    if final.exists():
        raise MigrateError(f"{final} is already exported")
    tmp = out / table / f".child={child}.tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    schema = tier2.SCHEMAS[table]
    if table == "protein":
        schema = schema.append(pa.field("prefix", pa.string(), False))
    started, rows = monotonic(), 0

    def batches(reader):
        nonlocal rows
        for batch in reader:
            rows += batch.num_rows
            yield conform(batch, schema, table)

    with adbc.connect(source) as conn, conn.cursor() as cursor:
        cursor.execute(select.format(child=f"{SOURCE}.{name}_hash_{child}"))
        ds.write_dataset(
            batches(cursor.fetch_record_batch()),
            tmp,
            schema=schema,
            format="parquet",
            partitioning=["prefix"] if table == "protein" else None,
            partitioning_flavor="hive",
            basename_template="part-{i}.parquet",
            file_options=ds.ParquetFileFormat().make_write_options(
                compression="zstd", compression_level=1
            ),
            # The Tier 2 build rewrites the pieces, so their row groups are kept small, and with
            # them the buffers of the 64 prefixes.
            min_rows_per_group=16_384,
            max_rows_per_group=tier2.ROW_GROUP_SIZE,
        )
    tmp.rename(final)
    logger.info(
        "%s child %d: %d rows in %.0f s", table, child, rows, monotonic() - started
    )
    return rows


def export_dims(source: str, out: Path) -> None:
    """Exports the four reference tables to out/dims/, as Tier 2 snapshot files.

    :param source: libpq URI of the current database.
    :param out: The export directory.
    """
    with adbc.connect(source) as conn, conn.cursor() as cursor:
        for name, select in DIMS.items():
            cursor.execute(select.format(source=SOURCE))
            schema = tier2.SCHEMAS[f"dims/{name}"]
            rows = pa.Table.from_batches(
                [conform(b, schema, name) for b in cursor.fetch_record_batch()],
                schema,
            )
            tier2.write(out / "dims" / f"{name}.parquet", f"dims/{name}", rows)


def conform(batch: pa.RecordBatch, schema: pa.Schema, table: str) -> pa.RecordBatch:
    """The batch in Tier 2's types. A cast does not check nulls, so they are checked here.

    :param batch: Rows read from the current database.
    :param schema: The table's Tier 2 schema.
    :param table: The table, named in errors.
    """
    for field in schema:
        if not field.nullable and batch.column(field.name).null_count:
            raise MigrateError(
                f"{table}.{field.name} has NULLs, which Tier 2 does not allow"
            )
    return batch.select(schema.names).cast(schema)


def build_tier2(exported: Path, root: Path, day: date) -> None:
    """Builds Tier 2's first day, `day`, from a complete export. Resumes where an interrupted build stopped.

    :param exported: The export directory.
    :param root: Tier 2's directory.
    :param day: M, the date of the freeze.
    """
    if tier2.complete_days(root):
        raise MigrateError(f"Tier 2 in {root} already has a complete day")
    missing = [
        *(
            f"{table}/child={child}"
            for table in EXPORTS
            for child in range(64)
            if not (exported / table / f"child={child}").is_dir()
        ),
        *(
            f"dims/{name}.parquet"
            for name in DIMS
            if not (exported / "dims" / f"{name}.parquet").exists()
        ),
    ]
    if missing:
        raise MigrateError(
            f"the export is missing {len(missing)}: {', '.join(missing[:5])}"
        )

    # Each file is written all or nothing, so one that exists is complete.
    for p in range(64):
        base = root / "protein" / f"prefix={p:02x}" / f"base-{day}.parquet"
        pieces = sorted(exported.glob(f"protein/child=*/prefix={p:02x}/*.parquet"))
        if pieces and not base.exists():
            started = monotonic()
            tier2.merge(base, "protein", pieces)
            logger.info("%s: %.0f s", base, monotonic() - started)
    for table in ("occurrence", "contig"):
        for child in range(64):
            part = root / table / f"ingest_date={day}" / f"part-{child:03d}.parquet"
            pieces = sorted((exported / table / f"child={child}").glob("*.parquet"))
            if pieces and not part.exists():
                started = monotonic()
                tier2.merge(part, table, pieces)
                logger.info("%s: %.0f s", part, monotonic() - started)

    snapshot = root / "dims" / f".tmp-snapshot={day}"
    shutil.rmtree(snapshot, ignore_errors=True)
    for name in DIMS:
        tier2.write(
            snapshot / f"{name}.parquet",
            f"dims/{name}",
            pq.read_table(exported / "dims" / f"{name}.parquet"),
        )
    snapshot.rename(root / "dims" / f"snapshot={day}")


def build_tier1(dsn: str, root: Path, protein_id_start: int | None = None) -> date:
    """Builds Tier 1 from the latest complete day of Tier 2, into a schema just created by tier1.sql.

    Without `protein_id_start`, this is the migration, and Tier 2 must hold its day only. With it,
    it is a rebuild, and protein ids start there. Returns the day built from.

    :param dsn: libpq connection string of Tier 1, as proteindb_owner.
    :param root: Tier 2's directory.
    :param protein_id_start: For a rebuild, the first protein id to allocate.
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
        # The load requires every complete day of Tier 2 to be done in load_log.
        conn.cursor().executemany(
            "INSERT INTO load_log (ingest_date, status, finished_at, message)"
            " VALUES (%s, 'done', now(), 'built by proteindb_migrate tier1')",
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
    """An interrupted build is not resumed, so it must start from the schema tier1.sql creates.

    :param conn: A connection to Tier 1, as proteindb_owner.
    """
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
    """Replaces an empty partition with one loaded before its index is built, which is faster.

    :param conn: A connection to Tier 1, as proteindb_owner.
    :param dsn: Its libpq connection string, for the connection that copies.
    :param p: The partition, 0 to 63.
    :param files: The prefix's protein files.
    """
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


def verify(source: str, dsn: str, root: Path, sample: int = 1_000_000) -> list[str]:
    """The checks of the migration that compare the frozen current database, Tier 2 and Tier 1. Returns those that failed.

    The hash contract is checked on `sample` rows of each prefix.

    :param source: libpq URI of the current database.
    :param dsn: libpq connection string of Tier 1.
    :param root: Tier 2's directory.
    :param sample: The rows of each prefix whose hash is checked.
    """
    days = tier2.complete_days(root)
    if len(days) != 1:
        raise MigrateError(
            f"Tier 2 has {len(days)} complete days, and verify checks the migration's only"
        )
    failed = []
    with psycopg.connect(source) as conn:
        frozen = {
            table: conn.execute(f"SELECT count(*) FROM {SOURCE}.{name}").fetchone()[0]
            for table, (name, _) in EXPORTS.items()
        }
    with psycopg.connect(dsn) as conn:
        tier1 = {
            name.removeprefix("protein_key_"): (rows, int(total))
            for name, rows, total in conn.execute(
                "SELECT c.relname, count(*), sum(k.id) FROM proteindb.protein_key k"
                " JOIN pg_class c ON c.oid = k.tableoid GROUP BY c.relname"
            )
        }

    spill = Path(tempfile.gettempdir()) / "proteindb-verify"
    with duckdb.connect(config={"temp_directory": str(spill)}) as con:

        def one(sql, files):
            return con.execute(sql, {"files": [str(f) for f in files]}).fetchone()

        proteins = tier2.files(root, "protein")
        for table in EXPORTS:
            (rows,) = one(
                "SELECT count(*) FROM read_parquet($files)", tier2.files(root, table)
            )
            if rows != frozen[table]:
                failed.append(
                    f"Tier 2 {table} has {rows} rows, and the frozen table {frozen[table]}"
                )
        rows, distinct = one(
            "SELECT count(*), count(DISTINCT id) FROM read_parquet($files)", proteins
        )
        if distinct != rows:
            failed.append(
                f"Tier 2 protein has {rows} rows, but {distinct} distinct ids"
            )
        in_tier1 = sum(count for count, _ in tier1.values())
        if in_tier1 != rows:
            failed.append(f"protein_key has {in_tier1} rows, and Tier 2 protein {rows}")

        in_tier2 = {}
        for path in proteins:
            in_tier2[path.parent.name.removeprefix("prefix=")] = one(
                "SELECT count(*), sum(id) FROM read_parquet($files)", [path]
            )
            (broken,) = one(
                "SELECT count(*) FROM (SELECT hash, sequence FROM read_parquet($files)"
                f" USING SAMPLE reservoir({sample} ROWS))"
                " WHERE sha256(sequence) <> lower(hex(hash))",
                [path],
            )
            if broken:
                failed.append(
                    f"{path}: {broken} sampled rows whose hash is not sha256(sequence)"
                )
        for p in sorted(set(tier1) | set(in_tier2)):
            if tier1.get(p, (0, 0)) != in_tier2.get(p, (0, 0)):
                failed.append(
                    f"prefix {p}: (rows, sum of ids) {tier1.get(p, (0, 0))} in Tier 1,"
                    f" and {in_tier2.get(p, (0, 0))} in Tier 2"
                )
    return failed


def resolution(source: str) -> tuple[int, list[list[str]]]:
    """How many assembly accessions the current database has, and each of its assemblies that emgapi-v2 does not know, or knows with another study or biome.

    The first load after cutover takes the dimensions from emgapi-v2, so an unknown assembly would
    be left out of every release, and a changed one would be released with emgapi-v2's study and biome.
    Each is listed as its pipeline version and accession, then what differs.

    :param source: libpq URI of the current database.
    """
    with psycopg.connect(source) as conn:
        assemblies = conn.execute(
            f"SELECT DISTINCT a.pipeline_version::text, a.accession, s.accession, b.lineage"
            f" FROM {SOURCE}.assembly a LEFT JOIN {SOURCE}.study s ON s.id = a.study_id"
            f" LEFT JOIN {SOURCE}.biome b ON b.id = a.biome_id"
        ).fetchall()
    accessions = sorted({accession for _, accession, *_ in assemblies})
    known = {s.assembly_accession: s for s in dims.resolve(accessions)}
    differing = []
    for version, accession, study, lineage in sorted(assemblies):
        if (now := known.get(accession)) is None:
            differing.append([version, accession, "not in emgapi-v2"])
            continue
        changes = [
            f"{name} {was} -> {now_}"
            for name, was, now_ in [
                ("study", study, now.study_accession),
                ("biome", lineage, now.biome_lineage),
            ]
            if was != now_
        ]
        if changes:
            differing.append([version, accession, *changes])
    return len(accessions), differing
