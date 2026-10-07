"""The daily load: moves what is staged in Tier 1 into a new day of Tier 2."""

import logging
import shutil
import tempfile
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from time import monotonic
from typing import NamedTuple

import adbc_driver_postgresql.dbapi as adbc
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from proteins import tier2
from proteins.dims import Resolve, snapshot
from proteins.owner import Owner

logger = logging.getLogger(__name__)

STAGED_COLUMNS = {
    "protein": "id, hash, sequence, get_byte(hash, 0) >> 2 AS prefix",
    "contig": "id, assembly_id, name, original_name, length, hash, kmer_coverage",
    "occurrence": "id, protein_id, contig_id, assembly_id, gene_caller_id,"
    " start_position, end_position, strand, truncation",
}


# A prefix is compacted when its oldest file is older than this.
COMPACT_AFTER = timedelta(days=30)
# No prefix is started once the load has been running this long, so that the
# load ends within its 12 h limit and a release never waits longer for it.
COMPACTION_BUDGET = 8 * 3600


class LoadError(Exception): ...


class Staged(NamedTuple):
    assembly_ids: list[int]
    files: dict[str, Path]  # each staging table's rows, in the scratch directory
    assemblies: pa.Table  # the registries, read in the same snapshot as the rows
    gene_callers: pa.Table


def load(dsn: str, root: Path, day: date, resolve: Resolve, owner: Owner) -> int | None:
    """Loads everything staged as day `day`, as the owner of Tier 2. Returns its load_log id, or None if the day was already done."""
    compact_until = monotonic() + COMPACTION_BUDGET
    with psycopg.connect(dsn, autocommit=True) as conn:
        owner.check()
        recover(conn, root)
        if conn.execute(
            "SELECT 1 FROM load_log WHERE ingest_date = %s AND status = 'done'", [day]
        ).fetchone():
            logger.info("%s is already loaded", day)
            return None
        log_id = start(conn, day)
        committed = False
        try:
            with tempfile.TemporaryDirectory(prefix="proteindb-load-") as scratch:
                staged = read_staging(dsn, Path(scratch))
                dims, unresolved = snapshot(
                    conn, staged.assemblies, staged.gene_callers, resolve
                )
                owner.check()
                counts = write_day(root, day, staged, dims)
                commit(conn, owner, log_id, staged.assembly_ids, counts, unresolved)
                committed = True
            owner.check()
            publish(root, day)
        except Exception as e:
            if not committed:
                record_error(dsn, log_id, e, failed=True)
            raise
        logger.info("loaded %s: %s, %d unresolved assemblies", day, counts, unresolved)

        try:
            compacted = compact(root, day, compact_until, owner)
        except Exception as e:
            record_error(dsn, log_id, e, failed=False)
            raise
        conn.execute(
            "UPDATE load_log SET compacted = %s WHERE id = %s", [compacted, log_id]
        )
    return log_id


def recover(conn: psycopg.Connection, root: Path) -> None:
    """Step 1: publishes committed days left unpublished, and deletes what failed loads left."""
    done = {
        row[0]
        for row in conn.execute(
            "SELECT ingest_date FROM load_log WHERE status = 'done'"
        )
    }
    complete = set(tier2.complete_days(root))
    for day in sorted((days_with_files(root) | done) - complete):
        if day not in done:
            logger.warning("deleting the files of %s, left by a failed load", day)
            delete_day(root, day)
        elif (root / "dims" / f".tmp-snapshot={day}").exists():
            logger.warning("publishing %s, committed by a load that ended before", day)
            publish(root, day)
        else:
            raise LoadError(
                f"{day} is done in load_log, but Tier 2 has no snapshot of it, published or not"
            )
    for leftover in root.rglob(".*.tmp"):
        leftover.unlink()
    for prefix in (root / "protein").glob("prefix=*"):
        bases = sorted(prefix.glob("base-*.parquet"))
        if bases:
            newest = bases[-1].name.removeprefix("base-")
            for path in bases[:-1]:
                path.unlink()
            for path in prefix.glob("part-*.parquet"):
                if path.name.removeprefix("part-") <= newest:
                    path.unlink()


def days_with_files(root: Path) -> set[date]:
    names = [
        *(p.name.removeprefix("part-")[:10] for p in root.glob("protein/*/part-*")),
        *(p.name.removeprefix("ingest_date=") for p in root.glob("*/ingest_date=*")),
        *(p.name.removeprefix(".tmp-snapshot=") for p in root.glob("dims/.tmp-*")),
    ]
    return {date.fromisoformat(name) for name in names}


def delete_day(root: Path, day: date) -> None:
    for path in root.glob(f"protein/*/part-{day}.parquet"):
        path.unlink()
    for directory in (
        root / "occurrence" / f"ingest_date={day}",
        root / "contig" / f"ingest_date={day}",
        root / "dims" / f".tmp-snapshot={day}",
    ):
        shutil.rmtree(directory, ignore_errors=True)


def start(conn: psycopg.Connection, day: date) -> int:
    """Step 3. Only the owner of Tier 2 runs a load, so any other running load is dead."""
    with conn.transaction():
        conn.execute(
            "UPDATE load_log SET status = 'failed', finished_at = now(),"
            " message = 'ended without recording it' WHERE status = 'running'"
        )
        return conn.execute(
            "INSERT INTO load_log (ingest_date, status) VALUES (%s, 'running') RETURNING id",
            [day],
        ).fetchone()[0]


def read_staging(dsn: str, scratch: Path) -> Staged:
    """Steps 4 and 5, in one snapshot, so that the registries hold no analysis without its rows."""
    with adbc.connect(dsn, autocommit=True) as conn, conn.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        assembly_ids = choose_assemblies(cursor)
        # A read-only transaction cannot create a temporary table, so the ids are inlined.
        chosen = "'{%s}'::integer[]" % ",".join(map(str, assembly_ids))
        files = {}
        for table, columns in STAGED_COLUMNS.items():
            files[table] = scratch / f"{table}.parquet"
            cursor.execute(
                f"SELECT {columns} FROM staging_{table} WHERE assembly_id = ANY({chosen})"
            )
            reader = cursor.fetch_record_batch()
            with pq.ParquetWriter(files[table], reader.schema) as writer:
                for batch in reader:
                    writer.write_batch(batch)
        cursor.execute("SELECT id, accession, pipeline_version FROM assembly")
        assemblies = cursor.fetch_arrow_table()
        cursor.execute("SELECT id, name, version FROM gene_caller")
        gene_callers = cursor.fetch_arrow_table()
        cursor.execute("COMMIT")
    return Staged(assembly_ids, files, assemblies, gene_callers)


def choose_assemblies(cursor) -> list[int]:
    """Step 4: every analysis's rows are committed together and never staged again, so they are complete."""
    cursor.execute(
        "SELECT assembly_id FROM staging_protein"
        " UNION SELECT assembly_id FROM staging_contig"
        " UNION SELECT assembly_id FROM staging_occurrence"
    )
    return sorted(cursor.fetch_arrow_table()["assembly_id"].to_pylist())


def write_day(
    root: Path, day: date, staged: Staged, dims: dict[str, pa.Table]
) -> dict[str, int]:
    """Step 7. Returns the number of rows written to each table."""
    proteins = pq.read_table(staged.files["protein"])
    for prefix in sorted(set(proteins["prefix"].to_pylist())):
        tier2.write(
            root / "protein" / f"prefix={prefix:02x}" / f"part-{day}.parquet",
            "protein",
            proteins.filter(pc.equal(proteins["prefix"], prefix)),
        )
    counts = {"proteins": proteins.num_rows}
    for table in ("occurrence", "contig"):
        rows = pq.read_table(staged.files[table])
        tier2.write(
            root / table / f"ingest_date={day}" / "part-000.parquet", table, rows
        )
        counts[f"{table}s"] = rows.num_rows
    for name, table in dims.items():
        tier2.write(
            root / "dims" / f".tmp-snapshot={day}" / f"{name}.parquet",
            f"dims/{name}",
            table,
        )
    return counts


def commit(
    conn: psycopg.Connection,
    owner: Owner,
    log_id: int,
    assembly_ids: list[int],
    counts: dict[str, int],
    unresolved: int,
) -> None:
    """Step 8. From here on, the day's rows exist only in its Tier 2 files."""
    with conn.transaction():
        owner.confirm(conn)
        for table in ("occurrence", "contig", "protein"):
            conn.execute(
                f"DELETE FROM staging_{table} WHERE assembly_id = ANY(%s)",
                [assembly_ids],
            )
        updated = conn.execute(
            """
            UPDATE load_log SET status = 'done', finished_at = now(), assemblies = %s,
                proteins = %s, contigs = %s, occurrences = %s, unresolved_assemblies = %s
            WHERE id = %s AND status = 'running'
            """,
            [
                len(assembly_ids),
                counts["proteins"],
                counts["contigs"],
                counts["occurrences"],
                unresolved,
                log_id,
            ],
        )
        if updated.rowcount != 1:
            raise LoadError(f"load_log row {log_id} is no longer running")


def publish(root: Path, day: date) -> None:
    """Step 9: the day is complete once its snapshot is in place."""
    (root / "dims" / f".tmp-snapshot={day}").rename(root / "dims" / f"snapshot={day}")


def compact(root: Path, day: date, until: float, owner: Owner) -> bool:
    """Step 10: compacts each prefix that is due, starting none after `until`. Returns whether it compacted any."""
    by_prefix = defaultdict(list)
    for path in tier2.files(root, "protein", day):
        by_prefix[path.parent].append(path)
    due = [
        (prefix, files)
        for prefix, files in sorted(by_prefix.items())
        if any(f.name.startswith("part-") for f in files)
        and min(file_date(f) for f in files) < day - COMPACT_AFTER
    ]
    compacted = 0
    for prefix, files in due:
        if monotonic() >= until:
            logger.info(
                "compaction stopped at its time limit, %d prefixes wait for the next load",
                len(due) - compacted,
            )
            break
        owner.check()
        compact_prefix(prefix, files, day)
        compacted += 1
    return compacted > 0


def file_date(path: Path) -> date:
    return date.fromisoformat(path.stem.split("-", 1)[1])


def compact_prefix(prefix: Path, files: list[Path], day: date) -> None:
    """Merges a prefix's base and parts into base-`day`, then deletes them."""
    logger.info("compacting %s: %d files", prefix.name, len(files))
    base = prefix / f"base-{day}.parquet"
    tier2.merge(base, "protein", files)
    for path in files:
        if path != base:
            path.unlink()


def record_error(dsn: str, log_id: int, error: Exception, failed: bool) -> None:
    """Records an error on the load's row, as far as it still can.

    Only a load that has not committed is marked failed: the condition on status keeps a committed day done.
    """
    message = f"{type(error).__name__}: {error}"
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            if failed:
                conn.execute(
                    "UPDATE load_log SET status = 'failed', finished_at = now(), message = %s"
                    " WHERE id = %s AND status = 'running'",
                    [message, log_id],
                )
            else:
                conn.execute(
                    "UPDATE load_log SET message = %s WHERE id = %s", [message, log_id]
                )
    except psycopg.Error:
        logger.exception("could not record the error in load_log row %s", log_id)
