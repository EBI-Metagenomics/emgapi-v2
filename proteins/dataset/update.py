"""The dataset update: moves the staged rows of the MGYP database into a new day of the protein dataset.

Each function is one step, which the update_protein_dataset flow runs as a task.
"""

import logging
import shutil
from datetime import date
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import Iterable, Iterator, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from django.conf import settings
from django.db import connections, transaction
from django.utils import timezone

from proteins.dataset import layout, metadata
from proteins.dataset.owner import Owner
from proteins.models import DatasetUpdate
from proteins.routers import MGYP

logger = logging.getLogger(__name__)

# The staged tables. Their columns are those of the same tables in the protein dataset.
STAGED_TABLES = ("protein", "contig", "occurrence")
# The rows that a server-side cursor sends at a time.
BATCH_ROWS = 100_000


class DatasetUpdateError(Exception):
    """The dataset_update table does not allow the update to go on."""


class Staged(NamedTuple):
    assembly_proteins_ids: list[int]
    files: dict[str, Path]  # each staged table's rows, in the scratch directory
    assembly_proteins: pa.Table  # the registries, read in the same snapshot as the rows
    gene_callers: pa.Table


def clean_up_failed_update(root: Path) -> None:
    """Marks committed days complete if the last run stopped before it could, and deletes what failed updates left.

    :param root: The protein dataset's directory.
    """
    done = set(
        DatasetUpdate.objects.filter(status=DatasetUpdate.Status.DONE).values_list(
            "day", flat=True
        )
    )
    complete = set(layout.complete_days(root))
    # As after a restore of the MGYP database to before an update: that update's rows are staged again.
    if ahead := sorted(complete - done):
        raise DatasetUpdateError(
            f"The protein dataset has days that are not done in dataset_update: {', '.join(map(str, ahead))}."
            " The protein dataset is ahead of the MGYP database, so updating again would duplicate their rows"
        )
    for day in sorted((layout.days_with_files(root) | done) - complete):
        if day not in done:
            logger.warning("deleting the files of %s, left by a failed update", day)
            delete_day(root, day)
        elif (root / "metadata" / f".tmp-day={day}").exists():
            logger.warning(
                "marking %s complete, committed by an update that ended before", day
            )
            mark_day_complete(root, day)
        else:
            raise DatasetUpdateError(
                f"{day} is done in dataset_update, but the protein dataset has no metadata snapshot of it"
            )
    for leftover in root.rglob(".*.tmp"):
        leftover.unlink()


def delete_day(root: Path, day: date) -> None:
    """Deletes the files of a failed day.

    :param root: The protein dataset's directory.
    :param day: The day.
    """
    for path in root.glob(f"protein/*/day-{day}.parquet"):
        path.unlink()
    for directory in (
        root / "occurrence" / f"day={day}",
        root / "contig" / f"day={day}",
        root / "metadata" / f".tmp-day={day}",
    ):
        shutil.rmtree(directory, ignore_errors=True)


def is_done(day: date) -> bool:
    """Whether a dataset update of this day is done.

    :param day: The day.
    """
    return DatasetUpdate.objects.filter(
        day=day, status=DatasetUpdate.Status.DONE
    ).exists()


def start_update(day: date) -> int:
    """Records the start of an update. The protein dataset concurrency limit lets one flow run at a time, so any other running update is dead.

    :param day: The day of the update.
    :return: The id of the DatasetUpdate row.
    """
    with transaction.atomic(using=MGYP):
        DatasetUpdate.objects.filter(status=DatasetUpdate.Status.RUNNING).update(
            status=DatasetUpdate.Status.FAILED,
            finished_at=timezone.now(),
            message="ended without recording it",
        )
        return DatasetUpdate.objects.create(
            day=day, status=DatasetUpdate.Status.RUNNING, started_at=timezone.now()
        ).id


def read_staged_rows(scratch: Path) -> Staged:
    """Reads the staged rows and the registries in one snapshot, so that the registries hold no record without its rows.

    A server-side cursor on the Django connection streams the rows to Parquet in batches, so a
    backlog does not have to fit in memory.

    :param scratch: A local directory for the Parquet files of the staged rows.
    """
    with transaction.atomic(using=MGYP):
        conn = connections[MGYP]
        with conn.cursor() as cursor:
            # The first statement of the transaction, so that every read below sees one snapshot.
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            assembly_proteins_ids = choose_assembly_proteins(cursor)
            cursor.execute(
                "SELECT id, assembly_accession, source, pipeline_version, analysis_accession"
                " FROM assembly_proteins"
            )
            assembly_proteins = to_table(
                cursor.fetchall(), layout.SCHEMAS["metadata/assembly_proteins_registry"]
            )
            cursor.execute("SELECT id, name, version FROM gene_caller")
            gene_callers = to_table(
                cursor.fetchall(), layout.SCHEMAS["metadata/gene_callers"]
            )
        files = {}
        for table in STAGED_TABLES:
            files[table] = scratch / f"{table}.parquet"
            schema = layout.SCHEMAS[table]
            # A named cursor is a server-side cursor: it sends the rows in batches.
            with conn.connection.cursor(name=f"read_staged_{table}") as rows:
                rows.execute(
                    f"SELECT {', '.join(schema.names)} FROM staged_{table}"
                    " WHERE assembly_proteins_id = ANY(%s)",
                    [assembly_proteins_ids],
                )
                with pq.ParquetWriter(files[table], schema) as writer:
                    while batch := rows.fetchmany(BATCH_ROWS):
                        writer.write_table(to_table(batch, schema))
    return Staged(assembly_proteins_ids, files, assembly_proteins, gene_callers)


def to_table(rows: list[tuple], schema: pa.Schema) -> pa.Table:
    """Rows from a cursor as an Arrow table.

    :param rows: Tuples in the column order of the schema.
    :param schema: The schema of the table.
    """
    columns = list(zip(*rows)) or [[] for _ in schema]
    return pa.Table.from_arrays(
        [pa.array(column, type=field.type) for column, field in zip(columns, schema)],
        schema=schema,
    )


def choose_assembly_proteins(cursor) -> list[int]:
    """The assembly proteins records with staged rows. Each record's rows are committed together and never staged again, so they are complete.

    :param cursor: A cursor in the snapshot of read_staged_rows().
    """
    cursor.execute(
        "SELECT assembly_proteins_id FROM staged_protein"
        " UNION SELECT assembly_proteins_id FROM staged_contig"
        " UNION SELECT assembly_proteins_id FROM staged_occurrence"
    )
    return sorted(row[0] for row in cursor.fetchall())


def write_day_files(root: Path, day: date, staged: Staged) -> dict[str, int]:
    """Writes the protein, contig and occurrence files of the day.

    :param root: The protein dataset's directory.
    :param day: The day.
    :param staged: What read_staged_rows() read.
    :return: The number of rows written to each table.
    """
    # A backlog can outgrow the job's memory, so the rows are sorted by DuckDB, which spills to disk.
    # Sorted by hash, the rows are also grouped by prefix, the hash's top bits.
    memory_limit = settings.EMG_CONFIG.proteins.duckdb_memory_limit
    tmp_dir = settings.EMG_CONFIG.proteins.tmp_dir
    batches = layout.sorted_batches(
        "protein", [staged.files["protein"]], memory_limit, tmp_dir
    )
    for prefix, slices in groupby(by_prefix(batches), itemgetter(0)):
        layout.write_sorted(
            root / "protein" / f"prefix={prefix}" / f"day-{day}.parquet",
            "protein",
            (batch for _, batch in slices),
        )
    counts = {}
    for table in ("protein", "occurrence", "contig"):
        counts[f"{table}s"] = pq.ParquetFile(staged.files[table]).metadata.num_rows
    for table in ("occurrence", "contig"):
        layout.merge(
            root / table / f"day={day}" / "part-000.parquet",
            table,
            [staged.files[table]],
            memory_limit,
            tmp_dir,
        )
    return counts


def by_prefix(
    batches: Iterable[pa.RecordBatch],
) -> Iterator[tuple[str, pa.RecordBatch]]:
    """Cuts protein batches sorted by hash wherever the prefix changes.

    :param batches: Proteins sorted by hash.
    :return: Each slice with its prefix, in order.
    """
    for batch in batches:
        start = 0
        for prefix, run in groupby(map(layout.prefix, batch["hash"].to_pylist())):
            length = sum(1 for _ in run)
            yield prefix, batch.slice(start, length)
            start += length


def write_metadata_snapshot(root: Path, day: date, staged: Staged) -> None:
    """Writes the metadata snapshot and the registries of the day, not yet complete.

    :param root: The protein dataset's directory.
    :param day: The day.
    :param staged: What read_staged_rows() read.
    """
    directory = root / "metadata" / f".tmp-day={day}"
    layout.write(
        directory / "assembly_proteins.parquet",
        "metadata/assembly_proteins",
        metadata.build_snapshot(staged.assembly_proteins),
    )
    layout.write(
        directory / "assembly_proteins_registry.parquet",
        "metadata/assembly_proteins_registry",
        staged.assembly_proteins,
    )
    layout.write(
        directory / "gene_callers.parquet",
        "metadata/gene_callers",
        staged.gene_callers,
    )


def finish_update(
    update_id: int,
    assembly_proteins_ids: list[int],
    counts: dict[str, int],
    owner: Owner,
) -> None:
    """Deletes the staged rows that the day's files hold, and marks the update done. From here on, these rows exist only in the protein dataset.

    :param update_id: The id of the DatasetUpdate row.
    :param assembly_proteins_ids: The records whose staged rows were written.
    :param counts: The rows written to each table.
    :param owner: This run's ownership of the protein dataset, confirmed in the transaction.
    """
    with transaction.atomic(using=MGYP):
        owner.confirm()
        # Raw SQL: QuerySet.delete() would load the rows first.
        with connections[MGYP].cursor() as cursor:
            for table in ("occurrence", "contig", "protein"):
                cursor.execute(
                    f"DELETE FROM staged_{table} WHERE assembly_proteins_id = ANY(%s)",
                    [assembly_proteins_ids],
                )
        updated = DatasetUpdate.objects.filter(
            id=update_id, status=DatasetUpdate.Status.RUNNING
        ).update(
            status=DatasetUpdate.Status.DONE,
            finished_at=timezone.now(),
            assembly_proteins=len(assembly_proteins_ids),
            proteins=counts["proteins"],
            contigs=counts["contigs"],
            occurrences=counts["occurrences"],
        )
        if updated != 1:
            raise DatasetUpdateError(f"Dataset update {update_id} is no longer running")


def mark_day_complete(root: Path, day: date) -> None:
    """Moves the metadata snapshot into place. Now readers can use the day.

    :param root: The protein dataset's directory.
    :param day: The day.
    """
    (root / "metadata" / f".tmp-day={day}").rename(root / "metadata" / f"day={day}")


def record_error(update_id: int, error: Exception, failed: bool) -> None:
    """Records an error on the update's row, as far as it still can.

    Only an update that has not committed is marked failed: the condition on status keeps a committed day done.

    :param update_id: The id of the DatasetUpdate row.
    :param error: The error.
    :param failed: Whether to mark the update failed. False for an error after the commit.
    """
    message = f"{type(error).__name__}: {error}"
    try:
        rows = DatasetUpdate.objects.filter(id=update_id)
        if failed:
            rows.filter(status=DatasetUpdate.Status.RUNNING).update(
                status=DatasetUpdate.Status.FAILED,
                finished_at=timezone.now(),
                message=message,
            )
        else:
            rows.update(message=message)
    except Exception:
        logger.exception("could not record the error on dataset update %s", update_id)
