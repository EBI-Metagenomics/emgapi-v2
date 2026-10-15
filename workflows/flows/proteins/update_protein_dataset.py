import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from proteins.dataset import update
from proteins.dataset.owner import owner
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


@task(task_run_name="Clean up a failed dataset update")
def clean_up_failed_update(root: Path) -> None:
    update.clean_up_failed_update(root)


@task(task_run_name="Check if the dataset update of {day} is done")
def is_done(day: date) -> bool:
    return update.is_done(day)


@task(task_run_name="Start the dataset update of {day}")
def start_update(day: date) -> int:
    return update.start_update(day)


@task(task_run_name="Read the staged rows")
def read_staged_rows(scratch: Path) -> update.Staged:
    return update.read_staged_rows(scratch)


@task(task_run_name="Write the protein, contig and occurrence files of {day}")
def write_day_files(root: Path, day: date, staged: update.Staged) -> dict[str, int]:
    return update.write_day_files(root, day, staged)


@task(task_run_name="Write the metadata snapshot of {day}")
def write_metadata_snapshot(root: Path, day: date, staged: update.Staged) -> None:
    update.write_metadata_snapshot(root, day, staged)


@task(task_run_name="Finish dataset update {update_id}")
def finish_update(
    update_id: int, assembly_proteins_ids: list[int], counts: dict[str, int]
) -> None:
    update.finish_update(update_id, assembly_proteins_ids, counts)


@task(task_run_name="Mark {day} complete")
def mark_day_complete(root: Path, day: date) -> None:
    update.mark_day_complete(root, day)


@flow(flow_run_name="Update the protein dataset")
def update_protein_dataset():
    """Moves the staged rows of the MGYP database into a new day of the protein dataset.

    The flow holds the protein dataset concurrency limit from start to end, so it never runs
    with the merge. It only adds files.
    """
    logger = get_run_logger()
    root = Path(EMG_CONFIG.proteins.dataset_dir)
    day = datetime.now(timezone.utc).date()

    with owner(EMG_CONFIG.proteins.dataset_concurrency_limit):
        clean_up_failed_update(root)
        if is_done(day):
            logger.info(f"The dataset update of {day} is done already")
            return

        update_id = start_update(day)
        try:
            with tempfile.TemporaryDirectory(
                prefix="protein-dataset-update-"
            ) as scratch:
                staged = read_staged_rows(Path(scratch))
                counts = write_day_files(root, day, staged)
                write_metadata_snapshot(root, day, staged)
                finish_update(update_id, staged.assembly_proteins_ids, counts)
        except Exception as e:
            # Marks the row failed only if finish_update did not commit.
            update.record_error(update_id, e, failed=True)
            raise

        mark_day_complete(root, day)
        logger.info(f"Updated the protein dataset with {day}: {counts}")
