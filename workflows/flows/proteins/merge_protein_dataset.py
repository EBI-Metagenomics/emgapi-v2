from pathlib import Path

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from proteins.dataset import merge
from proteins.dataset.owner import Owner, owner
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


@task(task_run_name="Clean up an interrupted merge")
def clean_up_interrupted_merge(root: Path) -> None:
    merge.clean_up_interrupted_merge(root)


@task(task_run_name="Merge the protein files of each prefix")
def merge_dataset(root: Path, owned: Owner) -> int:
    return merge.merge_dataset(root, owned)


@flow(flow_run_name="Merge the protein dataset")
def merge_protein_dataset() -> int:
    """For each hash prefix, merges the base file and the day files into a new base file.

    It owns the protein dataset, so it never runs with the dataset update.
    Do not run this flow while a protein release reads the protein dataset.

    :return: The number of prefixes merged.
    """
    logger = get_run_logger()
    root = Path(EMG_CONFIG.proteins.dataset_dir)
    with owner("merge", EMG_CONFIG.proteins.dataset_concurrency_limit) as owned:
        owned.check()
        clean_up_interrupted_merge(root)
        merged = merge_dataset(root, owned)
    logger.info(f"Merged {merged} prefixes of the protein dataset")
    return merged
