from pathlib import Path

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from proteins.dataset.status import status
from workflows.prefect_utils.flows_utils import django_db_flow as flow


class StatusCheckFailed(Exception):
    """At least one check of the state of the proteins app failed."""


@flow(flow_run_name="Check the proteins status")
def check_proteins_status():
    """Logs the state of the MGYP database and of the protein dataset, and fails if any of its checks fails."""
    logger = get_run_logger()
    now = status(Path(EMG_CONFIG.proteins.dataset_dir))
    logger.info(now.report())
    problems = now.problems()
    for problem in problems:
        logger.error(problem)
    if problems:
        raise StatusCheckFailed(f"{len(problems)} checks failed: {'; '.join(problems)}")
