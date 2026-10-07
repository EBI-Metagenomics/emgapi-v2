from pathlib import Path

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from proteins.flows.load import proteindb_dsn
from proteins.status import status
from workflows.prefect_utils.flows_utils import django_db_flow as flow


class CheckFailed(Exception): ...


@flow(flow_run_name="Check the protein DB")
def proteindb_check():
    """Logs the protein DB's state, and fails if any of its checks fails."""
    logger = get_run_logger()
    now = status(proteindb_dsn(), Path(EMG_CONFIG.proteindb.root) / "tier2")
    logger.info(now.report())
    problems = now.problems()
    for problem in problems:
        logger.error(problem)
    if problems:
        raise CheckFailed(f"{len(problems)} checks failed: {'; '.join(problems)}")
