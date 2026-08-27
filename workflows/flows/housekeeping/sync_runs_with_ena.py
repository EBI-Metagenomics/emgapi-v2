from prefect import get_run_logger
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG  # noqa: F401

import analyses.models
from workflows.ena_utils.ena_api_requests import sync_run_metadata_from_ena
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
)
from workflows.prefect_utils.flows_utils import (
    django_db_task as task,
)


@task(name="Sync batch of runs metadata from ENA")
def sync_runs(run_accessions: list[str]) -> list[str]:
    """Sync metadata for a batch of runs from ENA.

    Each run is synced individually with try/except so that a failure
    for one run does not block the rest of the batch.

    :param run_accessions: List of run accessions to sync.
    :return: List of accessions that failed to sync.
    """
    logger = get_run_logger()
    failed = []
    for run_accession in run_accessions:
        try:
            run = analyses.models.Run.objects.get_by_accession(run_accession)
            logger.info(f"Syncing metadata for run {run.first_accession}")
            sync_run_metadata_from_ena(run)
            logger.info(f"Successfully synced metadata for run {run.first_accession}")
        except Exception as e:
            logger.error(f"Failed to sync run {run_accession}: {e}")
            failed.append(run_accession)
    return failed


@flow(flow_run_name="Sync runs with ENA")
def sync_runs_with_ena(
    accessions: list[str] | None = None,
    all_runs: bool = False,
    batch_size: int = 50,
) -> list[str]:
    """Sync run metadata from ENA for a list of accessions or all runs.

    Runs are processed in batches to avoid long-running DB connections.

    :param accessions: List of run accessions to sync.
    :param all_runs: If True, sync all runs.
    :param batch_size: Number of runs to process per batch (default 50).
    :return: List of run accessions that failed to sync.
    """
    logger = get_run_logger()

    if accessions and all_runs:
        raise ValueError("Cannot provide both accessions and all_runs")

    if not accessions and not all_runs:
        raise ValueError("Must provide either accessions or all_runs=True")

    if accessions:
        run_accessions = accessions
    else:
        run_accessions = [
            known_accessions[0]
            for known_accessions in analyses.models.Run.objects.values_list(
                "ena_accessions", flat=True
            )
            if known_accessions
        ]

    total = len(run_accessions)
    logger.info(f"Syncing metadata for {total} runs in batches of {batch_size}")

    failed_accessions = []
    for i in range(0, total, batch_size):
        batch = run_accessions[i : i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} runs)")
        failed = sync_runs(batch)
        if failed:
            failed_accessions.extend(failed)

    if failed_accessions:
        logger.warning(
            f"Failed to sync {len(failed_accessions)} runs. "
            "See the 'failed-ena-run-syncs' table artifact for accessions."
        )
        create_table_artifact(
            key="failed-ena-run-syncs",
            table=[{"accession": accession} for accession in failed_accessions],
            description=f"{len(failed_accessions)} runs failed to sync from ENA.",
        )
    else:
        logger.info("All runs synced successfully")

    return failed_accessions
