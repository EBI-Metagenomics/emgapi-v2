from prefect import get_run_logger
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG  # noqa: F401

import ena.models
from workflows.ena_utils.ena_api_requests import sync_study_metadata_from_ena
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
    django_db_task as task,
)


@task(name="Sync batch of studies metadata from ENA")
def sync_studies(study_accessions: list[str]) -> list[str]:
    """Sync metadata for a batch of studies from ENA.

    Each study is synced individually with try/except so that a failure
    for one study does not block the rest of the batch.

    :param study_accessions: List of study accessions to sync.
    :return: List of accessions that failed to sync.
    """
    logger = get_run_logger()
    failed = []
    for study_accession in study_accessions:
        try:
            study = ena.models.Study.objects.get_ena_study(study_accession)
            logger.info(f"Syncing metadata for study {study.accession}")
            sync_study_metadata_from_ena(study)
            logger.info(f"Successfully synced metadata for study {study.accession}")
        except Exception as e:
            logger.error(f"Failed to sync study {study_accession}: {e}")
            failed.append(study_accession)
    return failed


@flow(flow_run_name="Sync studies with ENA")
def sync_studies_with_ena(
    accessions: list[str] | None = None,
    all_studies: bool = False,
    batch_size: int = 50,
) -> list[str]:
    """Sync study metadata from ENA for a list of accessions or all studies.

    Studies are processed in batches to avoid long-running DB connections.

    :param accessions: List of study accessions to sync.
    :param all_studies: If True, sync all studies.
    :param batch_size: Number of studies to process per batch (default 50).
    :return: List of study accessions that failed to sync.
    """
    logger = get_run_logger()

    if accessions and all_studies:
        raise ValueError("Cannot provide both accessions and all_studies")

    if not accessions and not all_studies:
        raise ValueError("Must provide either accessions or all_studies=True")

    if accessions:
        study_accessions = accessions
    else:
        study_accessions = list(
            ena.models.Study.objects.values_list("accession", flat=True)
        )

    total = len(study_accessions)
    logger.info(f"Syncing metadata for {total} studies in batches of {batch_size}")

    all_failed = []
    for i in range(0, total, batch_size):
        batch = study_accessions[i : i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} studies)")
        failed = sync_studies(batch)
        all_failed.extend(failed)

    if all_failed:
        create_table_artifact(
            key="failed-ena-study-syncs",
            table=[{"accession": accession} for accession in all_failed],
            description=f"{len(all_failed)} studies failed to sync from ENA.",
        )
        logger.warning(
            f"Failed to sync {len(all_failed)} studies. "
            "See the 'failed-ena-study-syncs' table artifact for accessions."
        )
    else:
        logger.info("All studies synced successfully")

    return all_failed
