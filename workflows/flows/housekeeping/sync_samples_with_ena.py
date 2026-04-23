from prefect import get_run_logger

from activate_django_first import EMG_CONFIG  # noqa: F401

import ena.models
from workflows.ena_utils.ena_api_requests import sync_sample_metadata_from_ena
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
    django_db_task as task,
)


@task(name="Sync batch of samples metadata from ENA")
def sync_samples(sample_accessions: list[str]) -> list[str]:
    """Sync metadata for a batch of samples from ENA.

    Each sample is synced individually with try/except so that a failure
    for one sample does not block the rest of the batch.

    :param sample_accessions: List of sample accessions to sync.
    :return: List of accessions that failed to sync.
    """
    logger = get_run_logger()
    failed = []
    for sample_accession in sample_accessions:
        try:
            sample = ena.models.Sample.objects.get(accession=sample_accession)
            logger.info(f"Syncing metadata for sample {sample.accession}")
            sync_sample_metadata_from_ena(sample)
            logger.info(f"Successfully synced metadata for sample {sample.accession}")
        except Exception as e:
            logger.error(f"Failed to sync sample {sample_accession}: {e}")
            failed.append(sample_accession)
    return failed


@flow(flow_run_name="Sync samples with ENA")
def sync_samples_with_ena(
    accessions: list[str] | None = None,
    all_samples: bool = False,
    batch_size: int = 50,
) -> list[str]:
    """Sync sample metadata from ENA for a list of accessions or all samples.

    Samples are processed in batches to avoid long-running DB connections.

    :param accessions: List of sample accessions to sync.
    :param all_samples: If True, sync all samples.
    :param batch_size: Number of samples to process per batch (default 50).
    :return: List of sample accessions that failed to sync.
    """
    logger = get_run_logger()

    if accessions and all_samples:
        raise ValueError("Cannot provide both accessions and all_samples")

    if not accessions and not all_samples:
        raise ValueError("Must provide either accessions or all_samples=True")

    if accessions:
        sample_accessions = accessions
    else:
        sample_accessions = list(
            ena.models.Sample.objects.values_list("accession", flat=True)
        )

    total = len(sample_accessions)
    logger.info(f"Syncing metadata for {total} samples in batches of {batch_size}")

    all_failed = []
    for i in range(0, total, batch_size):
        batch = sample_accessions[i : i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} samples)")
        failed = sync_samples(batch)
        all_failed.extend(failed)

    if all_failed:
        logger.warning(
            f"Failed to sync {len(all_failed)} samples: {', '.join(all_failed)}"
        )
    else:
        logger.info("All samples synced successfully")

    return all_failed
