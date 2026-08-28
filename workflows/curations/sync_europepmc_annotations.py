from datetime import timedelta

from django.utils import timezone
from prefect import get_run_logger

import analyses.models
from curations.europe_pmc import (
    EUROPE_PMC_PROVIDER,
    fetch_epmc_publication_annotations,
    publications_requiring_sync,
    record_publication_annotations,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


@task(
    name="Sync Europe PMC annotations for publication",
    retries=3,
    retry_delay_seconds=60,
)
def sync_publication(pubmed_id: int) -> int:
    """Fetch and persist one publication's Europe PMC annotation snapshot."""
    publication = analyses.models.Publication.objects.get(pubmed_id=pubmed_id)
    record_publication_annotations(
        publication, fetch_epmc_publication_annotations(publication.pubmed_id)
    )
    return pubmed_id


def _chunks(values: list[int], chunk_size: int):
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


@flow(flow_run_name="Sync Europe PMC annotations")
def sync_europepmc_annotations(
    pubmed_ids: list[int] | None = None,
    all_publications: bool = False,
    force_refresh: bool = False,
    stale_days: int = 30,
    chunk_size: int = 50,
) -> dict[str, object]:
    """Synchronize Europe PMC annotations for selected or eligible publications."""
    logger = get_run_logger()
    if pubmed_ids and all_publications:
        raise ValueError("Cannot provide both pubmed_ids and all_publications")
    if not pubmed_ids and not all_publications:
        raise ValueError("Must provide pubmed_ids or all_publications=True")
    if stale_days < 0:
        raise ValueError("stale_days must be non-negative")
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")

    requested_ids = set(pubmed_ids or [])
    selected_ids = pubmed_ids
    if force_refresh:
        publications = analyses.models.Publication.objects.all()
        if selected_ids is not None:
            publications = publications.filter(pubmed_id__in=selected_ids)
    else:
        stale_after = timezone.now() - timedelta(days=stale_days)
        publications = publications_requiring_sync(
            publication_ids=selected_ids,
            stale_after=stale_after,
        )
    selected_ids = list(publications.values_list("pubmed_id", flat=True))
    skipped = sorted(requested_ids.difference(selected_ids))

    synchronized: list[int] = []
    failed: list[int] = []
    logger.info(
        "Synchronizing %s %s publications in chunks of %s",
        len(selected_ids),
        EUROPE_PMC_PROVIDER,
        chunk_size,
    )
    for chunk in _chunks(selected_ids, chunk_size):
        futures = [sync_publication.submit(pubmed_id) for pubmed_id in chunk]
        for pubmed_id, future in zip(chunk, futures):
            try:
                synchronized.append(future.result())
            except Exception:
                logger.exception(
                    "Failed to sync Europe PMC annotations for %s", pubmed_id
                )
                failed.append(pubmed_id)

    return {
        "provider": EUROPE_PMC_PROVIDER,
        "synchronized": synchronized,
        "failed": failed,
        "skipped": skipped,
    }
