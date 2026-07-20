from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.utils import timezone

from emgapiv2.tasking import TaskContext, task
from genomes.models import SourmashSearchJob, SourmashSearchJobItem

from .sourmash import run_sourmash_gather

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


def refresh_sourmash_search_job(job_id: str) -> SourmashSearchJob:
    job = SourmashSearchJob.objects.prefetch_related("items").get(id=job_id)
    return job.recalculate_status()


def mark_sourmash_items_enqueue_failed(item_ids: list[str], error: Exception) -> None:
    failure_time = timezone.now()
    error_message = f"Failed to enqueue sourmash search task: {error}"
    jobs_to_refresh = list(
        SourmashSearchJobItem.objects.filter(id__in=item_ids)
        .values_list("job_id", flat=True)
        .distinct()
    )
    SourmashSearchJobItem.objects.filter(id__in=item_ids).update(
        status=SourmashSearchJobItem.Status.FAILED,
        error_message=error_message,
        finished_at=failure_time,
    )
    for job_id in jobs_to_refresh:
        refresh_sourmash_search_job(str(job_id))


@task(takes_context=True, queue_name="default")
def run_sourmash_gather_item(context: TaskContext, job_item_id: str) -> dict[str, Any]:
    job_item = SourmashSearchJobItem.objects.select_related(
        "job",
        "search_index",
        "search_index__catalogue",
    ).get(id=job_item_id)

    started_at = job_item.started_at or timezone.now()
    SourmashSearchJobItem.objects.filter(id=job_item.id).update(
        status=SourmashSearchJobItem.Status.RUNNING,
        started_at=started_at,
        task_result_id=context.task_result.id,
        error_message="",
    )
    refresh_sourmash_search_job(str(job_item.job_id))

    try:
        summary = run_sourmash_gather(
            query_path=job_item.query_staged_path,
            original_filename=job_item.query_original_name,
            catalogue_id=job_item.search_index.catalogue.catalogue_id,
            artifact_path=job_item.search_index.artifact_path,
            result_path=job_item.raw_csv_path,
            ksize=job_item.search_index.ksize,
            moltype=job_item.search_index.moltype,
            name_map_path=EMG_CONFIG.sourmash.name_map_path,
        )
        finished_at = timezone.now()
        item_status = (
            SourmashSearchJobItem.Status.NO_RESULTS
            if summary.get("status") == "NO_RESULTS"
            else SourmashSearchJobItem.Status.SUCCESS
        )
        SourmashSearchJobItem.objects.filter(id=job_item.id).update(
            status=item_status,
            result_summary=summary,
            error_message="",
            finished_at=finished_at,
            task_result_id=context.task_result.id,
        )
        refresh_sourmash_search_job(str(job_item.job_id))
        return {
            "job_item_id": str(job_item.id),
            "job_id": str(job_item.job_id),
            "catalogue_id": job_item.search_index.catalogue.catalogue_id,
            "search_index_id": str(job_item.search_index_id),
            "status": item_status,
            "result_summary": summary,
        }
    except Exception as exc:
        logger.exception("Sourmash gather task failed for item %s", job_item.id)
        SourmashSearchJobItem.objects.filter(id=job_item.id).update(
            status=SourmashSearchJobItem.Status.FAILED,
            error_message=str(exc),
            finished_at=timezone.now(),
            task_result_id=context.task_result.id,
        )
        refresh_sourmash_search_job(str(job_item.job_id))
        raise
