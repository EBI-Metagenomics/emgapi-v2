from __future__ import annotations

import logging
import tarfile
from pathlib import Path
from typing import Any

from django.conf import settings

from emgapiv2.tasking import TaskContext, task

from .sourmash import run_sourmash_gather

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


def _build_request_archive_path(request_id: str) -> Path:
    return Path(EMG_CONFIG.sourmash.results_path) / request_id / f"{request_id}.tgz"


def _archive_request_results(
    request_id: str,
    signature_results: list[dict[str, Any]],
) -> str:
    csv_paths = [
        Path(signature["raw_csv_path"])
        for signature in signature_results
        if signature.get("raw_csv_path")
    ]
    if len(csv_paths) <= 1:
        return ""

    archive_path = _build_request_archive_path(request_id)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive_path, "w:gz") as archive:
        for csv_path in csv_paths:
            if csv_path.exists():
                archive.add(csv_path, arcname=csv_path.name)

    return str(archive_path) if archive_path.exists() else ""


def _derive_request_status(signature_results: list[dict[str, Any]]) -> str:
    statuses = {signature["status"] for signature in signature_results}
    if statuses == {"NO_RESULTS"}:
        return "NO_RESULTS"
    if statuses == {"SUCCESS"}:
        return "SUCCESS"
    if statuses == {"FAILED"}:
        return "FAILED"
    if "SUCCESS" in statuses:
        return "PARTIAL_SUCCESS"
    if statuses.issubset({"FAILED", "NO_RESULTS"}):
        return "FAILED"
    return "FAILED"


@task(takes_context=True, queue_name="default")
def run_sourmash_gather_request(
    context: TaskContext,
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    request_id = request_payload["request_id"]
    signature_results: list[dict[str, Any]] = []

    for search_item in request_payload["search_items"]:
        signature_result: dict[str, Any] = {
            "job_id": context.task_result.id,
            "filename": search_item["original_filename"],
            "catalogue": search_item["catalogue_id"],
        }
        try:
            summary = run_sourmash_gather(
                query_path=search_item["query_path"],
                original_filename=search_item["original_filename"],
                catalogue_id=search_item["catalogue_id"],
                artifact_path=search_item["artifact_path"],
                result_path=search_item["result_path"],
                ksize=search_item["ksize"],
                moltype=search_item["moltype"],
                name_map_path=EMG_CONFIG.sourmash.name_map_path,
            )
            item_status = (
                "NO_RESULTS" if summary.get("status") == "NO_RESULTS" else "SUCCESS"
            )
            signature_result["status"] = item_status
            signature_result["result"] = summary
            result_path = Path(search_item["result_path"])
            if result_path.exists():
                signature_result["raw_csv_path"] = str(result_path)
        except Exception as exc:
            logger.exception(
                "Sourmash gather request failed for %s against %s",
                search_item["original_filename"],
                search_item["catalogue_id"],
            )
            signature_result["status"] = "FAILED"
            signature_result["reason"] = str(exc)

        signature_results.append(signature_result)

    archive_path = _archive_request_results(request_id, signature_results)

    return {
        "request_id": request_id,
        "status": _derive_request_status(signature_results),
        "signatures_received": request_payload["signatures_received"],
        "requested_catalogues": request_payload["requested_catalogues"],
        "signatures": signature_results,
        "archive_path": archive_path,
    }
