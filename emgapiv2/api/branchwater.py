import json
import logging
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from django.conf import settings
from django_tasks.exceptions import TaskResultDoesNotExist
from ninja import Query, Schema
from ninja.errors import HttpError
from ninja_extra import api_controller, http_get, http_post

from emgapiv2.api.schema_utils import ApiSections
from emgapiv2.tasking import default_task_backend
from genomes.branchwater import (
    BranchwaterError,
    _check_branchwater_health,
    _load_branchwater_mag_signature,
    _parse_branchwater_csv_results,  # noqa: F401 - retained for existing callers
    validate_branchwater_signature,
    write_branchwater_json,
)
from genomes.tasks import run_branchwater_search

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG
JobStatus = Literal["QUEUED", "RUNNING", "SUCCESS", "NO_RESULTS", "FAILED"]


class BranchwaterSearchIn(Schema):
    signatures: str


class BranchwaterHealthOut(Schema):
    status: str


class BranchwaterSubmissionData(Schema):
    job_id: str
    status: Literal["QUEUED"] = "QUEUED"
    status_url: str
    results_url: str


class BranchwaterSubmissionOut(Schema):
    data: BranchwaterSubmissionData


class BranchwaterStatusData(Schema):
    job_id: str
    status: JobStatus
    results_url: Optional[str] = None
    reason: Optional[str] = None


class BranchwaterStatusOut(Schema):
    data: BranchwaterStatusData


def _job_url(request, kind: str, job_id: str) -> str:
    # Preserve the mounted API prefix and current host in both production and dev.
    base = request.path.split("/services/branchwater", 1)[0]
    return request.build_absolute_uri(f"{base}/services/branchwater/{kind}/{job_id}/")


def _submit_branchwater(request, signatures: Any) -> BranchwaterSubmissionOut:
    try:
        validated = validate_branchwater_signature(signatures)
    except BranchwaterError as exc:
        raise HttpError(exc.status_code, exc.message) from exc
    request_id = uuid.uuid4().hex
    query_path = Path(EMG_CONFIG.branchwater.queries_path) / request_id / "query.sig"
    result_path = (
        Path(EMG_CONFIG.branchwater.results_path) / request_id / "results.json"
    )
    try:
        write_branchwater_json(query_path, validated)
        task_result = run_branchwater_search.enqueue(
            query_path=str(query_path), result_path=str(result_path)
        )
    except Exception as exc:
        logger.exception("Unable to stage or enqueue Branchwater search")
        raise HttpError(
            503,
            "Branchwater task service is temporarily unavailable. Please try later.",
        ) from exc
    return BranchwaterSubmissionOut(
        data=BranchwaterSubmissionData(
            job_id=task_result.id,
            status_url=_job_url(request, "status", task_result.id),
            results_url=_job_url(request, "results", task_result.id),
        )
    )


def _branchwater_task_result(job_id: str):
    # The dummy backend uses compact alphanumeric IDs, while django-tasks-db
    # uses canonical UUIDs. Only accept those two backend-generated formats.
    is_compact_id = re.fullmatch(r"[A-Za-z0-9]{32,64}", job_id) is not None
    try:
        is_canonical_uuid = str(uuid.UUID(job_id)) == job_id.lower()
    except (AttributeError, TypeError, ValueError):
        is_canonical_uuid = False
    if not (is_compact_id or is_canonical_uuid):
        raise HttpError(404, "Branchwater job not found.")
    try:
        result = default_task_backend.get_result(job_id)
    except TaskResultDoesNotExist as exc:
        raise HttpError(404, "Branchwater job not found.") from exc
    except Exception as exc:
        logger.exception("Unable to retrieve Branchwater task status")
        raise HttpError(
            503,
            "Branchwater task service is temporarily unavailable. Please try later.",
        ) from exc
    if (
        result.task.module_path != run_branchwater_search.module_path
        or result.task.queue_name != "branchwater"
    ):
        raise HttpError(404, "Branchwater job not found.")
    return result


def _branchwater_task_state(task_result) -> tuple[str, dict[str, Any]]:
    status = getattr(task_result.status, "name", str(task_result.status)).upper()
    if status == "RUNNING":
        return "RUNNING", {}
    if status == "FAILED":
        return "FAILED", {"reason": "Branchwater search failed. Please try again."}
    if status == "SUCCESSFUL":
        try:
            payload = task_result.return_value
        except ValueError:
            # The database backend can expose SUCCESSFUL immediately before the
            # serialized return value is readable. Treat that short window as
            # still running so clients can safely poll again.
            return "RUNNING", {}
        if isinstance(payload, dict) and payload.get("status") in {
            "SUCCESS",
            "NO_RESULTS",
            "FAILED",
        }:
            return payload["status"], payload
        return "FAILED", {
            "reason": "Branchwater search did not produce a valid result."
        }
    return "QUEUED", {}


@api_controller("services/branchwater", tags=[ApiSections.GENOMES])
class BranchwaterController:
    @http_post(
        "/",
        response={202: BranchwaterSubmissionOut},
        summary="Queue a Branchwater search using a precomputed signature",
        operation_id="branchwater_search",
    )
    def search(self, request, payload: BranchwaterSearchIn):
        return 202, _submit_branchwater(request, payload.signatures)

    @http_post(
        "/mags",
        response={202: BranchwaterSubmissionOut},
        summary="Queue a Branchwater search using a precomputed MAG signature",
        operation_id="branchwater_search_mags",
    )
    def mags(self, request, accession: str = Query(...), catalogue: str = Query(...)):
        try:
            signatures = _load_branchwater_mag_signature(accession, catalogue)
        except BranchwaterError as exc:
            raise HttpError(exc.status_code, exc.message) from exc
        return 202, _submit_branchwater(request, signatures)

    @http_get(
        "/status/{job_id}/",
        response=BranchwaterStatusOut,
        summary="Check a Branchwater search job",
        operation_id="branchwater_status",
    )
    def status(self, request, job_id: str):
        status, payload = _branchwater_task_state(_branchwater_task_result(job_id))
        return BranchwaterStatusOut(
            data=BranchwaterStatusData(
                job_id=job_id,
                status=status,
                results_url=(
                    _job_url(request, "results", job_id)
                    if status in {"SUCCESS", "NO_RESULTS"}
                    else None
                ),
                reason=payload.get("reason") if status == "FAILED" else None,
            )
        )

    @http_get(
        "/results/{job_id}/",
        response=List[Dict[str, Any]],
        summary="Fetch the enriched results of a completed Branchwater search",
        operation_id="branchwater_results",
    )
    def results(self, job_id: str):
        status, payload = _branchwater_task_state(_branchwater_task_result(job_id))
        if status not in {"SUCCESS", "NO_RESULTS"}:
            raise HttpError(409, "Branchwater results are not available for this job.")
        try:
            path = Path(payload["result_path"]).resolve()
            path.relative_to(Path(EMG_CONFIG.branchwater.results_path).resolve())
            with path.open(encoding="utf-8") as handle:
                results = json.load(handle)
            if not isinstance(results, list):
                raise ValueError("Invalid result file")
        except (KeyError, OSError, ValueError, TypeError) as exc:
            logger.exception("Unable to read results for Branchwater job %s", job_id)
            raise HttpError(
                503, "Branchwater results are temporarily unavailable."
            ) from exc
        return results

    @http_get(
        "/health",
        response=BranchwaterHealthOut,
        summary="Healthcheck for the integrated Branchwater service",
        operation_id="branchwater_health",
    )
    def health(self):
        try:
            _check_branchwater_health()
        except BranchwaterError as exc:
            raise HttpError(exc.status_code, exc.message) from exc
        return BranchwaterHealthOut(status="ok")
