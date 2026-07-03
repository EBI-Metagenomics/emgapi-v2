import hashlib
import json
import logging
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from celery import Celery, group
from celery.app.control import Control
from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import FileResponse, Http404
from kombu.exceptions import OperationalError
from ninja import Schema
from ninja.errors import HttpError
from ninja_extra import api_controller, http_get, http_post

from emgapiv2.api.schema_utils import ApiSections
from genomes.models import GenomeCatalogue

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG
RESULTS_EXPIRE = 60 * 60 * 24 * 30


class SourmashGatherSubmissionData(Schema):
    message: str
    job_id: str
    children_ids: Dict[str, str]
    signatures_received: List[str]
    status_url: str


class SourmashGatherSubmissionOut(Schema):
    data: SourmashGatherSubmissionData


class SourmashTaskResult(Schema):
    overlap: Optional[str] = None
    p_query: Optional[str] = None
    p_match: Optional[str] = None
    match: Optional[str] = None
    catalog: Optional[str] = None
    query_filename: Optional[str] = None
    md5_name: Optional[str] = None
    matches: Optional[int] = None
    status: Optional[str] = None


class SourmashGatherSignatureStatus(Schema):
    job_id: str
    status: str
    filename: Optional[str] = None
    result: Optional[SourmashTaskResult] = None
    results_url: Optional[str] = None
    catalogue: Optional[str] = None
    reason: Optional[str] = None
    position_in_queue: Optional[int] = None


class SourmashGatherStatusData(Schema):
    group_id: str
    signatures: List[SourmashGatherSignatureStatus]
    results_url: Optional[str] = None
    worker_status: str


class SourmashGatherStatusOut(Schema):
    data: SourmashGatherStatusData


def _build_celery_app() -> Celery:
    app = Celery(
        "sourmash_tasks",
        broker=EMG_CONFIG.sourmash.celery_broker,
        backend=EMG_CONFIG.sourmash.celery_backend,
    )
    app.conf.update(
        result_extended=True,
        result_expires=RESULTS_EXPIRE,
    )
    return app


app = _build_celery_app()
control = Control(app=app)


def _api_path(path_suffix: str) -> str:
    return f"/{settings.BASE_URL}{path_suffix.lstrip('/')}"


def _absolute_api_url(request, path_suffix: str) -> str:
    return request.build_absolute_uri(_api_path(path_suffix))


def _is_signature_valid(signature: Dict[str, Any]) -> bool:
    return signature.get("molecule", "").lower() == "dna"


def _normalize_form_list(values: List[Any]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            normalized.extend(str(item) for item in value if item not in (None, ""))
            continue
        if value not in (None, ""):
            normalized.append(str(value))
    return normalized


def _coerce_uploaded_file(uploaded_file: Any):
    if hasattr(uploaded_file, "chunks") and hasattr(uploaded_file, "name"):
        return uploaded_file

    if isinstance(uploaded_file, tuple) and len(uploaded_file) >= 2:
        filename = str(uploaded_file[0])
        payload = uploaded_file[1]
        content_type = (
            str(uploaded_file[2])
            if len(uploaded_file) >= 3
            else "application/octet-stream"
        )
        if hasattr(payload, "seek"):
            payload.seek(0)
        content = payload.read() if hasattr(payload, "read") else payload
        if isinstance(content, str):
            content = content.encode("utf-8")
        return SimpleUploadedFile(filename, content, content_type=content_type)

    raise TypeError("Unsupported uploaded file payload")


def _get_uploaded_files(request) -> List[Any]:
    files = getattr(request, "FILES", None)
    if not files:
        return []
    if hasattr(files, "getlist"):
        return [_coerce_uploaded_file(f) for f in files.getlist("file_uploaded")]

    uploaded = files.get("file_uploaded")
    if uploaded is None:
        return []
    if isinstance(uploaded, list):
        return [_coerce_uploaded_file(f) for f in uploaded]
    return [_coerce_uploaded_file(uploaded)]


def _validate_sourmash_signature(json_str: str) -> None:
    signature = json.loads(json_str)
    if isinstance(signature, list):
        for item in signature:
            if _is_signature_valid(item):
                continue
            if "signatures" in item:
                for nested in item["signatures"]:
                    if not _is_signature_valid(nested):
                        raise ValueError(
                            "One of the signatures in the uploaded file is not valid"
                        )
            else:
                raise ValueError(
                    "One of the signatures in the uploaded file is not valid"
                )
    elif not _is_signature_valid(signature):
        raise ValueError("The file is not a valid sourmash signature")


def _get_unique_name(uploaded_file) -> str:
    md5_hash = hashlib.md5()
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    for chunk in uploaded_file.chunks():
        md5_hash.update(chunk)
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    return f"{md5_hash.hexdigest()}.sig"


def _save_signature(uploaded_file) -> str:
    query_dir = Path(EMG_CONFIG.sourmash.queries_path)
    query_dir.mkdir(parents=True, exist_ok=True)
    name = _get_unique_name(uploaded_file)
    destination_path = query_dir / name
    with destination_path.open("wb") as destination:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        for chunk in uploaded_file.chunks():
            destination.write(chunk)
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    return name


def _send_sourmash_jobs(
    uploads: List[Dict[str, str]], mag_catalogues: set[str]
) -> tuple[str, Dict[str, str]]:
    pairs = [
        (upload["original_name"], upload["saved_name"], catalogue)
        for upload in uploads
        for catalogue in mag_catalogues
    ]
    job = group(
        [
            app.signature(
                "tasks.run_gather",
                args=(saved_name, original_name, catalogue),
            )
            for original_name, saved_name, catalogue in pairs
        ],
        app=app,
    )
    result = job.apply_async()
    result.save()
    children_ids = {
        f"{original_name}:{catalogue}": child_result.id
        for (original_name, _, catalogue), child_result in zip(pairs, result.results)
    }
    return result.id, children_ids


def _get_task_pos_in_reserved(
    task_id: str, reserved: Dict[str, List[Dict]]
) -> int | None:
    pos = 0
    for tasks in reserved.values():
        for task in tasks:
            if task.get("id") == task_id:
                return pos + 1
            pos += 1
    return None


def _get_task_details_by_worker(task_id: str, inspect) -> Dict[str, Any]:
    try:
        tasks_by_worker = inspect.query_task(task_id)
    except Exception:
        logger.exception("Failed to query worker status for task %s", task_id)
        return {}
    return tasks_by_worker or {}


def _get_task_worker_status(task_id: str, inspect) -> str:
    tasks_by_worker = _get_task_details_by_worker(task_id, inspect)
    for worker_tasks in tasks_by_worker.values():
        if task_id not in worker_tasks:
            continue
        status = worker_tasks[task_id][0]
        if status == "reserved":
            return "IN_QUEUE"
        if status == "active":
            return "RUNNING"
        return str(status).upper()
    return "UNKNOWN"


def _get_task_catalogue(task_id: str, inspect) -> Optional[str]:
    tasks_by_worker = _get_task_details_by_worker(task_id, inspect)
    for worker_tasks in tasks_by_worker.values():
        if task_id not in worker_tasks:
            continue
        args = worker_tasks[task_id][1].get("args", [])
        if len(args) >= 3:
            return args[2]
    return None


def _get_sourmash_job_status(
    job_id: str, request
) -> Optional[SourmashGatherStatusData]:
    inspect = control.inspect()
    ping = inspect.ping() if inspect else None
    reserved = None
    group_result = app.GroupResult.restore(job_id, app=app)
    if group_result is None:
        return None

    signatures = []
    has_results = False
    for result in group_result.results:
        signature: Dict[str, Any] = {
            "job_id": result.id,
            "status": result.status,
        }

        filename = getattr(result, "args", [None, None])[1]
        if filename:
            signature["filename"] = filename

        if result.status == "SUCCESS":
            payload = result.result or {}
            signature["result"] = payload
            signature["results_url"] = _absolute_api_url(
                request, f"genomes-search/results/{result.id}/"
            )
            signature["catalogue"] = payload.get("catalog")
            signature["filename"] = signature.get("filename") or payload.get(
                "query_filename"
            )
            has_results = True
        elif result.status == "FAILURE":
            signature["reason"] = str(result.result)
            signature["catalogue"] = None
        elif result.status == "PENDING" and ping is not None:
            signature["status"] = _get_task_worker_status(result.id, inspect)
            signature["catalogue"] = _get_task_catalogue(result.id, inspect)
            if signature["status"] == "IN_QUEUE":
                if reserved is None:
                    reserved = inspect.reserved() or {}
                signature["position_in_queue"] = _get_task_pos_in_reserved(
                    result.id, reserved
                )

        signatures.append(SourmashGatherSignatureStatus(**signature))

    return SourmashGatherStatusData(
        group_id=job_id,
        signatures=signatures,
        results_url=(
            _absolute_api_url(request, f"genomes-search/results/{job_id}/")
            if has_results
            else None
        ),
        worker_status="OFFLINE" if ping is None else "OK",
    )


def _generate_tgz_from_group_id(group_id: str) -> Optional[Path]:
    group_result = app.GroupResult.restore(group_id, app=app)
    if group_result is None:
        return None

    results_dir = Path(EMG_CONFIG.sourmash.results_path)
    results_dir.mkdir(parents=True, exist_ok=True)
    archive_path = results_dir / f"{group_id}.tgz"

    with tarfile.open(archive_path, "w:gz") as tar:
        added = False
        for result in group_result.results:
            if result.status != "SUCCESS":
                continue
            csv_path = results_dir / f"{result.id}.csv"
            if csv_path.exists():
                tar.add(csv_path, arcname=f"{result.id}.csv")
                added = True

    return archive_path if archive_path.exists() and added else None


def _get_result_file(job_id: str) -> tuple[Optional[Path], Optional[str]]:
    results_dir = Path(EMG_CONFIG.sourmash.results_path)
    csv_path = results_dir / f"{job_id}.csv"
    if csv_path.exists():
        return csv_path, "text/csv"

    tgz_path = results_dir / f"{job_id}.tgz"
    if tgz_path.exists():
        return tgz_path, "application/gzip"

    archive = _generate_tgz_from_group_id(job_id)
    if archive and archive.exists():
        return archive, "application/gzip"

    return None, None


@api_controller("genomes-search", tags=[ApiSections.GENOMES])
class GenomeSearchGatherController:
    @http_get(
        "/gather/",
        response=Dict[str, str],
        summary="Describe the sourmash gather submission endpoint",
        operation_id="genome_search_gather_info",
    )
    def gather_info(self):
        return {"message": "Use POST on this endpoint to submit sourmash gather jobs."}

    @http_post(
        "/gather/",
        response=SourmashGatherSubmissionOut,
        summary="Submit one or more sourmash gather jobs for uploaded signatures",
        operation_id="genome_search_gather_submit",
    )
    def submit_gather(self, request):
        mag_catalogues = set(
            _normalize_form_list(request.POST.getlist("mag_catalogues"))
        )
        if not mag_catalogues:
            raise HttpError(
                400, "A list of mag_catalogues to search against must be provided."
            )

        catalogue_choices = set(
            GenomeCatalogue.objects.values_list("catalogue_id", flat=True)
        )
        bad_catalogues = mag_catalogues.difference(catalogue_choices)
        if bad_catalogues:
            raise HttpError(
                400,
                "The provided mag_catalogues are not valid. "
                f"Available: {sorted(catalogue_choices)}; "
                f"Unavailable: {sorted(bad_catalogues)}",
            )

        uploaded_files = _get_uploaded_files(request)
        if not uploaded_files:
            raise HttpError(400, "At least one file_uploaded entry must be provided.")

        uploads = []
        for uploaded_file in uploaded_files:
            try:
                content = uploaded_file.read().decode("utf-8")
                _validate_sourmash_signature(content)
            except Exception as ex:
                raise HttpError(400, "Unable to parse the uploaded file.") from ex
            finally:
                if hasattr(uploaded_file, "seek"):
                    uploaded_file.seek(0)

            uploads.append(
                {
                    "original_name": uploaded_file.name,
                    "saved_name": _save_signature(uploaded_file),
                }
            )

        try:
            job_id, children_ids = _send_sourmash_jobs(uploads, mag_catalogues)
        except OperationalError as ex:
            logger.exception("Sourmash queue backend is unavailable")
            raise HttpError(
                503,
                "Sourmash queue backend is unavailable. "
                "Check Redis/Celery configuration and worker availability.",
            ) from ex

        return SourmashGatherSubmissionOut(
            data=SourmashGatherSubmissionData(
                message=(
                    f"Your files {','.join(upload['original_name'] for upload in uploads)} "
                    "were successfully uploaded. Use the given URL to check the status of the new job."
                ),
                job_id=job_id,
                children_ids=children_ids,
                signatures_received=[upload["original_name"] for upload in uploads],
                status_url=_absolute_api_url(
                    request, f"genomes-search/status/{job_id}/"
                ),
            )
        )

    @http_get(
        "/status/{job_id}/",
        response=SourmashGatherStatusOut,
        summary="Check the status of a submitted sourmash gather group job",
        operation_id="genome_search_gather_status",
    )
    def gather_status(self, request, job_id: str):
        response = _get_sourmash_job_status(job_id, request)
        if response is None:
            raise Http404
        return SourmashGatherStatusOut(data=response)

    @http_get(
        "/results/{job_id}/",
        summary="Download a sourmash gather CSV result or grouped tarball",
        operation_id="genome_search_gather_results",
    )
    def gather_results(self, job_id: str):
        file_path, content_type = _get_result_file(job_id)
        if file_path is None or content_type is None:
            raise Http404
        return FileResponse(
            file_path.open("rb"),
            content_type=content_type,
            as_attachment=True,
            filename=f"{job_id}.csv" if content_type == "text/csv" else f"{job_id}.tgz",
        )
