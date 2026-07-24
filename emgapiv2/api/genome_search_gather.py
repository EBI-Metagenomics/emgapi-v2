import hashlib
import json
import logging
import tarfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from django.conf import settings
from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db.models import Case, IntegerField, When
from django.http import FileResponse, Http404
from ninja import Schema
from ninja.errors import HttpError
from ninja_extra import api_controller, http_get, http_post

from emgapiv2.api.schema_utils import ApiSections
from emgapiv2.tasking import default_task_backend
from genomes.models import GenomeSearchIndex
from genomes.tasks import run_sourmash_gather_request

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


class SourmashGatherSubmissionData(Schema):
    message: str
    job_id: str
    status: str
    children_ids: Dict[str, str]
    signatures_received: List[str]
    requested_catalogues: List[str]
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
    status: str
    signatures: List[SourmashGatherSignatureStatus]
    results_url: Optional[str] = None
    worker_status: str


class SourmashGatherStatusOut(Schema):
    data: SourmashGatherStatusData


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


def _save_signature(uploaded_file, job_id: str) -> str:
    query_dir = Path(EMG_CONFIG.sourmash.queries_path) / job_id
    query_dir.mkdir(parents=True, exist_ok=True)
    destination_path = query_dir / _get_unique_name(uploaded_file)
    if destination_path.exists():
        return str(destination_path)

    with destination_path.open("wb") as destination:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        for chunk in uploaded_file.chunks():
            destination.write(chunk)
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    return str(destination_path)


def _build_item_results_path(job_id: str, item_id: uuid.UUID) -> str:
    return str(Path(EMG_CONFIG.sourmash.results_path) / job_id / f"{item_id}.csv")


def _parse_uuid(raw_id: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(raw_id))
    except (TypeError, ValueError):
        return None


def _active_sourmash_indexes_cache_identifier(mag_catalogues: set[str]) -> str:
    ordered_catalogues = ",".join(sorted(mag_catalogues))
    return (
        "genomes-search:active-sourmash-indexes:"
        f"{EMG_CONFIG.sourmash.default_ksize}:{ordered_catalogues}"
    )


def _get_active_sourmash_indexes(mag_catalogues: set[str]) -> List[GenomeSearchIndex]:
    cache_identifier = _active_sourmash_indexes_cache_identifier(mag_catalogues)
    cache_ttl = getattr(settings, "GENOME_SEARCH_INDEX_CACHE_TTL", 300)
    cached_ids = cache.get(cache_identifier)

    if cached_ids is None:
        cached_ids = list(
            GenomeSearchIndex.objects.filter(
                backend=GenomeSearchIndex.Backend.SOURMASH,
                status=GenomeSearchIndex.Status.ACTIVE,
                is_active=True,
                ksize=EMG_CONFIG.sourmash.default_ksize,
                catalogue__catalogue_id__in=mag_catalogues,
            )
            .order_by("catalogue__catalogue_id", "created_at")
            .values_list("id", flat=True)
        )
        cache.set(cache_identifier, cached_ids, timeout=cache_ttl)

    if not cached_ids:
        return []

    preserved_order = Case(
        *[
            When(id=index_id, then=position)
            for position, index_id in enumerate(cached_ids)
        ],
        output_field=IntegerField(),
    )
    return list(
        GenomeSearchIndex.objects.select_related("catalogue")
        .filter(id__in=cached_ids)
        .order_by(preserved_order)
    )


def _get_task_result(result_id: str):
    try:
        task_result = default_task_backend.get_result(result_id)
    except Exception:
        return None
    if hasattr(task_result, "refresh"):
        task_result.refresh()
    return task_result


def _task_result_status(task_result) -> str:
    raw_status = getattr(task_result, "status", "")
    status_name = getattr(raw_status, "name", str(raw_status)).upper()

    if "RUN" in status_name:
        return "RUNNING"
    if "SUCCESS" in status_name:
        try:
            payload = task_result.return_value
        except ValueError:
            return "RUNNING"
        return payload.get("status", "SUCCESS")
    if "FAIL" in status_name or getattr(task_result, "errors", None):
        return "FAILED"
    return "QUEUED"


def _get_sourmash_job_status(
    job_id: str, request
) -> Optional[SourmashGatherStatusData]:
    task_result = _get_task_result(job_id)
    if task_result is None:
        return None

    signatures = []
    has_results = False
    status = _task_result_status(task_result)

    if status in {"SUCCESS", "NO_RESULTS", "PARTIAL_SUCCESS", "FAILED"}:
        try:
            payload = task_result.return_value
        except ValueError:
            payload = {}

        for item in payload.get("signatures", []):
            signature = {
                "job_id": job_id,
                "status": item["status"],
                "filename": item.get("filename"),
                "catalogue": item.get("catalogue"),
            }
            if item.get("result"):
                signature["result"] = item["result"]
            if item.get("reason"):
                signature["reason"] = item["reason"]
            if item.get("raw_csv_path") and Path(item["raw_csv_path"]).exists():
                signature["results_url"] = _absolute_api_url(
                    request, f"genomes-search/results/{job_id}/"
                )
                has_results = True
            signatures.append(SourmashGatherSignatureStatus(**signature))
    elif getattr(task_result, "errors", None):
        reason = "\n".join(
            getattr(error, "traceback", "") or str(error)
            for error in task_result.errors[:1]
        )
        signatures.append(
            SourmashGatherSignatureStatus(
                job_id=job_id,
                status="FAILED",
                reason=reason,
            )
        )

    return SourmashGatherStatusData(
        group_id=job_id,
        status=status,
        signatures=signatures,
        results_url=(
            _absolute_api_url(request, f"genomes-search/results/{job_id}/")
            if has_results
            else None
        ),
        worker_status="UNKNOWN",
    )


def _generate_tgz_from_signature_results(
    task_id: str, signature_results: list[dict[str, Any]]
) -> Optional[Path]:
    archive_path = Path(EMG_CONFIG.sourmash.results_path) / task_id / f"{task_id}.tgz"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    added = False

    with tarfile.open(archive_path, "w:gz") as tar:
        for item in signature_results:
            raw_csv_path = item.get("raw_csv_path")
            if not raw_csv_path:
                continue
            csv_path = Path(raw_csv_path)
            if csv_path.exists():
                tar.add(csv_path, arcname=csv_path.name)
                added = True

    if not added:
        archive_path.unlink(missing_ok=True)
        return None

    return archive_path


def _get_result_file(job_id: str) -> tuple[Optional[Path], Optional[str]]:
    task_result = _get_task_result(job_id)
    if task_result is None:
        return None, None

    try:
        payload = task_result.return_value
    except ValueError:
        return None, None

    signature_results = payload.get("signatures", [])
    csv_paths = [
        Path(item["raw_csv_path"])
        for item in signature_results
        if item.get("raw_csv_path") and Path(item["raw_csv_path"]).exists()
    ]
    if len(csv_paths) == 1:
        return csv_paths[0], "text/csv"

    archive_path_value = payload.get("archive_path")
    if archive_path_value:
        archive_path = Path(archive_path_value)
        if archive_path.exists():
            return archive_path, "application/gzip"

    archive_path = _generate_tgz_from_signature_results(job_id, signature_results)
    if archive_path is not None and archive_path.exists():
        return archive_path, "application/gzip"

    return None, None


@api_controller("genomes-search", tags=[ApiSections.GENOMES])
class GenomeSearchGatherController:
    @http_post(
        "/gather/",
        response=SourmashGatherSubmissionOut,
        summary="Submit one or more sourmash gather jobs for uploaded signatures",
        operation_id="genome_search_gather_submit",
    )
    def submit_gather(self, request):
        # mag_catalogues = set(
        #     _normalize_form_list(request.POST.getlist("mag_catalogues"))
        # )
        mag_catalogues = set(request.POST.getlist("mag_catalogues"))
        if not mag_catalogues:
            raise HttpError(
                400, "A list of mag_catalogues to search against must be provided."
            )

        searchable_catalogues = set(
            GenomeSearchIndex.objects.filter(
                backend=GenomeSearchIndex.Backend.SOURMASH,
                status=GenomeSearchIndex.Status.ACTIVE,
                is_active=True,
                ksize=EMG_CONFIG.sourmash.default_ksize,
            ).values_list("catalogue__catalogue_id", flat=True)
        )
        bad_catalogues = mag_catalogues.difference(searchable_catalogues)
        if bad_catalogues:
            raise HttpError(
                400,
                "The provided mag_catalogues are not searchable. "
                f"Available searchable catalogues: {sorted(searchable_catalogues)}; "
                f"Unavailable: {sorted(bad_catalogues)}",
            )

        search_indexes = _get_active_sourmash_indexes(mag_catalogues)
        uploaded_files = _get_uploaded_files(request)
        if not uploaded_files:
            raise HttpError(400, "At least one file_uploaded entry must be provided.")

        for uploaded_file in uploaded_files:
            try:
                content = uploaded_file.read().decode("utf-8")
                _validate_sourmash_signature(content)
            except Exception as exc:
                raise HttpError(400, "Unable to parse the uploaded file.") from exc
            finally:
                if hasattr(uploaded_file, "seek"):
                    uploaded_file.seek(0)

        signature_names = [uploaded_file.name for uploaded_file in uploaded_files]
        requested_catalogues = sorted(mag_catalogues)
        request_id = str(uuid.uuid4())
        search_items: list[dict[str, Any]] = []

        try:
            for uploaded_file in uploaded_files:
                staged_path = _save_signature(uploaded_file, request_id)
                for search_index in search_indexes:
                    search_items.append(
                        {
                            "query_path": staged_path,
                            "original_filename": uploaded_file.name,
                            "catalogue_id": search_index.catalogue.catalogue_id,
                            "artifact_path": search_index.artifact_path,
                            "result_path": _build_item_results_path(
                                request_id,
                                uuid.uuid4(),
                            ),
                            "ksize": search_index.ksize,
                            "moltype": search_index.moltype,
                        }
                    )
            task_result = run_sourmash_gather_request.enqueue(
                request_payload={
                    "request_id": request_id,
                    "search_items": search_items,
                    "signatures_received": signature_names,
                    "requested_catalogues": requested_catalogues,
                }
            )
        except Exception as exc:
            logger.exception("Sourmash task backend is unavailable")
            raise HttpError(
                503,
                "Sourmash task backend is unavailable. "
                "Check Django Tasks configuration and worker availability.",
            ) from exc

        return SourmashGatherSubmissionOut(
            data=SourmashGatherSubmissionData(
                message=(
                    f"Your files {','.join(signature_names)} were successfully uploaded. "
                    "Use the given URL to check the status of the new job."
                ),
                job_id=task_result.id,
                status="QUEUED",
                children_ids={},
                signatures_received=signature_names,
                requested_catalogues=requested_catalogues,
                status_url=_absolute_api_url(
                    request, f"genomes-search/status/{task_result.id}/"
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
