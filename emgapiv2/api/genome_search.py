import logging
from typing import List, Optional, Dict, Any, Tuple
import json
from json import JSONDecodeError

import requests
from django.conf import settings
from ninja import Schema, Field
from ninja.errors import HttpError
from ninja_extra import api_controller, http_post

from emgapiv2.api.schema_utils import ApiSections
from genomes.models import Genome
from genomes.schemas import GenomeList

logger = logging.getLogger(__name__)


class GenomeFragmentSearchIn(Schema):
    sequence: Optional[str] = Field(None, description="FASTA or raw sequence")
    kmer_size: Optional[int] = Field(None, ge=1)
    max_results: Optional[int] = Field(None, ge=1)
    threshold: Optional[float] = None
    catalogues_filter: Optional[List[str]] = None


class CobsMatch(Schema):
    genome: str
    percent_kmers_found: Optional[float] = 0.0
    num_kmers: Optional[int] = None
    num_kmers_found: Optional[int] = None


class AnnotatedResult(Schema):
    mgnify: GenomeList
    cobs: CobsMatch


class GenomeSearchData(Schema):
    query: Optional[str] = None
    threshold: Optional[float] = None
    results: List[AnnotatedResult]


class GenomeFragmentSearchOut(Schema):
    data: GenomeSearchData


def _backend_url() -> str:
    return getattr(
        settings,
        "GENOME_SEARCH_PROXY",
        "https://cobs-genome-search-01.mgnify.org/search",
    )


def _parse_request(request) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool]:
    """
    Returns (payload, files, as_form).

    Ninja binds its parsing strategy to type annotations, so a single handler
    cannot natively accept both JSON and multipart for the same fields. This
    function handles that dispatch manually, keeping the handler thin.

    `as_form` is True for multipart/form requests and tells _post_to_backend
    to forward as form data rather than JSON.
    """
    content_type = request.headers.get("content-type", "").lower()
    is_form = (
        content_type.startswith("multipart/")
        or content_type.startswith("application/x-www-form-urlencoded")
        or (hasattr(request, "POST") and (bool(request.POST) or bool(getattr(request, "FILES", None))))
    )

    if is_form:
        seq = request.POST.get("sequence") or request.POST.get("seq")
        payload: Dict[str, Any] = {
            k: v
            for k, v in {
                "seq": seq,
                "kmer_size": request.POST.get("kmer_size"),
                "max_results": request.POST.get("max_results"),
                "threshold": request.POST.get("threshold"),
            }.items()
            if v not in (None, "")
        }
        for field, coerce in (("kmer_size", int), ("max_results", int), ("threshold", float)):
            if field in payload:
                try:
                    payload[field] = coerce(payload[field])  # type: ignore[operator]
                except ValueError:
                    del payload[field]

        if "catalogues_filter" in request.POST:
            payload["catalogues_filter"] = request.POST.getlist("catalogues_filter")

        files = None
        if "sequence_file" in request.FILES:
            f = request.FILES["sequence_file"]
            files = {
                "sequence_file": (
                    getattr(f, "name", "sequence_file"),
                    f,
                    getattr(f, "content_type", "application/octet-stream") or "application/octet-stream",
                )
            }
        return payload, files, True

    try:
        data = json.loads(request.body or b"{}")
    except (JSONDecodeError, ValueError):
        data = {}
    body = GenomeFragmentSearchIn(**data)
    payload = body.dict(exclude_none=True)
    if "sequence" in payload:
        payload["seq"] = payload.pop("sequence")
    return payload, None, False

def _post_to_backend(payload: Dict[str, Any], files: Optional[Dict[str, Any]] = None, as_form: bool = False) -> Dict[str, Any]:
    url = _backend_url()
    try:
        if files or as_form:
            resp = requests.post(url, data=payload, files=files, timeout=30)
        else:
            resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as ex:
        status = ex.response.status_code
        logger.error("Genome search backend returned %s: %s", status, ex.response.text[:500])
        if status >= 500:
            raise HttpError(502, "Genome search backend is unavailable. Please try later.")
        raise HttpError(400, "Genome search request was rejected by the backend.")
    except requests.exceptions.RequestException as ex:
        logger.exception("Failed to reach genome search backend at %s", url)
        raise HttpError(503, "Genome search is temporarily unavailable. Please try later.") from ex

    try:
        return resp.json()
    except (JSONDecodeError, ValueError) as ex:
        logger.exception("Failed to decode JSON from genome search backend")
        raise HttpError(502, "Genome search backend returned an invalid response.") from ex


def _annotate_results(raw_results: List[Dict[str, Any]]) -> List[AnnotatedResult]:
    cobs_result_by_accession = {r.get("genome"): r for r in raw_results if r.get("genome")}
    if not cobs_result_by_accession:
        return []

    genomes = (
        Genome.objects.filter(accession__in=list(cobs_result_by_accession.keys()))
        .select_related("catalogue", "biome")
        .all()
    )

    annotated: List[AnnotatedResult] = []
    for genome in genomes:
        cobs_result = cobs_result_by_accession.get(genome.accession)
        if not cobs_result:
            continue
        annotated.append(
            AnnotatedResult(
                mgnify=GenomeList.from_orm(genome),
                cobs=CobsMatch(**cobs_result),
            )
        )

    annotated.sort(key=lambda r: (r.cobs.percent_kmers_found or 0), reverse=True)
    return annotated


@api_controller("genome-search", tags=[ApiSections.GENOMES])
class GenomeSearchController():
    @http_post(
        "/",
        response=GenomeFragmentSearchOut,
        summary="Search genomes by short sequence and annotate with MGnify metadata",
        operation_id="genome_fragment_search",
    )
    def genome_fragment_search(self, request):
        payload, files, as_form = _parse_request(request)
        backend = _post_to_backend(payload, files=files, as_form=as_form)
        results = backend.get("results") or []
        annotated = _annotate_results(results)
        return GenomeFragmentSearchOut(
            data=GenomeSearchData(
                query=payload.get("seq"),
                threshold=payload.get("threshold"),
                results=annotated,
            )
        )
