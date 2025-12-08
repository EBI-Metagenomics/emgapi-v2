import logging
from typing import List, Optional, Dict, Any
import json
from json import JSONDecodeError

import requests
from django.conf import settings
from django.http import Http404
from ninja import Schema, Field, File, Form
from ninja.files import UploadedFile
from ninja_extra import api_controller, http_post

from emgapiv2.api.perms import UnauthorisedIsUnfoundController
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


class AnnotatedResult(Schema):
    mgnify: GenomeList
    cobs: CobsMatch


class GenomeFragmentSearchOut(Schema):
    results: List[AnnotatedResult]


def _backend_url() -> str:
    # Use the provided default if not set in Django settings
    return getattr(
        settings,
        "GENOME_SEARCH_PROXY",
        "https://cobs-genome-search-01.mgnify.org/search",
    )


def _post_to_backend(payload: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = _backend_url()
    try:
        if files:
            resp = requests.post(url, data=payload, files=files, timeout=30)
        else:
            resp = requests.post(url, json=payload, timeout=30)
    except requests.exceptions.RequestException as ex:
        logger.exception("Failed to talk to genome search backend at %s", url)
        raise Http404("Genome search failed. Please try later.") from ex

    if not 200 <= resp.status_code < 300:
        logger.error("Genome search backend returned %s: %s", resp.status_code, resp.text[:500])
        raise Http404("Genome search failed. Please try later.")

    try:
        return resp.json()
    except (JSONDecodeError, ValueError) as ex:
        logger.exception("Failed to decode JSON from genome search backend")
        raise Http404("Genome search failed. Please try later.") from ex


def _annotate_results(raw_results: List[Dict[str, Any]]) -> List[AnnotatedResult]:
    matches = {r.get("genome"): r for r in raw_results if r.get("genome")}
    if not matches:
        return []

    genomes = (
        Genome.objects.filter(accession__in=list(matches.keys()))
        .select_related("catalogue", "biome")
        .all()
    )

    annotated: List[AnnotatedResult] = []
    for genome in genomes:
        m = matches.get(genome.accession)
        if not m:
            continue
        annotated.append(
            AnnotatedResult(
                mgnify=GenomeList.from_orm(genome),
                cobs=CobsMatch(**m),
            )
        )

    annotated.sort(key=lambda r: (r.cobs.percent_kmers_found or 0), reverse=True)
    return annotated


@api_controller("genome-search", tags=[ApiSections.GENOMES])
class GenomeSearchController(UnauthorisedIsUnfoundController):
    @http_post(
        "/",
        response=GenomeFragmentSearchOut,
        summary="Search genomes by short sequence and annotate with MGnify metadata",
        operation_id="genome_fragment_search",
    )
    def genome_fragment_search_json(self, request, body: Optional[GenomeFragmentSearchIn] = None):
        # Accept both JSON and multipart/form-data on the same endpoint for client convenience
        content_type = request.headers.get("content-type", "").lower()
        files = None
        if content_type.startswith("multipart/"):
            # Build payload from form-data
            seq = request.POST.get("sequence") or request.POST.get("seq")
            payload: Dict[str, Any] = {
                k: v
                for k, v in {
                    "sequence": seq,
                    "kmer_size": request.POST.get("kmer_size"),
                    "max_results": request.POST.get("max_results"),
                    "threshold": request.POST.get("threshold"),
                }.items()
                if v not in (None, "")
            }
            # Coerce numeric fields if present
            if "kmer_size" in payload:
                try:
                    payload["kmer_size"] = int(payload["kmer_size"])  # type: ignore[index]
                except ValueError:
                    del payload["kmer_size"]
            if "max_results" in payload:
                try:
                    payload["max_results"] = int(payload["max_results"])  # type: ignore[index]
                except ValueError:
                    del payload["max_results"]
            if "threshold" in payload:
                try:
                    payload["threshold"] = float(payload["threshold"])  # type: ignore[index]
                except ValueError:
                    del payload["threshold"]

            if "catalogues_filter" in request.POST:
                payload["catalogues_filter"] = request.POST.getlist("catalogues_filter")

            if "sequence_file" in request.FILES:
                f = request.FILES["sequence_file"]
                files = {
                    "sequence_file": (
                        getattr(f, "name", "sequence_file"),
                        f,
                        getattr(f, "content_type", "application/octet-stream") or "application/octet-stream",
                    )
                }
        else:
            # JSON or x-www-form-urlencoded automatically parsed by Ninja
            payload = (body.dict(exclude_none=True) if body else {})

        backend = _post_to_backend(payload, files=files)
        results = backend.get("results") or []
        annotated = _annotate_results(results)
        return GenomeFragmentSearchOut(results=annotated)

    @http_post(
        "/:multipart",
        response=GenomeFragmentSearchOut,
        summary="Search genomes by sequence file (multipart/form-data)",
        operation_id="genome_fragment_search_multipart",
    )
    def genome_fragment_search_multipart(
        self,
        request,
        sequence: Optional[str] = Form(None),
        seq: Optional[str] = Form(None),
        kmer_size: Optional[int] = Form(None),
        max_results: Optional[int] = Form(None),
        threshold: Optional[float] = Form(None),
        sequence_file: Optional[UploadedFile] = File(None),
    ):
        # Prefer 'sequence'; fall back to 'seq' alias for compatibility
        effective_sequence = sequence if sequence is not None else seq

        payload: Dict[str, Any] = {
            k: v
            for k, v in {
                "sequence": effective_sequence,
                "kmer_size": kmer_size,
                "max_results": max_results,
                "threshold": threshold,
            }.items()
            if v is not None
        }

        # Collect repeated form fields
        if hasattr(request, "POST") and "catalogues_filter" in request.POST:
            payload["catalogues_filter"] = request.POST.getlist("catalogues_filter")

        files = None
        if sequence_file is not None:
            files = {
                "sequence_file": (
                    sequence_file.name,
                    sequence_file,
                    sequence_file.content_type or "application/octet-stream",
                )
            }

        backend = _post_to_backend(payload, files=files)
        results = backend.get("results") or []
        annotated = _annotate_results(results)
        return GenomeFragmentSearchOut(results=annotated)
