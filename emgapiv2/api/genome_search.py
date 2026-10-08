import json
from uuid import UUID

from django.conf import settings
from django.http import Http404
from django_tasks.exceptions import TaskResultDoesNotExist, TaskResultMismatch
from ninja import Schema
from ninja.errors import HttpError
from ninja_extra import api_controller, http_get, http_post

from emgapiv2.api.schema_utils import ApiSections
from genomes.lexicmap_schema import LexicMapMatch, SearchQuery
from genomes.models import CatalogueGenome, GenomeCatalogue, GenomeSearchIndex
from genomes.schemas import GenomeList
from genomes.tasks import run_lexicmap_search

EMG_CONFIG = settings.EMG_CONFIG


class AnnotatedResult(Schema):
    mgnify: GenomeList
    lexicmap: LexicMapMatch


class GenomeSearchData(Schema):
    job_id: str
    status: str
    status_url: str
    query: str | None = None
    results: list[AnnotatedResult] | None = None


class GenomeFragmentSearchOut(Schema):
    data: GenomeSearchData


def _parse_request(request) -> SearchQuery:
    try:
        content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
        if content_type in ("multipart/form-data", "application/x-www-form-urlencoded"):
            data = request.POST.dict()
            if "catalogues_filter" in request.POST:
                data["catalogues_filter"] = request.POST.getlist("catalogues_filter")
            upload = request.FILES.get("sequence_file")
            if upload:
                if upload.size > 100_000:
                    raise HttpError(413, "Sequence file exceeds 100 KB")
                if data.get("sequence") or data.get("seq"):
                    raise HttpError(400, "Provide sequence text or a file, not both")
                data["sequence"] = upload.read(100_001).decode("utf-8")
        else:
            data = json.loads(request.body or b"{}")
        if isinstance(data, dict) and "seq" in data:
            data["sequence"] = data.pop("seq")
        return SearchQuery.model_validate(data)
    except ValueError as exc:
        raise HttpError(
            400,
            "Invalid search. Provide one sequence of 50–100000 bases; use min_identity and min_query_coverage (0–100). COBS threshold and kmer_size are no longer supported.",
        ) from exc


def _select_indexes(query):
    indexes = GenomeSearchIndex.objects.filter(
        backend=GenomeSearchIndex.Backend.LEXICMAP,
        status=GenomeSearchIndex.Status.ACTIVE,
        is_active=True,
        catalogue__in=GenomeCatalogue.public_objects.all(),
    )
    if query.catalogues_filter is not None:
        indexes = indexes.filter(catalogue_id__in=query.catalogues_filter)
    selected = list(indexes.values("catalogue_id", "artifact_path"))
    if query.catalogues_filter and set(query.catalogues_filter) - {
        i["catalogue_id"] for i in selected
    }:
        raise HttpError(400, "One or more catalogues has no published LexicMap index")
    return [
        {"catalogue": i["catalogue_id"], "path": i["artifact_path"]} for i in selected
    ]


def _annotate_results(results, selected_catalogues):
    snapshots = CatalogueGenome.public_objects.filter(
        genome__accession__in={r.genome for r in results},
        catalogue_id__in=selected_catalogues,
    ).select_related("genome", "catalogue", "catalogue__series", "biome")
    by_key = {(g.catalogue_id, g.accession): g for g in snapshots}
    return [
        AnnotatedResult(
            mgnify=GenomeList.from_orm(by_key[(r.catalogue, r.genome)]), lexicmap=r
        )
        for r in results
        if (r.catalogue, r.genome) in by_key
    ]


@api_controller("genomes/gene-search", tags=[ApiSections.GENOMES])
class GenomeSearchController:
    @http_post(
        "/",
        response={202: GenomeFragmentSearchOut},
        summary="Submit a nucleotide sequence for LexicMap search",
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {"schema": SearchQuery.model_json_schema()},
                },
            }
        },
        operation_id="genome_fragment_search",
    )
    def genome_fragment_search(self, request):
        query = _parse_request(request)
        indexes = _select_indexes(query)
        try:
            job = run_lexicmap_search.enqueue(
                request_payload={"query": query.model_dump(), "indexes": indexes}
            )
        except Exception as exc:
            raise HttpError(503, "Genome search queue is unavailable") from exc
        return 202, self._response(job)

    @http_get(
        "/status/{job_id}/",
        response=GenomeFragmentSearchOut,
        summary="Get LexicMap search status and annotated results",
        operation_id="genome_fragment_search_status",
    )
    def genome_fragment_search_status(self, request, job_id: UUID):
        try:
            job = run_lexicmap_search.get_result(str(job_id))
        except (TaskResultDoesNotExist, TaskResultMismatch) as exc:
            raise Http404 from exc
        return self._response(job, include_results=True)

    @staticmethod
    def _response(job, include_results=False):
        data = GenomeSearchData(
            job_id=job.id,
            status=job.status,
            status_url=f"{EMG_CONFIG.service_urls.app_root.rstrip('/')}/{settings.BASE_URL}genomes/gene-search/status/{job.id}/",
        )
        if include_results and job.status == "SUCCESSFUL":
            payload = job.return_value
            data.query = payload["query"]
            data.results = _annotate_results(
                [LexicMapMatch(**match) for match in payload["results"]],
                payload["catalogues"],
            )
        return GenomeFragmentSearchOut(data=data)
