from typing import List, Literal, Optional

from django.db.models import Q
from django.shortcuts import get_object_or_404
from ninja import Query
from ninja.pagination import RouterPaginated
from pydantic import Field

import analyses.models
from analyses.schemas import (
    MGnifyStudyDetail,
    MGnifyStudy,
    MGnifyAnalysis,
    OrderByFilter,
)
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    BiomeFilter,
)

router = RouterPaginated()


@router.get(
    "/{accession}",
    response=MGnifyStudyDetail,
    summary="Get the detail of a single study analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
    operation_id="get_mgnify_study",
)
def get_mgnify_study(request, accession: str):
    study = get_object_or_404(analyses.models.Study.public_objects, accession=accession)
    return study


class StudyListFilters(BiomeFilter):
    has_analyses_from_pipeline: Optional[analyses.models.Analysis.PipelineVersions] = (
        Field(
            None,
            description="If set, will only show studies with analyses from the specified MGnify pipeline version",
        )
    )

    def filter_has_analyses_from_pipeline(self, version: str | None) -> Q:
        if not version:
            return Q()
        if version == analyses.models.Analysis.PipelineVersions.v6:
            return Q(features__has_v6_analyses=True)
        if version == analyses.models.Analysis.PipelineVersions.v5:
            # TODO
            return Q(features__has_prev6_analyses=True)


@router.get(
    "/",
    response=List[MGnifyStudy],
    summary="List all studies analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
    operation_id="list_mgnify_studies",
)
def list_mgnify_studies(
    request,
    order: OrderByFilter[
        Literal["accession", "-accession", "updated_at", "-updated_at", ""]
    ] = Query(...),
    filters: StudyListFilters = Query(...),
):
    qs = analyses.models.Study.public_objects.all()
    qs = order.order_by(qs)
    qs = filters.filter(qs)
    return qs


@router.get(
    "/{accession}/analyses/",
    response=List[MGnifyAnalysis],
    summary="List MGnify Analyses associated with this Study",
    description="MGnify analyses correspond to an individual Run or Assembly within this study,"
    "analysed by a MGnify Pipelione. ",
    operation_id="list_mgnify_study_analyses",
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_mgnify_analysis",
            self_object_name="study",
            related_object_name="analysis",
            related_id_in_response="accession",
            from_list_to_detail=True,
        )
    ),
)
def list_mgnify_study_analyses(request, accession: str):
    study = get_object_or_404(analyses.models.Study.public_objects, accession=accession)
    return study.analyses.select_related("study", "sample", "run", "assembly")
