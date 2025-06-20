from typing import Literal, Optional

from django.db.models import Q
from ninja import Query
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

import analyses.models
from analyses.schemas import (
    MGnifyStudyDetail,
    MGnifyStudy,
    MGnifyAnalysis,
    OrderByFilter,
)
from emgapiv2.api import perms
from emgapiv2.api.auth import WebinJWTAuth, DjangoSuperUserAuth, NoAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    BiomeFilter,
    ApiSections,
)


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
        return Q()


@api_controller("studies", tags=[ApiSections.STUDIES])
class StudyController(UnauthorisedIsUnfoundController):
    @http_get(
        "/{accession}",
        response=MGnifyStudyDetail,
        summary="Get the detail of a single study analysed by MGnify",
        description="MGnify studies inherit directly from studies (or projects) in ENA.",
        operation_id="get_mgnify_study",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    def get_mgnify_study(self, accession: str):
        return self.get_object_or_exception(
            analyses.models.Study.objects, accession=accession
        )

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[MGnifyStudy],
        summary="List all studies analysed by MGnify",
        description="MGnify studies inherit directly from studies (or projects) in ENA.",
        operation_id="list_mgnify_studies",
    )
    @paginate()
    def list_mgnify_studies(
        self,
        order: OrderByFilter[
            Literal["accession", "-accession", "updated_at", "-updated_at", ""]
        ] = Query(...),
        filters: StudyListFilters = Query(...),
    ):
        qs = analyses.models.Study.public_objects.all()
        qs = order.order_by(qs)
        qs = filters.filter(qs)
        return qs

    @http_get(
        "/{accession}/analyses/",
        response=NinjaPaginationResponseSchema[MGnifyAnalysis],
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
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    @paginate()
    def list_mgnify_study_analyses(self, accession: str):
        return self.get_object_or_exception(
            analyses.models.Study.objects, accession=accession
        ).analyses.all()
