from ninja_extra import api_controller, http_get
from ninja_extra.pagination import paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from ninja_extra.exceptions import NotFound
from ninja import FilterSchema

import analyses.models
from analyses.schemas import AnalysedRun, AnalysedRunDetail, MGnifyAnalysis
from emgapiv2.api import ApiSections
from emgapiv2.api import perms
from emgapiv2.api.auth import WebinJWTAuth, DjangoSuperUserAuth, NoAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from typing import Optional
from pydantic import Field
from django.db.models import Q
from ninja import Query


class RunListFilters(FilterSchema):
    has_experiment_type: Optional[analyses.models.Run.ExperimentTypes] = Field(
        None,
        description="If set, will only show runs with the specified experiment type",
    )

    def filter_has_experiment_type(self, experiment_type: str | None) -> Q:
        if not experiment_type:
            return Q()
        return Q(experiment_type=experiment_type)


@api_controller("runs", tags=[ApiSections.RUNS])
class AnalysedRunController(UnauthorisedIsUnfoundController):
    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[AnalysedRun],
        summary="List all analysed runs",
        description="List all analysed runs in the MGnify database.",
        operation_id="list_analysed_runs",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic
            | (perms.IsWebinOwner & perms.IsReady)
            | perms.IsAdminUserWithObjectPerms
        ],
    )
    @paginate()
    def list_analysed_runs(
        self,
        filters: RunListFilters = Query(...),
    ):
        qs = analyses.models.Run.public_objects.select_related()
        qs = filters.filter(qs)
        return qs

    @http_get(
        "/{accession}",
        response=AnalysedRunDetail,
        summary="Get the detail of a single analysed run",
        description="Get the detail of a single analysed run in the MGnify database.",
        operation_id="get_analysed_run",
    )
    def get_analysed_run(self, accession: str):
        try:
            run = analyses.models.Run.objects.get_by_accession(accession)
        except (
            analyses.models.Run.DoesNotExist,
            analyses.models.Run.MultipleObjectsReturned,
        ):
            raise NotFound(detail=f"Analysed run with accession {accession} not found.")
        return run

    @http_get(
        "/{accession}/analyses/",
        response=NinjaPaginationResponseSchema[MGnifyAnalysis],
        summary="List MGnify Analyses associated with this Run",
        description="MGnify analyses correspond to an individual Run or Assembly",
        operation_id="list_runs_analyses",
        tags=[ApiSections.RUNS, ApiSections.ANALYSES],
        # openapi_extra=make_links_section(
        #     make_related_detail_link(
        #         related_detail_operation_id="get_mgnify_analysis",
        #         self_object_name="run",
        #         related_object_name="analysis",
        #         related_id_in_response="accession",
        #         from_list_to_detail=True,
        #     )
        # ),
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    @paginate()
    def list_runs_analyses(self, accession: str):
        return self.get_object_or_exception(
            analyses.models.Run.objects, accession=accession
        ).analyses.filter(is_ready=True)
