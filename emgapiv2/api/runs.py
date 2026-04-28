from emgapiv2.api.schema_utils import (
    make_child_link,
    make_links_section,
    make_related_detail_link,
)
from ninja_extra import api_controller, http_get
from ninja_extra.pagination import paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from ninja_extra.exceptions import NotFound
from ninja import FilterSchema

import analyses.models
from analyses.schemas import (
    AnalysedRun,
    AnalysedRunDetail,
    MGnifyAnalysis,
    AssemblyDetail,
)
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

    def filter_has_experiment_type(self, experiment_type: Optional[str]) -> Q:
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
        openapi_extra=make_links_section(
            {
                **make_related_detail_link(
                    related_detail_operation_id="get_analysed_run",
                    self_object_name="run",
                    related_object_name="run",
                    related_id_in_response="accession",
                ),
                **make_related_detail_link(
                    related_detail_operation_id="get_mgnify_sample",
                    self_object_name="run",
                    related_object_name="sample",
                    related_id_in_response="sample_accession",
                ),
                **make_related_detail_link(
                    related_detail_operation_id="get_mgnify_study",
                    self_object_name="run",
                    related_object_name="study",
                    related_id_in_response="study_accession",
                ),
            }
        ),
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
        qs = analyses.models.Run.public_objects.select_related(
            "sample",
            "sample__ena_sample",
            "study",
        )
        qs = filters.filter(qs)
        return qs

    @http_get(
        "/{accession}",
        response=AnalysedRunDetail,
        summary="Get the detail of a single analysed run",
        description="Get the detail of a single analysed run in the MGnify database.",
        operation_id="get_analysed_run",
        openapi_extra=make_links_section(
            {
                **make_related_detail_link(
                    related_detail_operation_id="get_mgnify_sample",
                    self_object_name="run",
                    related_object_name="sample",
                    related_id_in_response="sample_accession",
                ),
                **make_related_detail_link(
                    related_detail_operation_id="get_mgnify_study",
                    self_object_name="run",
                    related_object_name="study",
                    related_id_in_response="study_accession",
                ),
                **make_child_link(
                    operation_id="list_runs_analyses",
                    child_name="analyses",
                    self_object_name="run",
                    description="Analyses performed on this run",
                ),
                **make_child_link(
                    operation_id="list_runs_assemblies",
                    child_name="assemblies",
                    self_object_name="run",
                    description="Assemblies generated from or including this run",
                ),
            }
        ),
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic
            | (perms.IsWebinOwner & perms.IsReady)
            | perms.IsAdminUserWithObjectPerms
        ],
    )
    def get_analysed_run(self, accession: str):
        try:
            run = analyses.models.Run.objects.get_by_accession(accession)
        except (
            analyses.models.Run.DoesNotExist,
            analyses.models.Run.MultipleObjectsReturned,
        ):
            raise NotFound(detail=f"Analysed run with accession {accession} not found.")
        # raise not found if user doesn't have permission to see
        self.check_object_permissions(run)
        return run

    @http_get(
        "/{accession}/analyses/",
        response=NinjaPaginationResponseSchema[MGnifyAnalysis],
        summary="List MGnify Analyses associated with this Run",
        description="MGnify analyses correspond to an individual Run or Assembly",
        operation_id="list_runs_analyses",
        tags=[ApiSections.RUNS, ApiSections.ANALYSES],
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_mgnify_analysis",
                self_object_name="run",
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
    def list_runs_analyses(self, accession: str):
        try:
            run = analyses.models.Run.objects.get_by_accession(accession)
        except (
            analyses.models.Run.DoesNotExist,
            analyses.models.Run.MultipleObjectsReturned,
        ):
            raise NotFound(detail=f"Analysed run with accession {accession} not found.")
        # raise not found if user doesn't have permission to see
        self.check_object_permissions(run)
        return run.analyses.filter(is_ready=True)

    @http_get(
        "/{accession}/assemblies/",
        response=NinjaPaginationResponseSchema[AssemblyDetail],
        summary="List Assemblies associated with this Run",
        description="Assemblies generated from or including this run",
        operation_id="list_runs_assemblies",
        tags=[ApiSections.RUNS, ApiSections.ASSEMBLIES],
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_assembly",
                self_object_name="run",
                related_object_name="assembly",
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
    def list_runs_assemblies(self, accession: str):
        try:
            run = analyses.models.Run.objects.get_by_accession(accession)
        except (
            analyses.models.Run.DoesNotExist,
            analyses.models.Run.MultipleObjectsReturned,
        ):
            raise NotFound(detail=f"Analysed run with accession {accession} not found.")
        # raise not found if user doesn't have permission to see
        self.check_object_permissions(run)
        return (
            analyses.models.Assembly.public_objects.select_related(
                "reads_study", "assembly_study", "assembler", "sample"
            )
            .prefetch_related("runs")
            .filter(runs__ena_accessions__contains=[accession])
            .distinct()
        )
