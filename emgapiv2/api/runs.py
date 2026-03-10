from ninja_extra import api_controller, http_get
from ninja_extra.pagination import paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from ninja_extra.exceptions import NotFound

import analyses.models
from analyses.schemas import AnalysedRun, AnalysedRunDetail
from emgapiv2.api import ApiSections
from emgapiv2.api import perms
from emgapiv2.api.auth import WebinJWTAuth, DjangoSuperUserAuth, NoAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController


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
        # filters: BiomeListFilters = Query(...),
    ):
        qs = analyses.models.Run.public_objects.select_related()
        # qs = filters.filter(qs)
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
