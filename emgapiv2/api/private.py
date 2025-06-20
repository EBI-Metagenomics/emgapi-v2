from ninja_extra import api_controller, ControllerBase, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import (
    MGnifyStudy,
)
from emgapiv2.api import ApiSections
from emgapiv2.api.auth import DjangoSuperUserAuth, WebinJWTAuth


@api_controller("my-data", tags=[ApiSections.PRIVATE_DATA])
class MyDataController(ControllerBase):
    @http_get(
        "/studies/",
        response=NinjaPaginationResponseSchema[MGnifyStudy],
        tags=[ApiSections.PRIVATE_DATA],
        summary="List all private studies available from MGnify",
        operation_id="list_private_mgnify_studies",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth()],
    )
    @paginate()
    def list_private_mgnify_studies(self):
        auth = self.context.request.auth
        qs = analyses.models.Study.objects

        if auth and auth.is_superuser:
            qs = qs.all()
        elif auth and (webin_id := auth.username):
            qs = qs.filter(webin_submitter=webin_id)
        else:
            qs = qs.none()

        return qs
