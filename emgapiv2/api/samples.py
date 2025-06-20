from ninja_extra import api_controller, http_get, paginate
from ninja_extra.exceptions import NotFound
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import MGnifySample, MGnifySampleDetail
from emgapiv2.api import perms
from emgapiv2.api.auth import WebinJWTAuth, NoAuth, DjangoSuperUserAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    ApiSections,
)


@api_controller("samples", tags=[ApiSections.SAMPLES])
class SampleController(UnauthorisedIsUnfoundController):
    @http_get(
        "/{accession}",
        response=MGnifySampleDetail,
        summary="Get the detail of a single sample analysed by MGnify",
        description="MGnify samples inherit directly from samples (or BioSamples) in ENA.",
        operation_id="get_mgnify_sample",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_mgnify_study",
                related_object_name="study",
                self_object_name="sample",
                related_id_in_response="accession",
                from_list_to_detail=True,
                from_list_at_path="studies/",
            )
        ),
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    def get_mgnify_sample(self, accession: str):
        try:
            sample = analyses.models.Sample.objects.get_by_accession(accession)
        except (
            analyses.models.Sample.DoesNotExist,
            analyses.models.Sample.MultipleObjectsReturned,
        ):
            raise NotFound(f"No sample available for accession {accession}")
        self.check_object_permissions(sample)
        return sample

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[MGnifySample],
        summary="List all samples analysed by MGnify",
        description="MGnify samples inherit directly from samples (or BioSamples) in ENA.",
        operation_id="list_mgnify_samples",
    )
    @paginate()
    def list_mgnify_samples(self):
        qs = analyses.models.Sample.public_objects.all().prefetch_related("studies")
        return qs
