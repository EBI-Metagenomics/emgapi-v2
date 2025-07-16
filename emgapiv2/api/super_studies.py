from ninja_extra import api_controller, http_get, paginate, ControllerBase
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import SuperStudy, SuperStudyDetail
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    ApiSections,
)


@api_controller("super-studies", tags=[ApiSections.STUDIES])
class SuperStudyController(ControllerBase):
    @http_get(
        "/{slug}",
        response=SuperStudyDetail,
        summary="Get the detail of a single Super Study",
        description="A Super Study is a collection of MGnify Studies all related to a single large initiative.",
        operation_id="get_super_study",
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_mgnify_study",
                self_object_name="super_study",
                related_object_name="study",
                related_id_in_response="accession",
                from_list_to_detail=True,
                from_list_at_path="studies/",
            )
        ),
    )
    def get_super_study(self, slug: str):
        return self.get_object_or_exception(analyses.models.SuperStudy, slug=slug)

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[SuperStudy],
        summary="List all Super Studies",
        description="Super Studies are collections of MGnify Studies associated with major initiatives.",
        operation_id="list_super_studies",
        openapi_extra=make_links_section(
            make_related_detail_link(
                from_list_to_detail=True,
                related_detail_operation_id="get_super_study",
                self_object_name="super_study",
                related_object_name="super_study",
                related_id_in_response="slug",
                related_lookup_param="slug",
            )
        ),
    )
    @paginate(page_size=50)
    def list_super_studies(
        self,
    ):
        qs = analyses.models.SuperStudy.objects.order_by("-updated_at")
        return qs
