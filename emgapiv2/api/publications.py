from typing import Literal, Optional

from ninja import Query, FilterSchema, Field
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import (
    MGnifyPublication,
    MGnifyPublicationDetail,
    OrderByFilter,
    PublicationAnnotations,
)
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    ApiSections,
)
from emgapiv2.api.third_party_metadata import get_epmc_publication_annotations


class PublicationListFilters(FilterSchema):
    published_after: Optional[int] = Field(
        None, description="Filter by minimum publication year", q="published_year__gte"
    )
    published_before: Optional[int] = Field(
        None, description="Filter by maximum publication year", q="published_year__lte"
    )
    title: Optional[str] = Field(
        None, description="Search within publication titles", q="title__icontains"
    )


@api_controller("publications", tags=[ApiSections.PUBLICATIONS])
class PublicationController(UnauthorisedIsUnfoundController):
    @http_get(
        "/{pubmed_id}",
        response=MGnifyPublicationDetail,
        summary="Get the detail of a single publication",
        description="Get detailed information about a publication, including associated studies.",
        operation_id="get_mgnify_publication",
    )
    def get_mgnify_publication(self, pubmed_id: int):
        return self.get_object_or_exception(
            analyses.models.Publication.objects.get_queryset(), pubmed_id=pubmed_id
        )

    @http_get(
        "/{pubmed_id}/annotations",
        response=PublicationAnnotations,
        summary="Get any full-text annotations associated with the publication",
        description="Full-text annotations are retrieved from Europe PMC, text mined for relevant metagenomic metadata terms",
        operation_id="get_mgnify_publication_annotations",
    )
    def get_mgnify_publication_annotations(self, pubmed_id: int):
        self.get_object_or_exception(
            analyses.models.Publication.objects.get_queryset(), pubmed_id=pubmed_id
        )
        return get_epmc_publication_annotations(pubmed_id)

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[MGnifyPublication],
        summary="List all publications",
        description="List all publications in the MGnify database.",
        operation_id="list_mgnify_publications",
    )
    @paginate()
    def list_mgnify_publications(
        self,
        order: OrderByFilter[Literal["published_year", "-published_year", ""]] = Query(
            ...
        ),
        filters: PublicationListFilters = Query(...),
    ):
        qs = analyses.models.Publication.objects.all()
        qs = order.order_by(qs)
        qs = filters.filter(qs)
        return qs
