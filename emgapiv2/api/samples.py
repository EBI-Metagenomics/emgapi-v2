from typing import Literal, Optional

from ninja import Field, Query
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.exceptions import NotFound
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import (
    AnalysedRun,
    AssemblyDetail,
    MGnifySample,
    MGnifySampleDetail,
    OrderByFilter,
)
from emgapiv2.api import perms
from emgapiv2.api.auth import DjangoSuperUserAuth, NoAuth, WebinJWTAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    ApiSections,
    BiomeFilter,
    make_child_link,
    make_links_section,
    make_related_detail_link,
)


class SampleListFilters(BiomeFilter):
    search: Optional[str] = Field(
        None,
        description="Search within sample titles and accessions",
        q=["sample_title__icontains", "ena_accessions__icontains"],
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
            {
                **make_related_detail_link(
                    related_detail_operation_id="get_mgnify_study",
                    related_object_name="study",
                    self_object_name="sample",
                    related_id_in_response="accession",
                    from_list_to_detail=True,
                    from_list_at_path="studies/",
                ),
                **make_child_link(
                    operation_id="list_sample_runs",
                    child_name="runs",
                    self_object_name="sample",
                    description="Runs associated with this sample",
                ),
                **make_child_link(
                    operation_id="list_sample_assemblies",
                    child_name="assemblies",
                    self_object_name="sample",
                    description="Assemblies associated with this sample",
                ),
            }
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
    def list_mgnify_samples(
        self,
        filters: SampleListFilters = Query(...),
        order: OrderByFilter[
            Literal["sample_title", "-sample_title", "updated_at", "-updated_at", ""]
        ] = Query(...),
    ):
        qs = analyses.models.Sample.public_objects.all().prefetch_related("studies")
        qs = order.order_by(qs)
        qs = filters.filter(qs)
        return qs

    @http_get(
        "/{accession}/runs/",
        response=NinjaPaginationResponseSchema[AnalysedRun],
        summary="List ENA Runs associated with this sample",
        description=(
            "Samples may be associated with one or more ENA runs. "
            "ENA runs 'Hold raw read files and sequencing methods'"
        ),
        operation_id="list_sample_runs",
        tags=[ApiSections.SAMPLES, ApiSections.RUNS],
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_analysed_run",
                self_object_name="sample",
                related_object_name="run",
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
    def list_sample_runs(self, accession: str):
        sample = analyses.models.Sample.objects.get_by_accession(accession)
        self.check_object_permissions(sample)
        return sample.runs.all()

    @http_get(
        "/{accession}/assemblies/",
        response=NinjaPaginationResponseSchema[AssemblyDetail],
        summary="List assemblies associated with this sample",
        description=(
            "Samples may be associated with one or more assemblies. "
            "Assemblies are collections of contigs generated from sequencing reads."
        ),
        operation_id="list_sample_assemblies",
        tags=[ApiSections.SAMPLES, ApiSections.ASSEMBLIES],
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_assembly",
                self_object_name="sample",
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
    def list_sample_assemblies(self, accession: str):
        sample = analyses.models.Sample.objects.get_by_accession(accession)
        self.check_object_permissions(sample)
        return sample.assemblies.all()
