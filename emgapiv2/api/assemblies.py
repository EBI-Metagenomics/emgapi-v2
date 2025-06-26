from typing import List
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated

import analyses.models
from analyses.schemas import (
    Assembly,
    AssemblyDetail,
)
from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link

router = RouterPaginated()


@router.get(
    "/",
    response=List[Assembly],
    summary="List all assemblies available from MGnify",
    description="Each assembly represents a metagenome assembly created from raw reads. "
    "This endpoint is accessible at `/analyses/assemblies/`.",
    operation_id="list_assemblies",
)
def list_assemblies(request):
    qs = analyses.models.Assembly.public_objects.all()
    return qs


@router.get(
    "/{accession}",
    response=AssemblyDetail,
    summary="Get assembly by accession",
    description="Get detailed information about a specific assembly. "
    "This endpoint is accessible at `/assemblies/{accession}`.",
    operation_id="get_assembly",
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_mgnify_study",
            related_object_name="study",
            self_object_name="assembly",
            related_id_in_response="reads_study_accession",
        )
    ),
)
def get_assembly(request, accession: str):
    assembly = get_object_or_404(
        analyses.models.Assembly.public_objects.select_related(
            "run", "sample", "reads_study", "assembly_study", "assembler"
        ).prefetch_related("genome_links__genome"),
        ena_accessions__contains=[accession],
    )
    return assembly
