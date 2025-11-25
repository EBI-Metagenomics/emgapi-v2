from typing import List
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated

import analyses.models
from analyses.schemas import (
    Assembly,
    AssemblyDetail, MGnifyAnalysis, GenomeAssemblyLinkSchema,
)
from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link, make_child_link
from genomes.models import GenomeAssemblyLink

router = RouterPaginated()


@router.get(
    "/",
    response=List[Assembly],
    summary="List all assemblies available in MGnify",
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
        {
            **make_related_detail_link(
                related_detail_operation_id="get_mgnify_study",
                related_object_name="study",
                self_object_name="assembly",
                related_id_in_response="reads_study_accession",
            ),
            **make_related_detail_link(
                related_detail_operation_id="get_mgnify_sample",
                related_object_name="study",
                self_object_name="analysis",
                related_id_in_response="study_accession",
            ),
            **make_child_link(
                operation_id="list_genome_links_for_assembly",
                child_name="genome-links",
                self_object_name="assembly",
                description="Genome/MAG links for this assembly",
            )
        }
    ),
)
def get_assembly(request, accession: str):
    assembly = get_object_or_404(
        analyses.models.Assembly.public_objects
        .select_related("run", "sample", "reads_study", "assembly_study", "assembler"),
        ena_accessions__contains=[accession],
    )
    return assembly


@router.get(
    "/{accession}/genome-links",
    response=List[GenomeAssemblyLinkSchema],
    summary="List genome/MAG links for an assembly",
    description=(
        "Return genome links (e.g. MAG associations) for a specific assembly.\n"
        "Accessible at `/assemblies/{accession}/genome-links`."
    ),
    operation_id="list_genome_links_for_assembly",
)
def list_genome_links_for_assembly(request, accession: str):
    # Ensure assembly exists and is public
    assembly = get_object_or_404(
        analyses.models.Assembly.public_objects,
        ena_accessions__contains=[accession],
    )

    return analyses.models.Assembly.public_objects.all()



@router.get(
    "/{accession}/analyses",
    response=List[MGnifyAnalysis],
    summary="List analyses for an assembly",
    description=(
        "Return MGnify analyses (MGYAs) that were generated from this assembly.\n"
        "Accessible at `/assemblies/{accession}/analyses`."
    ),
    operation_id="list_analyses_for_assembly",
)
def list_analyses_for_assembly(request, accession: str):
    get_object_or_404(
        analyses.models.Assembly.public_objects,
        ena_accessions__contains=[accession],
    )
    qs = analyses.models.Analysis.public_objects.select_related(
        "study", "sample", "run", "assembly"
    ).filter(
        assembly__ena_accessions__contains=[accession]
    )
    return qs