from typing import List
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated

from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link
from genomes.models import Genome
from genomes.schemas import GenomeDetail, GenomeList, GenomeWithAnnotations

router = RouterPaginated(tags=["Genomes"])


@router.get(
    "/",
    response=List[GenomeList],
    summary="List all genomes",
    operation_id="list_genomes",
)
def list_genomes(request):
    return Genome.objects.select_related("biome", "catalogue").prefetch_related(
        "pangenome_geographic_range"
    )


@router.get(
    "/{accession}",
    response=GenomeDetail,
    summary="Get genome by accession",
    operation_id="get_genome",
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_genome_catalogue",
            related_object_name="catalogue",
            self_object_name="genome",
            related_id_in_response="catalogue_id",
        )
    ),
)
def get_genome(request, accession: str):
    genome = get_object_or_404(
        Genome.objects.select_related(
            "biome", "geo_origin", "catalogue"
        ).prefetch_related("pangenome_geographic_range"),
        accession=accession,
    )
    return genome


@router.get(
    "/{accession}/annotations",
    response=GenomeWithAnnotations,
    summary="Get genome annotations by accession",
    operation_id="get_genome_annotations",
)
def get_genome_annotations(request, accession: str):
    genome = get_object_or_404(Genome.objects_and_annotations, accession=accession)
    return genome
