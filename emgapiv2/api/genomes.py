from django.shortcuts import get_object_or_404
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    ApiSections,
)
from genomes.models import Genome, GenomeCatalogue
from genomes.schemas import GenomeDetail, GenomeList, GenomeWithAnnotations
from genomes.schemas.GenomeCatalogue import GenomeCatalogueList, GenomeCatalogueDetail


@api_controller("genomes", tags=[ApiSections.GENOMES])
class GenomeController(UnauthorisedIsUnfoundController):
    @http_get(
        "/{accession}",
        response=GenomeDetail,
        summary="Get the detail of a single MGnify Genome",
        description="MGnify Genomes are either isolates, or MAGs derived from binned metagenomes.",
        operation_id="get_mgnify_genome",
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_genome_catalogue",
                related_object_name="catalogue",
                self_object_name="genome",
                related_id_in_response="catalogue_id",
            )
        ),
    )
    def get_mgnify_genome(self, accession: str):
        genome = get_object_or_404(
            Genome.objects.select_related("biome", "catalogue"),
            accession=accession,
        )
        return genome

    @http_get(
        "/{accession}/annotations",
        response=GenomeWithAnnotations,
        summary="Get the annotations for a single MGnify Genome",
        description="Annotations are taxonomic and functional assignments for the genome.",
        operation_id="get_genome_annotations",
    )
    def get_genome_annotations(self, accession: str):
        genome = get_object_or_404(Genome.objects_and_annotations, accession=accession)
        return genome

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[GenomeList],
        summary="List all genomes across MGnify Genome catalogues",
        description="MGnify Genomes are either isolates, or MAGs derived from binned metagenomes.",
        operation_id="list_mgnify_genomes",
    )
    @paginate()
    def list_genomes(
        self,
    ):
        return Genome.objects.select_related("biome", "catalogue")

    @http_get(
        "/catalogues/",
        response=NinjaPaginationResponseSchema[GenomeCatalogueList],
        summary="List all genome catalogues",
        description="MGnify Genomes Catalogues are biome-specific collections of isolate and MAG genomes.",
        operation_id="list_genome_catalogues",
    )
    @paginate()
    def list_genome_catalogues(
        self,
    ):
        return GenomeCatalogue.objects.select_related("biome")

    @http_get(
        "/catalogues/{catalogue_id}",
        response=GenomeCatalogueDetail,
        summary="Get genome catalogue by ID",
        operation_id="get_genome_catalogue",
    )
    def get_catalogue(self, request, catalogue_id: str):
        catalogue = get_object_or_404(
            GenomeCatalogue.objects.select_related("biome"), catalogue_id=catalogue_id
        )
        return catalogue

    @http_get(
        "/catalogues/{catalogue_id}/genomes/",
        response=NinjaPaginationResponseSchema[GenomeList],
        summary="Get genomes within the genome catalogue",
        operation_id="get_genome_catalogue_genomes",
    )
    @paginate()
    def get_catalogue_genomes(self, request, catalogue_id: str):
        catalogue = get_object_or_404(
            GenomeCatalogue.objects.select_related("biome"), catalogue_id=catalogue_id
        )
        return catalogue.genomes.all()
