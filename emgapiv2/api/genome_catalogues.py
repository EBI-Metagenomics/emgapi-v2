from typing import List
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated

from emgapiv2.api import ApiSections
from genomes.models import GenomeCatalogue
from genomes.schemas import GenomeCatalogueDetail
from genomes.schemas.GenomeCatalogue import GenomeCatalogueList

router = RouterPaginated(tags=[ApiSections.GENOMES])


@router.get(
    "/",
    response=List[GenomeCatalogueList],
    summary="List all genome catalogues",
    operation_id="list_genome_catalogues",
)
def list_catalogues(request):
    return GenomeCatalogue.objects.select_related("biome")


@router.get(
    "/{catalogue_id}",
    response=GenomeCatalogueDetail,
    summary="Get genome catalogue by ID",
    operation_id="get_genome_catalogue",
)
def get_catalogue(request, catalogue_id: str):
    catalogue = get_object_or_404(
        GenomeCatalogue.objects.select_related("biome"), catalogue_id=catalogue_id
    )
    return catalogue
