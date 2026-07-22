import logging
from typing import Literal, Optional

from django.shortcuts import get_object_or_404
from ninja import Query
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

from analyses.schemas import OrderByFilter
from emgapiv2.api.auth import NoAuth, WebinJWTAuth
from emgapiv2.api.perms import IsWebinAdmin, UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    ApiSections,
    BiomeFilter,
    make_links_section,
    make_related_detail_link,
)
from genomes.models import CatalogueGenome, GenomeCatalogue
from genomes.models.genome import COG_CATEGORIES, KEGG_CLASSES
from genomes.schemas import GenomeDetail, GenomeList, GenomeWithAnnotations
from genomes.schemas.GenomeCatalogue import GenomeCatalogueDetail, GenomeCatalogueList
from kvstore.models import KeyValueStore
from kvstore.schemas import CogCategories, KeggClasses

logger = logging.getLogger(__name__)


class GenomeFilters(BiomeFilter):
    search: Optional[str] = Field(
        None,
        description="Search with genome taxonomies and accessions",
        q=[
            "genome__accession",
            "taxon_lineage__icontains",
            "catalogue__catalogue_id__icontains",
        ],
    )


GENOME_ORDERING_OPTIONS = Literal[
    "accession",
    "-accession",
    "length",
    "-length",
    "completeness",
    "-completeness",
    "contamination",
    "-contamination",
    "num_genomes_total",
    "-num_genomes_total",
    "",
]


def order_catalogue_genomes(qs, order):
    if order.order in {"accession", "-accession"}:
        prefix = "-" if order.order.startswith("-") else ""
        return qs.order_by(f"{prefix}genome__accession")
    return order.order_by(qs)


def catalogue_releases_visible_to(request):
    manager = (
        GenomeCatalogue.objects
        if IsWebinAdmin().has_permission(request, None)
        else GenomeCatalogue.public_objects
    )
    return manager.select_related("series__biome")


def catalogue_genomes_visible_to(request):
    return (
        CatalogueGenome.objects
        if IsWebinAdmin().has_permission(request, None)
        else CatalogueGenome.public_objects
    )


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
        auth=[WebinJWTAuth(), NoAuth()],
    )
    def get_mgnify_genome(
        self, request, accession: str, catalogue_id: str | None = None
    ):
        genomes = (
            catalogue_genomes_visible_to(request)
            if catalogue_id
            else CatalogueGenome.public_objects
        )
        genome = get_object_or_404(
            genomes.select_related("genome", "biome", "catalogue", "catalogue__series"),
            genome__accession=accession,
            **({"catalogue_id": catalogue_id} if catalogue_id else {}),
        )
        return genome

    @http_get(
        "/{accession}/annotations",
        response=GenomeWithAnnotations,
        summary="Get the annotations for a single MGnify Genome",
        description="Annotations are taxonomic and functional assignments for the genome.",
        operation_id="get_genome_annotations",
        auth=[WebinJWTAuth(), NoAuth()],
    )
    def get_genome_annotations(
        self, request, accession: str, catalogue_id: str | None = None
    ):
        genomes = CatalogueGenome.public_objects_and_annotations
        if catalogue_id and IsWebinAdmin().has_permission(request, None):
            genomes = CatalogueGenome.objects_and_annotations
        genome = get_object_or_404(
            genomes.select_related("genome"),
            genome__accession=accession,
            **({"catalogue_id": catalogue_id} if catalogue_id else {}),
        )

        # Augment COG categories with descriptions from KVStore
        if COG_CATEGORIES in genome.annotations:
            try:
                cog_kv = KeyValueStore.get_model(
                    "cog_category_description_map", CogCategories
                )
                cog_desc_map = cog_kv.root
                for cog in genome.annotations[COG_CATEGORIES]:
                    cog["description"] = cog_desc_map.get(cog.get("name"), "")
            except (KeyValueStore.DoesNotExist, Exception):
                logger.exception("Failed to load COG category descriptions")

        # Augment KEGG classes with descriptions from KVStore
        if KEGG_CLASSES in genome.annotations:
            try:
                kegg_kv = KeyValueStore.get_model(
                    "kegg_class_description_map", KeggClasses
                )
                kegg_desc_map = kegg_kv.root
                for kegg in genome.annotations[KEGG_CLASSES]:
                    kegg["description"] = kegg_desc_map.get(kegg.get("class_id"), "")
            except (KeyValueStore.DoesNotExist, Exception):
                logger.exception("Failed to load KEGG class descriptions")

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
        order: OrderByFilter[GENOME_ORDERING_OPTIONS] = Query(...),
        filters: GenomeFilters = Query(...),
    ):
        qs = CatalogueGenome.public_objects.select_related(
            "genome", "biome", "catalogue", "catalogue__series"
        )
        qs = order_catalogue_genomes(qs, order)
        qs = filters.filter(qs)
        return qs

    @http_get(
        "/catalogues/",
        response=NinjaPaginationResponseSchema[GenomeCatalogueList],
        summary="List all genome catalogues",
        description="MGnify Genomes Catalogues are biome-specific collections of isolate and MAG genomes.",
        operation_id="list_genome_catalogues",
        auth=[WebinJWTAuth(), NoAuth()],
    )
    @paginate()
    def list_genome_catalogues(
        self,
        request,
    ):
        return catalogue_releases_visible_to(request)

    @http_get(
        "/catalogues/{catalogue_id}",
        response=GenomeCatalogueDetail,
        summary="Get genome catalogue by ID",
        operation_id="get_genome_catalogue",
        auth=[WebinJWTAuth(), NoAuth()],
    )
    def get_catalogue(self, request, catalogue_id: str):
        catalogue = get_object_or_404(
            catalogue_releases_visible_to(request),
            catalogue_id=catalogue_id,
        )
        return catalogue

    @http_get(
        "/catalogues/{catalogue_id}/genomes/",
        response=NinjaPaginationResponseSchema[GenomeList],
        summary="Get genomes within the genome catalogue",
        operation_id="get_genome_catalogue_genomes",
        auth=[WebinJWTAuth(), NoAuth()],
    )
    @paginate()
    def get_catalogue_genomes(
        self,
        request,
        catalogue_id: str,
        order: OrderByFilter[GENOME_ORDERING_OPTIONS] = Query(...),
        filters: GenomeFilters = Query(...),
    ):
        catalogue = get_object_or_404(
            catalogue_releases_visible_to(request),
            catalogue_id=catalogue_id,
        )
        qs = catalogue.genomes.select_related("genome", "biome", "catalogue__series")
        qs = order_catalogue_genomes(qs, order)
        qs = filters.filter(qs)
        return qs
