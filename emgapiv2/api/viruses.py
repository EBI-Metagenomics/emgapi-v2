"""Public endpoints for MGnify Viruses."""

from typing import Literal

from django.db.models import Count, Exists, F, Func, OuterRef, Q, Sum, Value
from django.db.models.fields.json import DataContains
from django.db.models.functions import Coalesce, JSONObject
from django.shortcuts import get_object_or_404
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

from analyses.models import Analysis
from emgapiv2.api.schema_utils import ApiSections
from genomes.models import CatalogueGenome
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment
from viruses.schemas import (
    ViralCatalogueSchema,
    ViralFragmentDetail,
    ViralFragmentSchema,
)


def referenced_source(queryset, key):
    """Check both occurrence arrays for sources whose visibility changed."""
    needle = Func(JSONObject(**{key: F("pk")}), function="jsonb_build_array")
    return Exists(
        queryset.filter(
            DataContains(OuterRef("occurrences"), needle)
            | DataContains(OuterRef("cluster_member_occurrences"), needle)
        )
    )


def public_fragments():
    """Hide clusters containing private analyses or unreleased genome sources."""
    invalid_analyses = Analysis.objects.filter(
        Q(is_private=True) | Q(is_suppressed=True) | Q(is_ready=False)
    )
    invalid_genomes = CatalogueGenome.objects.exclude(
        catalogue__status__in=["published", "retired"]
    )
    return (
        ViralCatalogueViralFragment.objects.filter(catalogue__is_public=True)
        .alias(
            hidden_analysis=referenced_source(invalid_analyses, "analysis_id"),
            hidden_genome=referenced_source(invalid_genomes, "catalogue_genome_id"),
        )
        .filter(hidden_analysis=False, hidden_genome=False)
    )


def public_catalogues():
    """Count representatives separately from sequences across their clusters."""
    visible = Q(fragment_entries__in=public_fragments())
    return ViralCatalogue.objects.filter(is_public=True).annotate(
        fragment_count=Count("fragment_entries", filter=visible),
        cluster_count=Count("fragment_entries", filter=visible),
        total_sequence_count=Coalesce(
            Sum("fragment_entries__cluster_size", filter=visible), Value(0)
        ),
        virus_count=Count(
            "fragment_entries",
            filter=visible & Q(fragment_entries__sequence_type="virus"),
        ),
        prophage_count=Count(
            "fragment_entries",
            filter=visible & Q(fragment_entries__sequence_type="prophage"),
        ),
        provirus_count=Count(
            "fragment_entries", filter=visible & Q(fragment_entries__is_provirus=True)
        ),
    )


@api_controller("viruses", tags=[ApiSections.VIRUSES])
class VirusController:
    """Browse catalogue releases and their fragments."""

    @http_get(
        "/catalogues/",
        response=NinjaPaginationResponseSchema[ViralCatalogueSchema],
        operation_id="list_viral_catalogues",
    )
    @paginate()
    def list_catalogues(self, request):
        return public_catalogues()

    @http_get(
        "/catalogues/{catalogue_id}",
        response=ViralCatalogueSchema,
        operation_id="get_viral_catalogue",
    )
    def get_catalogue(self, request, catalogue_id: str):
        return get_object_or_404(public_catalogues(), pk=catalogue_id)

    @http_get(
        "/catalogues/{catalogue_id}/fragments",
        response=NinjaPaginationResponseSchema[ViralFragmentSchema],
        operation_id="list_viral_fragments",
    )
    @paginate()
    def list_fragments(
        self,
        request,
        catalogue_id: str,
        search: str = "",
        source_type: Literal["genome", "analysis"] | None = None,
        cluster_representative: str | None = None,
        representatives_only: bool = False,
        is_provirus: bool | None = None,
        sequence_type: Literal["virus", "prophage"] | None = None,
        order: Literal[
            "accession",
            "-accession",
            "length",
            "-length",
            "completeness",
            "-completeness",
            "",
        ] = "accession",
    ):
        get_object_or_404(ViralCatalogue, pk=catalogue_id, is_public=True)
        fragments = (
            public_fragments()
            .filter(catalogue_id=catalogue_id)
            .select_related("fragment")
        )
        if search:
            fragments = fragments.filter(
                Q(fragment__accession__icontains=search)
                | Q(occurrences__icontains=search)
                | Q(cluster_member_occurrences__icontains=search)
                | Q(taxonomy__icontains=search)
            )
        if source_type:
            fragments = fragments.filter(
                Q(occurrences__contains=[{"source_type": source_type}])
                | Q(cluster_member_occurrences__contains=[{"source_type": source_type}])
            )
        if is_provirus is not None:
            fragments = fragments.filter(is_provirus=is_provirus)
        if sequence_type:
            fragments = fragments.filter(sequence_type=sequence_type)
        if cluster_representative:
            fragments = fragments.filter(fragment__accession=cluster_representative)
        # Every stored catalogue entry is a representative; keep the old flag harmless.
        field = order or "accession"
        if field in {"accession", "-accession"}:
            field = field.replace("accession", "fragment__accession")
        return fragments.order_by(field, "pk")

    @http_get(
        "/catalogues/{catalogue_id}/fragments/{accession}",
        response=ViralFragmentDetail,
        operation_id="get_viral_fragment",
    )
    def get_fragment(self, request, catalogue_id: str, accession: str):
        return get_object_or_404(
            public_fragments().select_related("fragment"),
            catalogue_id=catalogue_id,
            fragment__accession=accession,
        )
