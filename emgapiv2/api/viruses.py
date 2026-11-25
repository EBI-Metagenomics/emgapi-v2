"""Live and administrator-preview MGnify Viruses endpoints."""

from typing import Literal

from django.db.models import Count, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.shortcuts import get_object_or_404
from ninja import Query
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

from analyses.models import Analysis
from emgapiv2.api.auth import NoAuth, WebinJWTAuth
from emgapiv2.api.perms import IsWebinAdmin
from emgapiv2.api.schema_utils import ApiSections
from genomes.models import CatalogueGenome
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment
from viruses.schemas import (
    ViralCatalogueSchema,
    ViralFragmentDetail,
    ViralFragmentSchema,
)


def visible_fragments(request):
    """Keep unpublished catalogues and private source rows behind admin access."""
    entries = ViralCatalogueViralFragment.objects.all()
    if IsWebinAdmin().has_permission(request, None):
        return entries
    return entries.filter(catalogue__is_public=True).exclude(
        Q(
            source_analyses__in=Analysis.objects.filter(
                Q(is_private=True) | Q(is_suppressed=True) | Q(is_ready=False)
            )
        )
        | Q(
            source_genomes__in=CatalogueGenome.objects.exclude(
                catalogue__status__in=["published", "retired"]
            )
        )
    )


def visible_catalogues(request):
    """Count the imported representatives visible to this caller."""
    catalogues = ViralCatalogue.objects.all()
    if not IsWebinAdmin().has_permission(request, None):
        catalogues = catalogues.filter(is_public=True)
    visible = Q(fragment_entries__in=visible_fragments(request))
    viral = visible & ~Q(fragment_entries__sequence_type="plasmid")
    return catalogues.annotate(
        fragment_count=Count("fragment_entries", filter=viral),
        cluster_count=Count("fragment_entries", filter=viral),
        total_sequence_count=Coalesce(
            Sum("fragment_entries__cluster_size", filter=viral), Value(0)
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
            "fragment_entries", filter=viral & Q(fragment_entries__is_provirus=True)
        ),
        plasmid_count=Count(
            "fragment_entries",
            filter=visible & Q(fragment_entries__sequence_type="plasmid"),
        ),
    )


@api_controller("viruses", tags=[ApiSections.VIRUSES], auth=[WebinJWTAuth(), NoAuth()])
class VirusController:
    """Open the live release, or an administrator's preview version."""

    @http_get(
        "/catalogues/latest",
        response=ViralCatalogueSchema,
        operation_id="get_latest_viral_catalogue",
    )
    def latest(self, request):
        return get_object_or_404(visible_catalogues(request), is_public=True)

    @http_get(
        "/catalogues/{catalogue_id}",
        response=ViralCatalogueSchema,
        operation_id="get_viral_catalogue",
    )
    def get_catalogue(self, request, catalogue_id: str):
        return get_object_or_404(visible_catalogues(request), pk=catalogue_id)

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
        kind: Literal["viruses", "plasmids"] = "viruses",
        search: str = "",
        source_type: Literal["genome", "analysis"] | None = None,
        sequence_type: Literal["virus", "prophage", "plasmid"] | None = None,
        completeness_min: float | None = Query(None, ge=0, le=100),
        completeness_max: float | None = Query(None, ge=0, le=100),
        contamination_min: float | None = Query(None, ge=0, le=100),
        contamination_max: float | None = Query(None, ge=0, le=100),
        length_min: int | None = Query(None, ge=0),
        length_max: int | None = Query(None, ge=0),
        order: Literal[
            "accession",
            "-accession",
            "length",
            "-length",
            "completeness",
            "-completeness",
            "contamination",
            "-contamination",
            "",
        ] = "accession",
    ):
        get_object_or_404(visible_catalogues(request), pk=catalogue_id)
        entries = (
            visible_fragments(request)
            .filter(catalogue_id=catalogue_id)
            .select_related("fragment")
        )
        entries = (
            entries.filter(sequence_type="plasmid")
            if kind == "plasmids"
            else entries.exclude(sequence_type="plasmid")
        )
        if search:
            entries = entries.filter(
                Q(fragment__accession__icontains=search)
                | Q(metadata_rows__icontains=search)
            )
        if source_type:
            sources = (
                [source_type, "assembly"]
                if source_type == "analysis"
                else [source_type]
            )
            query = Q()
            for source in sources:
                query |= Q(metadata_rows__contains=[{"Source": source}])
            entries = entries.filter(query)
        if sequence_type:
            entries = entries.filter(sequence_type=sequence_type)
        for field, lower, upper in [
            ("completeness", completeness_min, completeness_max),
            ("contamination", contamination_min, contamination_max),
            ("length", length_min, length_max),
        ]:
            if lower is not None:
                entries = entries.filter(**{f"{field}__gte": lower})
            if upper is not None:
                entries = entries.filter(**{f"{field}__lte": upper})
        field = order or "accession"
        if field in {"accession", "-accession"}:
            field = field.replace("accession", "fragment__accession")
        return entries.order_by(field, "pk")

    @http_get(
        "/catalogues/{catalogue_id}/fragments/{accession}",
        response=ViralFragmentDetail,
        operation_id="get_viral_fragment",
    )
    def get_fragment(self, request, catalogue_id: str, accession: str):
        return get_object_or_404(
            visible_fragments(request).select_related("fragment", "catalogue"),
            catalogue_id=catalogue_id,
            fragment__accession=accession,
        )
