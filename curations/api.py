"""Domain-first HTTP API for biome curations."""

from datetime import datetime
from typing import Literal

from django.http import HttpRequest
from ninja import Query, Schema
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.exceptions import NotFound
from ninja_extra.schemas import NinjaPaginationResponseSchema

from analyses.models import Analysis
from emgapiv2.api import perms
from emgapiv2.api.auth import DjangoSuperUserAuth, NoAuth, WebinJWTAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController

from .models import TrapicheBiomeCuration


class BiomeCurationSchema(Schema):
    """Provider-independent representation of a biome curation."""

    id: int
    analysis_accession: str
    study_accession: str
    sample_accession: str
    biome: str | None = None
    raw_lineage: str
    confidence: float | None = None
    status: str
    mapped: bool
    provider: str = "trapiche"
    provider_version: str
    configuration: dict
    raw_result: dict
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def resolve_analysis_accession(obj):
        """Resolve the owning analysis accession."""
        return obj.analysis.accession

    @staticmethod
    def resolve_study_accession(obj):
        """Resolve the owning study accession."""
        return obj.analysis.study.accession

    @staticmethod
    def resolve_sample_accession(obj):
        """Resolve the owning sample accession."""
        return obj.analysis.sample.first_accession

    @staticmethod
    def resolve_biome(obj):
        """Return the mapped biome lineage, when available."""
        return obj.biome.pretty_lineage if obj.biome else None

    @staticmethod
    def resolve_mapped(obj):
        """Return whether the lineage resolved to a stored biome."""
        return obj.is_mapped

    @staticmethod
    def resolve_provider_version(obj):
        """Expose the provider's recorded version."""
        return obj.source_version


class BiomeCurationFilters(Schema):
    """Filters supported by the biome curation collection endpoint."""

    status: Literal["suggested", "approved", "rejected"] | None = None
    mapped: bool | None = None
    analysis_accession: str | None = None
    study_accession: str | None = None
    sample_accession: str | None = None


@api_controller("curations", tags=["Curations"])
class CurationController(UnauthorisedIsUnfoundController):
    """Serve visibility-filtered, domain-first biome curations."""

    auth = [WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()]
    permissions = [
        perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
    ]

    @staticmethod
    def _curation_queryset():
        return TrapicheBiomeCuration.objects.select_related(
            "analysis__study", "analysis__sample", "biome"
        )

    def _visible(self, curations):
        """Filter curations using the visibility of their target objects."""
        visible = []
        for curation in curations:
            target = curation.analysis
            try:
                self.check_object_permissions(target)
            except NotFound:
                continue
            visible.append(curation)
        return visible

    @http_get(
        "/analyses/{accession}/biome",
        response=BiomeCurationSchema,
        auth=auth,
        permissions=permissions,
    )
    def analysis_biome(self, accession: str):
        """Return the effective biome curation for an analysis."""
        try:
            analysis = Analysis.objects_not_suppressed.get(accession=accession)
        except (Analysis.DoesNotExist, Analysis.MultipleObjectsReturned) as exc:
            raise NotFound(f"No analysis found with accession {accession}") from exc
        self.check_object_permissions(analysis)
        curation = TrapicheBiomeCuration.objects.effective_for_analysis(analysis)
        if curation is None:
            raise NotFound(f"No biome curation available for analysis {accession}")
        return curation

    @http_get(
        "/biomes",
        response=NinjaPaginationResponseSchema[BiomeCurationSchema],
    )
    @paginate()
    def list_biome_curations(
        self, request: HttpRequest, filters: BiomeCurationFilters = Query(...)
    ):
        """List visible Trapiche biome curations."""
        queryset = self._curation_queryset().filter(
            analysis__in=Analysis.public_objects.all()
        )
        if filters.status is not None:
            queryset = queryset.filter(status=filters.status)
        if filters.mapped is not None:
            queryset = queryset.filter(biome__isnull=not filters.mapped)
        if filters.analysis_accession is not None:
            queryset = queryset.filter(analysis__accession=filters.analysis_accession)
        if filters.study_accession is not None:
            queryset = queryset.filter(
                analysis__study__ena_accessions__contains=[filters.study_accession]
            )
        if filters.sample_accession is not None:
            queryset = queryset.filter(
                analysis__sample__ena_accessions__contains=[filters.sample_accession]
            )
        return self._visible(queryset.order_by("-updated_at", "-pk"))

    @http_get(
        "/biomes/{curation_id}",
        response=BiomeCurationSchema,
        auth=auth,
        permissions=permissions,
    )
    def biome_curation(self, curation_id: int):
        """Return one visible Trapiche biome curation."""
        curation = self.get_object_or_exception(
            self._curation_queryset(), pk=curation_id
        )
        self.check_object_permissions(curation.analysis)
        return curation
