"""Domain-first HTTP API for biome curations."""

from datetime import datetime
from typing import Literal

from django.http import HttpRequest
from ninja import Query, Schema
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.exceptions import NotFound
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

from analyses.models import Sample, Study
from emgapiv2.api import perms
from emgapiv2.api.auth import DjangoSuperUserAuth, NoAuth, WebinJWTAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController

from .models import TrapicheBiomeCuration


class BiomeCurationSchema(Schema):
    """Provider-independent representation of a biome curation."""

    id: int
    study_accession: str
    sample_accession: str | None = None
    biome: str | None = None
    raw_lineage: str
    confidence: float | None = None
    status: str
    mapped: bool
    provider: str = "trapiche"
    provider_version: str
    configuration: dict
    raw_result: dict
    analysis_accessions: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def resolve_study_accession(obj):
        """Resolve the owning study accession."""
        return obj.study.accession

    @staticmethod
    def resolve_sample_accession(obj):
        """Resolve the direct or inherited sample accession."""
        inherited_sample = getattr(obj, "inherited_sample", None)
        if inherited_sample is not None:
            return inherited_sample.first_accession
        return obj.sample.first_accession if obj.sample_id else None

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

    @staticmethod
    def resolve_analysis_accessions(obj):
        """Return accessions of analyses used as evidence."""
        return list(obj.evidence.values_list("accession", flat=True))


class BiomeCurationFilters(Schema):
    """Filters supported by the biome curation collection endpoint."""

    status: Literal["suggested", "approved", "rejected"] | None = None
    mapped: bool | None = None
    study_accession: str | None = None
    sample_accession: str | None = None


class InheritedBiomeCuration:
    """Expose a study curation as the effective result for a sample."""

    def __init__(self, curation, sample):
        self.curation = curation
        self.inherited_sample = sample

    def __getattr__(self, name):
        return getattr(self.curation, name)


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
            "study", "sample", "biome"
        ).prefetch_related("evidence")

    def _visible(self, curations):
        """Filter curations using the visibility of their target objects."""
        visible = []
        for curation in curations:
            target = curation.sample if curation.sample_id else curation.study
            try:
                self.check_object_permissions(target)
            except NotFound:
                continue
            visible.append(curation)
        return visible

    @http_get(
        "/studies/{accession}/biome",
        response=BiomeCurationSchema,
        auth=auth,
        permissions=permissions,
    )
    def study_biome(self, accession: str):
        """Return the effective biome curation for a study."""
        try:
            study = Study.objects_not_suppressed.get_by_accession(accession)
        except (Study.DoesNotExist, Study.MultipleObjectsReturned) as exc:
            raise NotFound(f"No study found with accession {accession}") from exc
        self.check_object_permissions(study)
        curation = TrapicheBiomeCuration.objects.effective_for_study(study)
        if curation is None:
            raise NotFound(f"No biome curation available for study {accession}")
        return curation

    @http_get(
        "/samples/{accession}/biome",
        response=BiomeCurationSchema,
        auth=auth,
        permissions=permissions,
    )
    def sample_biome(self, accession: str):
        """Return a sample curation or its effective study-level fallback."""
        try:
            sample = Sample.objects_not_suppressed.get_by_accession(accession)
        except (Sample.DoesNotExist, Sample.MultipleObjectsReturned) as exc:
            raise NotFound(f"No sample found with accession {accession}") from exc

        curation = TrapicheBiomeCuration.objects.effective_for_sample(sample)
        if curation is None:
            raise NotFound(f"No biome curation available for sample {accession}")
        self.check_object_permissions(sample)
        if curation.sample_id:
            return curation
        self.check_object_permissions(curation.study)
        return InheritedBiomeCuration(curation, sample)

    @http_get(
        "/studies/{accession}/biome/history",
        response=NinjaPaginationResponseSchema[BiomeCurationSchema],
        auth=auth,
        permissions=permissions,
    )
    @paginate()
    def study_biome_history(self, accession: str):
        """Return all visible Trapiche study-level biome curations."""
        try:
            study = Study.objects_not_suppressed.get_by_accession(accession)
        except (Study.DoesNotExist, Study.MultipleObjectsReturned) as exc:
            raise NotFound(f"No study found with accession {accession}") from exc
        self.check_object_permissions(study)
        return (
            self._curation_queryset()
            .filter(study=study, sample__isnull=True)
            .order_by("-updated_at", "-pk")
        )

    @http_get(
        "/samples/{accession}/biome/history",
        response=NinjaPaginationResponseSchema[BiomeCurationSchema],
        auth=auth,
        permissions=permissions,
    )
    @paginate()
    def sample_biome_history(self, accession: str):
        """Return direct and inherited study-level sample curations."""
        try:
            sample = Sample.objects_not_suppressed.get_by_accession(accession)
        except (Sample.DoesNotExist, Sample.MultipleObjectsReturned) as exc:
            raise NotFound(f"No sample found with accession {accession}") from exc
        self.check_object_permissions(sample)
        direct = list(self._curation_queryset().filter(sample=sample))
        inherited = [
            InheritedBiomeCuration(curation, sample)
            for curation in self._curation_queryset().filter(
                study__samples=sample, sample__isnull=True
            )
        ]
        inherited = self._visible(inherited)
        return sorted(
            direct + inherited, key=lambda item: item.updated_at, reverse=True
        )

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
            study__in=Study.public_objects.all()
        )
        if filters.status is not None:
            queryset = queryset.filter(status=filters.status)
        if filters.mapped is not None:
            queryset = queryset.filter(biome__isnull=not filters.mapped)
        if filters.study_accession is not None:
            queryset = queryset.filter(
                study__ena_accessions__contains=[filters.study_accession]
            )
        if filters.sample_accession is not None:
            queryset = queryset.filter(
                sample__ena_accessions__contains=[filters.sample_accession]
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
        target = curation.sample if curation.sample_id else curation.study
        self.check_object_permissions(target)
        return curation
