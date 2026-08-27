from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import Case, IntegerField, QuerySet, When

from analyses.models import Analysis, Biome, Publication, Sample, Study


def normalize_lineage(lineage: str | None) -> str:
    """Remove empty lineage components and surrounding whitespace."""
    return ":".join(part.strip() for part in (lineage or "").split(":") if part.strip())


def map_lineage(lineage: str | None, biomes: QuerySet | None = None) -> Biome | None:
    """Map a classifier lineage to the corresponding stored biome."""
    normalized = normalize_lineage(lineage)
    if not normalized:
        return None
    queryset = biomes if biomes is not None else Biome.objects.all()
    return queryset.filter(path=Biome.lineage_to_path(normalized)).first()


class CurationLifecycleMixin(models.Model):
    """Shared review, provenance, and lifecycle fields for curations."""

    class Status(models.TextChoices):
        SUGGESTED = "suggested", "Suggested"
        APPROVED = "approved", "Approved"
        REJECTED = "rejected", "Rejected"

    status = models.CharField(
        max_length=16, choices=Status.choices, default=Status.SUGGESTED, db_index=True
    )
    provider = models.CharField(max_length=64, blank=True, db_index=True)
    confidence = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(1)],
    )
    source_version = models.CharField(max_length=64, blank=True)
    configuration = models.JSONField(default=dict, blank=True)
    raw_result = models.JSONField(default=dict, blank=True)
    curator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    note = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        abstract = True


class AnalysisEvidenceMixin(models.Model):
    """Evidence relationship shared by curations backed by analyses."""

    evidence = models.ManyToManyField(Analysis, blank=True, related_name="+")

    class Meta:
        abstract = True


class CurationMixin(CurationLifecycleMixin, AnalysisEvidenceMixin):
    """Backward-compatible composite mixin for analysis-backed curations."""

    class Meta:
        abstract = True


class EffectiveCurationManagerMixin:
    """Provide a shared selection of the effective curation record."""

    def _effective(self, queryset):
        """Return the newest non-rejected approved result, if any."""
        return (
            queryset.exclude(status=self.model.Status.REJECTED)
            .order_by(
                Case(
                    When(status=self.model.Status.APPROVED, then=0),
                    default=1,
                    output_field=IntegerField(),
                ),
                "-updated_at",
                "-pk",
            )
            .first()
        )


class TrapicheBiomeCurationManager(EffectiveCurationManagerMixin, models.Manager):
    """Create and select Trapiche biome curations."""

    def record(self, study, result, sample=None, evidence=()):
        """Persist one Trapiche result without replacing previous runs."""
        curation = self.create(
            study=study,
            sample=sample,
            provider="trapiche",
            biome=map_lineage(result.lineage),
            raw_lineage=result.lineage,
            confidence=result.confidence,
            source_version=result.version,
            configuration=result.configuration or {},
            raw_result=result.raw_result or {},
        )
        curation.evidence.set(evidence)
        return curation

    def effective_for_study(self, study):
        """Return the effective study-level Trapiche curation."""
        return self._effective(
            self.select_related("study", "biome")
            .prefetch_related("evidence")
            .filter(study=study, sample__isnull=True)
        )

    def effective_for_sample(self, sample):
        """Return a direct sample curation or its study-level fallback."""
        direct = self._effective(
            self.select_related("study", "sample", "biome")
            .prefetch_related("evidence")
            .filter(sample=sample)
        )
        if direct is not None:
            return direct

        return self._effective(
            self.select_related("study", "biome")
            .prefetch_related("evidence")
            .filter(study__in=Study.objects_not_suppressed.all(), study__samples=sample)
            .filter(sample__isnull=True)
        )


class TrapicheBiomeCuration(CurationMixin):
    """A biome curation produced by Trapiche for a study or sample."""

    objects = TrapicheBiomeCurationManager()

    study = models.ForeignKey(
        Study,
        on_delete=models.CASCADE,
        related_name="trapiche_biome_curations",
    )
    sample = models.ForeignKey(
        Sample,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="trapiche_biome_curations",
    )
    biome = models.ForeignKey(
        Biome,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    raw_lineage = models.TextField(blank=True)

    class Meta:
        indexes = [
            models.Index(fields=("study", "sample", "-updated_at")),
            models.Index(fields=("status", "-updated_at")),
        ]

    @property
    def is_mapped(self):
        """Return whether the raw lineage resolved to a stored biome."""
        return self.biome_id is not None

    def __str__(self):
        """Return the target accession and curated lineage."""
        target = self.sample if self.sample_id else self.study
        return f"{target}: {self.raw_lineage or 'unclassified'}"


class EuropePmcPublicationCurationManager(
    EffectiveCurationManagerMixin, models.Manager
):
    """Select effective and historical Europe PMC publication curations."""

    def effective_for_publication(self, publication):
        return self._effective(
            self.filter(publication=publication).prefetch_related(
                "groups__annotations__mentions__tags"
            )
        )

    def history_for_publication(self, publication):
        return (
            self.filter(publication=publication)
            .prefetch_related("groups__annotations__mentions__tags")
            .order_by("-updated_at", "-pk")
        )


class EuropePmcPublicationCuration(CurationLifecycleMixin):
    """One persisted Europe PMC annotation assertion for a publication."""

    objects = EuropePmcPublicationCurationManager()

    publication = models.ForeignKey(
        Publication,
        on_delete=models.CASCADE,
        related_name="europe_pmc_curations",
    )

    class Meta:
        indexes = [
            models.Index(fields=("publication", "-updated_at")),
            models.Index(fields=("status", "-updated_at")),
        ]


class EuropePmcAnnotationGroup(models.Model):
    """A grouped annotation type within one Europe PMC snapshot."""

    curation = models.ForeignKey(
        EuropePmcPublicationCuration,
        on_delete=models.CASCADE,
        related_name="groups",
    )
    annotation_type = models.CharField(max_length=64)
    category = models.CharField(max_length=32)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("curation", "annotation_type", "category"),
                name="unique_europe_pmc_group_per_curation",
            )
        ]


class EuropePmcAnnotation(models.Model):
    """A normalized annotation text within an Europe PMC annotation group."""

    group = models.ForeignKey(
        EuropePmcAnnotationGroup,
        on_delete=models.CASCADE,
        related_name="annotations",
    )
    annotation_text = models.TextField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("group", "annotation_text"),
                name="unique_europe_pmc_annotation_per_group",
            )
        ]


class EuropePmcAnnotationMention(models.Model):
    """One occurrence of a Europe PMC annotation in publication text."""

    annotation = models.ForeignKey(
        EuropePmcAnnotation,
        on_delete=models.CASCADE,
        related_name="mentions",
    )
    exact = models.TextField()
    external_id = models.CharField(max_length=255, blank=True)
    postfix = models.TextField(blank=True)
    prefix = models.TextField(blank=True)
    provider = models.CharField(max_length=64, blank=True)
    annotation_type = models.CharField(max_length=64)
    section = models.CharField(max_length=255, blank=True)


class EuropePmcAnnotationTag(models.Model):
    """Ontology tag attached to a Europe PMC annotation mention."""

    mention = models.ForeignKey(
        EuropePmcAnnotationMention,
        on_delete=models.CASCADE,
        related_name="tags",
    )
    name = models.CharField(max_length=255)
    uri = models.URLField(max_length=2048)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("mention", "name", "uri"),
                name="unique_europe_pmc_tag_per_mention",
            )
        ]
