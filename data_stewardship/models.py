"""Database models and managers for curator-reviewed biome predictions."""

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from analyses.models import Analysis, Biome, Sample, Study


class PredictionManager(models.Manager):
    """Persist the latest classifier output for a prediction target."""

    target_field: str

    def replace(self, target, result, evidence=()):
        """Store the latest automated result and return it to review."""
        from .workflows.predict_biomes import map_lineage

        obj, _ = self.update_or_create(
            **{self.target_field: target},
            defaults={
                "predicted_biome": map_lineage(result.lineage),
                "raw_predicted_lineage": result.lineage,
                "confidence": result.confidence,
                "status": self.model.Status.SUGGESTED,
                "method": result.method,
                "source": result.source,
                "source_version": result.version,
                "configuration": result.configuration or {},
                "curator": None,
                "note": "",
            },
        )
        obj.evidence.set(evidence)
        return obj


class StudyPredictionManager(PredictionManager):
    """Manage predictions attached to studies."""

    target_field = "study"


class SamplePredictionManager(PredictionManager):
    """Manage predictions attached directly to samples."""

    target_field = "sample"

    def effective_for_sample(self, sample):
        """Return an explicit sample prediction or its study-level fallback."""
        prediction = (
            self.select_related("sample", "predicted_biome")
            .prefetch_related("evidence")
            .filter(sample=sample)
            .first()
        )
        if prediction is not None:
            return prediction
        return (
            BiomeStudyBiomePrediction.objects.select_related("study", "predicted_biome")
            .prefetch_related("evidence")
            .filter(
                study__in=Study.objects_not_suppressed.all(),
                study__samples=sample,
            )
            .order_by("-predicted_at", "-pk")
            .first()
        )


class BiomePredictionMixin(models.Model):
    """Shared provenance, review, and classifier fields for predictions."""

    class Methods(models.TextChoices):
        TRAPICHE = "trapiche", "Trapiche"
        MANUAL = "manual", "Manual"
        OTHER = "other", "Other"

    class Status(models.TextChoices):
        SUGGESTED = "suggested", "Suggested"
        APPROVED = "approved", "Approved"
        REJECTED = "rejected", "Rejected"

    predicted_biome = models.ForeignKey(
        Biome, null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    raw_predicted_lineage = models.TextField(blank=True)
    confidence = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(1)],
    )
    status = models.CharField(
        max_length=16, choices=Status.choices, default=Status.SUGGESTED, db_index=True
    )
    method = models.CharField(
        max_length=64, choices=Methods.choices, default=Methods.TRAPICHE
    )
    source = models.CharField(max_length=64, blank=True)
    source_version = models.CharField(max_length=64, blank=True)
    configuration = models.JSONField(default=dict, blank=True)
    predicted_at = models.DateTimeField(auto_now=True)
    curator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    note = models.TextField(blank=True)
    evidence = models.ManyToManyField(Analysis, blank=True, related_name="+")

    class Meta:
        abstract = True

    @property
    def is_mapped(self):
        """Return whether the raw lineage resolved to a stored biome."""
        return self.predicted_biome_id is not None


class BiomeStudyBiomePrediction(BiomePredictionMixin):
    """A curator-reviewed biome prediction for one study."""

    objects = StudyPredictionManager()

    study = models.OneToOneField(
        Study, on_delete=models.CASCADE, related_name="biome_prediction"
    )

    def __str__(self):
        """Return the study accession and predicted lineage."""
        return f"{self.study.accession}: {self.raw_predicted_lineage or 'unclassified'}"


class BiomeSampleBiomePrediction(BiomePredictionMixin):
    """A curator-reviewed biome prediction for one sample."""

    objects = SamplePredictionManager()

    sample = models.OneToOneField(
        Sample, on_delete=models.CASCADE, related_name="biome_prediction"
    )

    def __str__(self):
        """Return the sample accession and predicted lineage."""
        return f"{self.sample.first_accession}: {self.raw_predicted_lineage or 'unclassified'}"
