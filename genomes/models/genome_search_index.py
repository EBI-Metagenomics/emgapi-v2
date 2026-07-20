import uuid

from django.db import models
from django.db.models import Q

from analyses.base_models.base_models import TimeStampedModel

from .genome_catalogue import GenomeCatalogue


class GenomeSearchIndex(TimeStampedModel):
    class Backend(models.TextChoices):
        SOURMASH = "sourmash", "Sourmash"
        BRANCHWATER = "branchwater", "Branchwater"

    class Status(models.TextChoices):
        BUILDING = "BUILDING", "Building"
        ACTIVE = "ACTIVE", "Active"
        RETIRED = "RETIRED", "Retired"
        FAILED = "FAILED", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    catalogue = models.ForeignKey(
        GenomeCatalogue,
        on_delete=models.CASCADE,
        related_name="search_indexes",
    )
    backend = models.CharField(max_length=32, choices=Backend.choices)
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.BUILDING,
    )
    is_active = models.BooleanField(default=False)
    ksize = models.PositiveSmallIntegerField(default=31)
    moltype = models.CharField(max_length=16, default="DNA")
    scaled = models.PositiveIntegerField(null=True, blank=True)
    artifact_path = models.CharField(max_length=500)
    manifest_path = models.CharField(max_length=500, blank=True)
    genome_count = models.PositiveIntegerField(default=0)
    checksum = models.CharField(max_length=128, blank=True)
    built_at = models.DateTimeField(null=True, blank=True)
    activated_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ("catalogue__catalogue_id", "backend", "ksize", "-created_at")
        constraints = [
            models.UniqueConstraint(
                fields=["catalogue", "backend", "ksize", "moltype"],
                condition=Q(is_active=True),
                name="unique_active_search_index_per_catalogue_backend",
            )
        ]

    def __str__(self):
        return (
            f"{self.catalogue.catalogue_id} {self.backend} "
            f"k={self.ksize} ({self.status})"
        )
