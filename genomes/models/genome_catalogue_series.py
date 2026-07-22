from django.db import models

from analyses.base_models.base_models import TimeStampedModel
from analyses.models import Biome


class GenomeCatalogueSeries(TimeStampedModel):
    """Stable identity shared by all versions of a genome catalogue."""

    PROK = "prokaryotes"
    EUKS = "eukaryotes"
    VIRS = "viruses"
    CATALOGUE_TYPE_CHOICES = (
        (PROK, PROK),
        (EUKS, EUKS),
        (VIRS, VIRS),
    )

    name = models.CharField(max_length=100)
    catalogue_biome_label = models.CharField(
        max_length=100,
        help_text="Stable biome label shared by releases in this series.",
    )
    catalogue_type = models.CharField(
        choices=CATALOGUE_TYPE_CHOICES,
        max_length=20,
    )
    biome = models.ForeignKey(Biome, on_delete=models.PROTECT, null=True, blank=True)

    class Meta:
        verbose_name_plural = "genome catalogue series"
        constraints = [
            models.UniqueConstraint(
                fields=["catalogue_biome_label", "catalogue_type"],
                name="unique_genome_catalogue_series",
            )
        ]

    def __str__(self):
        return f"{self.name} ({self.catalogue_type})"
