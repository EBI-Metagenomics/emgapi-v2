from __future__ import annotations

from django.db import models


class WithExperimentTypeModel(models.Model):
    class ExperimentTypes(models.TextChoices):
        METATRANSCRIPTOMIC = "METAT", "Metatranscriptomic"
        METAGENOMIC = "METAG", "Metagenomic"
        AMPLICON = "AMPLI", "Amplicon"
        ASSEMBLY = "ASSEM", "Assembly"
        HYBRID_ASSEMBLY = "HYASS", "Hybrid assembly"
        LONG_READ_ASSEMBLY = "LRASS", "Long-read assembly"

        # legacy
        METABARCODING = "METAB", "Metabarcoding"
        UNKNOWN = "UNKNO", "Unknown"

    experiment_type = models.CharField(
        choices=ExperimentTypes, max_length=5, default=ExperimentTypes.UNKNOWN
    )

    class Meta:
        abstract = True
