from django.db import models

from analyses.base_models.base_models import TimeStampedModel
from analyses.models import Run, Assembly
from genomes.models import Genome


class AdditionalContainedGenomes(TimeStampedModel):
    """
    A many-to-many relationship model between Genome and Assembly with Run context.
    Stores containment metrics for additional contained genomes discovered externally.
    """

    run = models.ForeignKey(
        Run, on_delete=models.CASCADE, related_name="additional_contained_genomes"
    )
    genome = models.ForeignKey(
        Genome, on_delete=models.CASCADE, related_name="additional_contained_genomes"
    )
    assembly = models.ForeignKey(
        Assembly, on_delete=models.CASCADE, related_name="additional_contained_genomes"
    )

    containment = models.FloatField(null=True, blank=True)
    cani = models.FloatField(null=True, blank=True, db_column="cANI")

    class Meta:
        db_table = "additional_contained_genomes"
        unique_together = ("run", "genome", "assembly")
        indexes = [
            models.Index(fields=["run"]),
            models.Index(fields=["genome"]),
            models.Index(fields=["assembly"]),
        ]

    def __str__(self):
        return f"Run {self.run.first_accession if self.run else '-'} | Genome {self.genome.accession} | Assembly {self.assembly.id}"
