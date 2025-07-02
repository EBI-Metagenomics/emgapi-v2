from django.db import models

from analyses.models import Assembly
from genomes.models import Genome
from genomes.models.base_model import BaseModel


class GenomeAssemblyLink(BaseModel):
    """
    A many-to-many relationship model between Genome and Assembly.
    Stores additional information about the relationship.
    """

    genome = models.ForeignKey(
        Genome, on_delete=models.CASCADE, related_name="assembly_links"
    )
    assembly = models.ForeignKey(
        Assembly, on_delete=models.CASCADE, related_name="genome_links"
    )
    species_rep = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Arbitrary genome accession for species representative",
    )
    mag_accession = models.CharField(
        max_length=100, null=True, blank=True, help_text="Arbitrary accession for MAG"
    )

    class Meta:
        db_table = "genome_assembly_link"
        unique_together = ("genome", "assembly")

    def __str__(self):
        return f"Link between {self.genome.accession} and {self.assembly.id}"