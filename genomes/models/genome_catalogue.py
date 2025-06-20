from django.db import models
from django.utils import timezone

from analyses.models import Biome
from emgapiv2 import settings
from genomes.models.base_model import BaseModel


class GenomeCatalogue(BaseModel):
    catalogue_id = models.SlugField(db_column="CATALOGUE_ID", max_length=100)
    version = models.CharField(db_column="VERSION", max_length=20)
    name = models.CharField(db_column="NAME", max_length=100, unique=True)
    description = models.TextField(
        db_column="DESCRIPTION", null=True, blank=True, help_text="This is a description of the catalogue."
    )
    protein_catalogue_name = models.CharField(
        db_column="PROTEIN_CATALOGUE_NAME", max_length=100, null=True, blank=True
    )
    protein_catalogue_description = models.TextField(
        db_column="PROTEIN_CATALOGUE_DESCRIPTION",
        null=True,
        blank=True,
        help_text="Description of the protein catalogue, if applicable.",
    )
    last_update = models.DateTimeField(db_column="LAST_UPDATE", default=timezone.now)
    result_directory = models.CharField(
        db_column="RESULT_DIRECTORY", max_length=100, null=True, blank=True
    )
    biome = models.ForeignKey(
        Biome, db_column="BIOME_ID", on_delete=models.CASCADE, null=True, blank=True
    )
    genome_count = models.IntegerField(
        db_column="GENOME_COUNT",
        null=True,
        blank=True,
        help_text="Number of genomes available in the web database (species-level cluster reps only)",
    )
    unclustered_genome_count = models.IntegerField(
        db_column="UNCLUSTERED_GENOME_COUNT",
        null=True,
        blank=True,
        help_text="Total number of genomes in the catalogue (including cluster reps and members)",
    )
    ftp_url = models.CharField(
        db_column="FTP_URL", max_length=200, default=settings.MAGS_FTP_SITE
    )
    pipeline_version_tag = models.CharField(
        db_column="PIPELINE_VERSION_TAG",
        max_length=20,
        default=settings.LATEST_MAGS_PIPELINE_TAG,
    )
    catalogue_biome_label = models.CharField(
        db_column="CATALOGUE_BIOME_LABEL",
        max_length=100,
        help_text="The biome label for the catalogue (and any others that share the same practical biome). "
        "Need not be a GOLD biome, e.g. may include host species.",
    )
    PROK = "prokaryotes"
    EUKS = "eukaryotes"
    VIRS = "viruses"
    CATALOGUE_TYPE_CHOICES = (
        (PROK, PROK),
        (EUKS, EUKS),
        (VIRS, VIRS),
    )
    catalogue_type = models.CharField(
        db_column="CATALOGUE_TYPE",
        choices=CATALOGUE_TYPE_CHOICES,
        max_length=20,
    )
    other_stats = models.JSONField(db_column="OTHER_STATS_JSON", blank=True, null=True)

    class Meta:
        unique_together = ("catalogue_biome_label", "version", "catalogue_type")
        db_table = "genome_catalogue"

    def __str__(self):
        return self.name

    def calculate_genome_count(self):
        self.genome_count = self.genomes.count()
        self.save()
