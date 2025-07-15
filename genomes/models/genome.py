from django.db import models
from django.contrib.postgres.fields import ArrayField

from analyses.base_models.with_downloads_models import WithDownloadsModel
from analyses.models import Biome

# Annotation field constants
COG_CATEGORIES = "cog_categories"
KEGG_CLASSES = "kegg_classes"
KEGG_MODULES = "kegg_modules"
ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"


def default_annotations():
    return {
        COG_CATEGORIES: [],
        KEGG_CLASSES: [],
        KEGG_MODULES: [],
        ANTISMASH_GENE_CLUSTERS: [],
    }


class GenomeManagerDeferringAnnotations(models.Manager):
    """
    The annotations field is a potentially large JSONB field.
    Defer it by default, since most queries don't need to transfer this large dataset.
    """

    def get_queryset(self):
        return super().get_queryset().defer("annotations")


class GenomeManagerIncludingAnnotations(models.Manager):
    def get_queryset(self):
        return super().get_queryset()


class Genome(WithDownloadsModel):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    ISOLATE = "isolate"
    MAG = "mag"
    TYPE_CHOICES = (
        (MAG, "MAG"),
        (ISOLATE, "Isolate"),
    )

    objects = GenomeManagerDeferringAnnotations()
    objects_and_annotations = GenomeManagerIncludingAnnotations()

    genome_id = models.AutoField(primary_key=True)
    accession = models.CharField(max_length=40, unique=True)

    ena_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ena_sample_accession = models.CharField(max_length=20, null=True, blank=True)
    ena_study_accession = models.CharField(max_length=20, null=True)

    ncbi_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ncbi_sample_accession = models.CharField(max_length=20, null=True, blank=True)
    ncbi_study_accession = models.CharField(max_length=20, null=True, blank=True)

    img_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    patric_genome_accession = models.CharField(
        max_length=20,
        unique=True,
        blank=True,
        null=True,
    )

    biome = models.ForeignKey(Biome, on_delete=models.PROTECT)

    length = models.IntegerField()
    num_contigs = models.IntegerField()
    n_50 = models.IntegerField()
    gc_content = models.FloatField()
    type = models.CharField(choices=TYPE_CHOICES, max_length=80)
    completeness = models.FloatField()
    contamination = models.FloatField()

    # EUKS:
    busco_completeness = models.FloatField(null=True, blank=True)

    # EUKS + PROKS:
    rna_5s = models.FloatField(null=True, blank=True)
    # PROKS:
    rna_16s = models.FloatField(null=True, blank=True)
    rna_23s = models.FloatField(null=True, blank=True)
    # EUKS:
    rna_5_8s = models.FloatField(null=True, blank=True)
    rna_18s = models.FloatField(null=True, blank=True)
    rna_28s = models.FloatField(null=True, blank=True)

    trnas = models.FloatField()
    nc_rnas = models.IntegerField()
    num_proteins = models.IntegerField()
    eggnog_coverage = models.FloatField()
    ipr_coverage = models.FloatField()
    taxon_lineage = models.CharField(max_length=400)

    num_genomes_total = models.IntegerField(null=True, blank=True)
    pangenome_size = models.IntegerField(null=True, blank=True)
    pangenome_core_size = models.IntegerField(null=True, blank=True)
    pangenome_accessory_size = models.IntegerField(null=True, blank=True)

    annotations = models.JSONField(default=default_annotations)
    default_annotations = staticmethod(default_annotations)

    last_update = models.DateTimeField(auto_now=True)
    first_created = models.DateTimeField(auto_now_add=True)

    result_directory = models.CharField(max_length=100, blank=True, null=True)

    catalogue = models.ForeignKey(
        "GenomeCatalogue",
        db_column="genome_catalogue",
        on_delete=models.PROTECT,
        related_name="genomes",
    )

    geographic_range = ArrayField(
        models.CharField(max_length=80),
        blank=True,
        null=True,
        help_text="Array of geographic locations where this genome is found",
    )

    geographic_origin = models.CharField(
        max_length=80,
        blank=True,
        null=True,
        help_text="Geographic origin of this genome",
    )

    @property
    def last_update_iso(self):
        return self.last_update.isoformat() if self.last_update else None

    @property
    def first_created_iso(self):
        return self.first_created.isoformat() if self.first_created else None

    @classmethod
    def clean_data(cls, genome_data):
        """
        Clean genome data by removing unnecessary fields and ensuring required fields are present.
        """
        genome_data.pop("gold_biome", None)
        if "annotations" not in genome_data:
            genome_data["annotations"] = cls.default_annotations()
        genome_data.pop("genome_accession", None)
        genome_data.pop("pangenome", None)

        return genome_data

    def __str__(self):
        return self.accession
