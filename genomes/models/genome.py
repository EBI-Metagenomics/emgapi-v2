from django.db import models

from analyses.base_models.with_downloads_models import WithDownloadsModel
from analyses.models import Assembly, Biome

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

    genome_id = models.AutoField(db_column="genome_id", primary_key=True)
    accession = models.CharField(
        db_column="genome_accession", max_length=40, unique=True
    )

    ena_genome_accession = models.CharField(
        db_column="ena_genome_accession",
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ena_sample_accession = models.CharField(
        db_column="ena_sample_accession", max_length=20, null=True, blank=True
    )
    ena_study_accession = models.CharField(
        db_column="ena_study_accession", max_length=20, null=True
    )

    ncbi_genome_accession = models.CharField(
        db_column="ncbi_genome_accession",
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    ncbi_sample_accession = models.CharField(
        db_column="ncbi_sample_accession", max_length=20, null=True, blank=True
    )
    ncbi_study_accession = models.CharField(
        db_column="ncbi_study_accession", max_length=20, null=True, blank=True
    )

    img_genome_accession = models.CharField(
        db_column="img_genome_accession",
        max_length=20,
        unique=True,
        null=True,
        blank=True,
    )
    patric_genome_accession = models.CharField(
        db_column="patric_genome_accession",
        max_length=20,
        unique=True,
        blank=True,
        null=True,
    )

    biome = models.ForeignKey(Biome, db_column="biome_id", on_delete=models.CASCADE)

    length = models.IntegerField(db_column="length")
    num_contigs = models.IntegerField(db_column="n_contigs")
    n_50 = models.IntegerField(db_column="n50")
    gc_content = models.FloatField(db_column="gc_content")
    type = models.CharField(db_column="type", choices=TYPE_CHOICES, max_length=80)
    completeness = models.FloatField(db_column="completeness")
    contamination = models.FloatField(db_column="contamination")

    # EUKS:
    busco_completeness = models.FloatField(
        db_column="busco_completeness", null=True, blank=True
    )

    # EUKS + PROKS:
    rna_5s = models.FloatField(db_column="rna_5s", null=True, blank=True)
    # PROKS:
    rna_16s = models.FloatField(db_column="rna_16s", null=True, blank=True)
    rna_23s = models.FloatField(db_column="rna_23s", null=True, blank=True)
    # EUKS:
    rna_5_8s = models.FloatField(db_column="rna_5_8s", null=True, blank=True)
    rna_18s = models.FloatField(db_column="rna_18s", null=True, blank=True)
    rna_28s = models.FloatField(db_column="rna_28s", null=True, blank=True)

    trnas = models.FloatField(db_column="T_RNA")
    nc_rnas = models.IntegerField(db_column="nc_rna")
    num_proteins = models.IntegerField(db_column="num_proteins")
    eggnog_coverage = models.FloatField(db_column="eggnog_coverage")
    ipr_coverage = models.FloatField(db_column="ipr_coverage")
    taxon_lineage = models.CharField(db_column="taxon_lineage", max_length=400)

    num_genomes_total = models.IntegerField(
        db_column="pangenome_total_genomes", null=True, blank=True
    )
    pangenome_size = models.IntegerField(
        db_column="pangenome_size", null=True, blank=True
    )
    pangenome_core_size = models.IntegerField(
        db_column="pangenome_core_prop", null=True, blank=True
    )
    pangenome_accessory_size = models.IntegerField(
        db_column="pangenome_accessory_prop", null=True, blank=True
    )

    annotations = models.JSONField(default=default_annotations, db_column="annotations")
    default_annotations = staticmethod(default_annotations)

    last_update = models.DateTimeField(db_column="last_update", auto_now=True)
    first_created = models.DateTimeField(db_column="first_created", auto_now_add=True)

    geo_origin = models.ForeignKey(
        "GeographicLocation",
        db_column="geographic_origin",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
    )

    pangenome_geographic_range = models.ManyToManyField(
        "GeographicLocation",
        db_table="genome_pangenome_geographic_range",
        related_name="geographic_range",
    )

    result_directory = models.CharField(
        db_column="result_directory", max_length=100, blank=True, null=True
    )

    catalogue = models.ForeignKey(
        "GenomeCatalogue",
        db_column="genome_catalogue",
        on_delete=models.CASCADE,
        related_name="genomes",
    )

    @property
    def geographic_range(self):
        return [v.name for v in self.pangenome_geographic_range.all()]

    @property
    def geographic_origin(self):
        if self.geo_origin:
            name = self.geo_origin.name
        else:
            name = None
        return name

    @property
    def last_update_iso(self):
        return self.last_update.isoformat() if self.last_update else None

    @property
    def first_created_iso(self):
        return self.first_created.isoformat() if self.first_created else None

    class Meta:
        db_table = "genome_genomes"

    def __str__(self):
        return self.accession


class GenomeAssemblyLink(models.Model):
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
