from typing import Any

from django.contrib.postgres.fields import ArrayField
from django.db import models

from analyses.base_models.base_models import TimeStampedModel
from analyses.base_models.with_downloads_models import WithDownloadsModel
from analyses.models import Biome
from genomes.models.genome import Genome, default_annotations


def _genome_attribute(name):
    return property(lambda catalogue_genome: getattr(catalogue_genome.genome, name))


class CatalogueGenomeQuerySet(models.QuerySet):
    def published(self):
        return self.filter(catalogue__status="published")


class CatalogueGenomeManagerDeferringAnnotations(
    models.Manager.from_queryset(CatalogueGenomeQuerySet)
):
    def get_queryset(self):
        return super().get_queryset().defer("annotations")


class CatalogueGenomeManagerIncludingAnnotations(
    models.Manager.from_queryset(CatalogueGenomeQuerySet)
):
    pass


class PublishedCatalogueGenomeManagerDeferringAnnotations(
    CatalogueGenomeManagerDeferringAnnotations
):
    def get_queryset(self):
        return super().get_queryset().published()


class PublishedCatalogueGenomeManagerIncludingAnnotations(
    CatalogueGenomeManagerIncludingAnnotations
):
    def get_queryset(self):
        return super().get_queryset().published()


class CatalogueGenome(WithDownloadsModel, TimeStampedModel):
    """A versioned snapshot of a genome in a catalogue release."""

    DOWNLOAD_PARENT_IDENTIFIER_ATTR = "pk"

    objects = CatalogueGenomeManagerDeferringAnnotations()
    objects_and_annotations = CatalogueGenomeManagerIncludingAnnotations()
    public_objects = PublishedCatalogueGenomeManagerDeferringAnnotations()
    public_objects_and_annotations = (
        PublishedCatalogueGenomeManagerIncludingAnnotations()
    )

    genome = models.ForeignKey(
        Genome, on_delete=models.CASCADE, related_name="catalogue_entries"
    )
    catalogue = models.ForeignKey(
        "GenomeCatalogue", on_delete=models.CASCADE, related_name="genomes"
    )

    biome = models.ForeignKey(Biome, on_delete=models.PROTECT)

    length = models.IntegerField()
    num_contigs = models.IntegerField()
    n_50 = models.IntegerField()
    gc_content = models.FloatField()
    type = models.CharField(choices=Genome.GenomeType.as_choices(), max_length=80)
    completeness = models.FloatField()
    contamination = models.FloatField()

    busco_completeness = models.FloatField(null=True, blank=True)
    rna_5s = models.FloatField(null=True, blank=True)
    rna_16s = models.FloatField(null=True, blank=True)
    rna_23s = models.FloatField(null=True, blank=True)
    rna_5_8s = models.FloatField(null=True, blank=True)
    rna_18s = models.FloatField(null=True, blank=True)
    rna_28s = models.FloatField(null=True, blank=True)

    trnas = models.FloatField()
    nc_rnas = models.IntegerField()
    num_proteins = models.IntegerField()
    eggnog_coverage = models.FloatField()
    ipr_coverage = models.FloatField()
    taxon_lineage = models.CharField(max_length=400)

    geographic_origin = models.CharField(
        max_length=80,
        blank=True,
        null=True,
        help_text="Geographic origin of this genome",
    )

    annotations = models.JSONField(default=default_annotations)
    result_directory = models.CharField(max_length=100, blank=True, null=True)

    num_genomes_total = models.IntegerField(null=True, blank=True)
    pangenome_size = models.IntegerField(null=True, blank=True)
    pangenome_core_size = models.IntegerField(null=True, blank=True)
    pangenome_accessory_size = models.IntegerField(null=True, blank=True)
    geographic_range = ArrayField(
        models.CharField(max_length=80),
        blank=True,
        null=True,
        help_text="Array of geographic locations where this genome is found",
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["catalogue", "genome"],
                name="unique_genome_per_catalogue_release",
            )
        ]

    accession = _genome_attribute("accession")
    ena_genome_accession = _genome_attribute("ena_genome_accession")
    ena_sample_accession = _genome_attribute("ena_sample_accession")
    ena_study_accession = _genome_attribute("ena_study_accession")
    ncbi_genome_accession = _genome_attribute("ncbi_genome_accession")
    ncbi_sample_accession = _genome_attribute("ncbi_sample_accession")
    ncbi_study_accession = _genome_attribute("ncbi_study_accession")
    img_genome_accession = _genome_attribute("img_genome_accession")
    patric_genome_accession = _genome_attribute("patric_genome_accession")

    @classmethod
    def clean_data(cls, genome_data: dict[str, Any]):
        for field in ("accession", "gold_biome", "genome_accession", "study_accession"):
            genome_data.pop(field, None)

        pangenome = genome_data.pop("pangenome", None)
        if isinstance(pangenome, dict):
            if "num_genomes_total" in pangenome:
                genome_data.setdefault(
                    "num_genomes_total", pangenome["num_genomes_total"]
                )
            for field in (
                "geographic_range",
                "pangenome_size",
                "pangenome_core_size",
                "pangenome_accessory_size",
            ):
                genome_data[field] = pangenome.get(field)

        genome_data.setdefault("num_genomes_total", 1)
        genome_data.setdefault("annotations", default_annotations())

        if "rna_5.8s" in genome_data:
            genome_data["rna_5_8s"] = genome_data.pop("rna_5.8s")

        return genome_data

    def __str__(self):
        return f"{self.accession} in {self.catalogue_id}"
