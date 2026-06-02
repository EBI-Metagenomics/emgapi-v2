from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import Genome


@admin.register(Genome)
class GenomeCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["accession", "catalogue_id", "taxon_lineage"]

    fieldsets = (
        (None, {"fields": ["accession", "taxon_lineage", "catalogue"]}),
        (
            "Metadata",
            {
                "fields": [
                    "completeness",
                    "contamination",
                    "busco_completeness",
                    "geographic_origin",
                    "type",
                    "n_50",
                    "gc_content",
                    "length",
                    "num_contigs",
                    "biome",
                ],
                "classes": ["tab"],
            },
        ),
        (
            "Accessions",
            {
                "classes": ["tab"],
                "fields": [
                    "ena_genome_accession",
                    "ena_sample_accession",
                    "ena_study_accession",
                    "ncbi_genome_accession",
                    "ncbi_study_accession",
                    "ncbi_sample_accession",
                    "img_genome_accession",
                    "patric_genome_accession",
                ],
            },
        ),
        (
            "Files",
            {
                "classes": ["tab"],
                "fields": ["downloads", "result_directory"],
            },
        ),
        (
            "rRNA, Proteins",
            {
                "classes": ["tab"],
                "fields": [
                    "rna_5s",
                    "rna_16s",
                    "rna_23s",
                    "rna_5_8s",
                    "rna_18s",
                    "rna_28s",
                    "trnas",
                    "nc_rnas",
                ],
            },
        ),
        (
            "Pangenome",
            {
                "fields": [
                    "num_genomes_total",
                    "pangenome_size",
                    "pangenome_core_size",
                    "pangenome_accessory_size",
                    "geographic_range",
                ],
                "classes": ["tab"],
            },
        ),
    )
