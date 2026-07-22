from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import CatalogueGenome, Genome


@admin.register(Genome)
class GenomeAdmin(ModelAdmin):
    search_fields = [
        "accession",
        "ena_genome_accession",
        "ncbi_genome_accession",
        "catalogue_entries__catalogue__catalogue_id",
    ]
    fieldsets = (
        (None, {"fields": ["accession"]}),
        (
            "Accessions",
            {
                "fields": [
                    "ena_genome_accession",
                    "ena_sample_accession",
                    "ena_study_accession",
                    "ncbi_genome_accession",
                    "ncbi_study_accession",
                    "ncbi_sample_accession",
                    "img_genome_accession",
                    "patric_genome_accession",
                ]
            },
        ),
    )


@admin.register(CatalogueGenome)
class CatalogueGenomeAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = [
        "genome__accession",
        "catalogue__catalogue_id",
        "taxon_lineage",
    ]
    list_display = ["accession", "catalogue", "catalogue_status", "type", "biome"]
    list_filter = ["catalogue__status", "catalogue", "type"]
    autocomplete_fields = ["genome", "catalogue", "biome"]
    readonly_fields = ["accession"]

    fieldsets = (
        (None, {"fields": ["accession", "genome", "catalogue", "taxon_lineage"]}),
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
                    "annotations",
                ],
                "classes": ["tab"],
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
                    "num_proteins",
                    "eggnog_coverage",
                    "ipr_coverage",
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

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related("genome", "catalogue", "catalogue__series", "biome")
        )

    @admin.display(description="Status", ordering="catalogue__status")
    def catalogue_status(self, obj):
        return obj.catalogue.get_status_display()
