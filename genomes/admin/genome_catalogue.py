from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import GenomeCatalogue


@admin.register(GenomeCatalogue)
class GenomeCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["name", "catalogue_id", "description"]

    fieldsets = (
        (None, {"fields": ["catalogue_id", "name", "version"]}),
        (
            "Metadata",
            {
                "fields": [
                    "unclustered_genome_count",
                    "catalogue_biome_label",
                    "catalogue_type",
                    "biome",
                    "updated_at",
                    "description",
                    "other_stats",
                ],
                "classes": ["tab"],
            },
        ),
        (
            "Files",
            {
                "classes": ["tab"],
                "fields": ["downloads", "result_directory", "ftp_url"],
            },
        ),
        (
            "Protein catalogue",
            {
                "classes": ["tab"],
                "fields": [
                    "protein_catalogue_name",
                    "protein_catalogue_description",
                ],
            },
        ),
        (
            "Pipeline",
            {
                "fields": ["pipeline_version_tag"],
                "classes": ["tab"],
            },
        ),
    )
