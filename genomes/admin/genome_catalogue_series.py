from django.contrib import admin
from unfold.admin import ModelAdmin

from genomes.models import GenomeCatalogueSeries


@admin.register(GenomeCatalogueSeries)
class GenomeCatalogueSeriesAdmin(ModelAdmin):
    search_fields = ["name", "catalogue_biome_label"]
    list_display = ["name", "catalogue_biome_label", "catalogue_type", "biome"]
    list_filter = ["catalogue_type"]
    autocomplete_fields = ["biome"]
