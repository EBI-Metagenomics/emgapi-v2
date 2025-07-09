from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import GenomeCatalogue, Genome


@admin.register(GenomeCatalogue)
class GenomeCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["name", "catalogue_id", "description"]

@admin.register(Genome)
class GenomeAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["accession"]