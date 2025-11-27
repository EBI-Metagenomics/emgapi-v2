from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import GenomeCatalogue


@admin.register(GenomeCatalogue)
class GenomeCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["name", "catalogue_id", "description"]