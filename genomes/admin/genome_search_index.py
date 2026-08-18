from django.contrib import admin
from unfold.admin import ModelAdmin

from genomes.models import GenomeSearchIndex


@admin.register(GenomeSearchIndex)
class GenomeSearchIndexAdmin(ModelAdmin):
    list_display = (
        "catalogue",
        "backend",
        "status",
        "is_active",
        "ksize",
        "moltype",
        "artifact_path",
    )
    list_filter = ("backend", "status", "is_active", "ksize", "moltype")
    search_fields = ("catalogue__catalogue_id", "catalogue__name", "artifact_path")
