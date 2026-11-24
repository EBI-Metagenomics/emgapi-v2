"""Administration for viral identities and catalogue annotations."""

from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment, ViralFragment


@admin.register(ViralCatalogue)
class ViralCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    """Manage catalogue visibility."""

    list_display = ("catalogue_id", "name", "version", "biome", "is_public")
    list_filter = ("is_public",)
    search_fields = ("catalogue_id", "name")


@admin.register(ViralFragment)
class ViralFragmentAdmin(ModelAdmin):
    """Inspect stable sequence identities."""

    list_display = ("accession", "nt_sequence_sha256")
    search_fields = ("accession", "nt_sequence_sha256")


@admin.register(ViralCatalogueViralFragment)
class ViralCatalogueViralFragmentAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    """Inspect versioned representative annotations and occurrence snapshots."""

    list_display = ("fragment", "catalogue", "cluster_size", "checkv_quality")
    list_filter = ("catalogue", "checkv_quality")
    search_fields = ("fragment__accession",)
    raw_id_fields = ("fragment", "source_analyses", "source_genomes")
