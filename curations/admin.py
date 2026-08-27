"""Django admin configuration for Trapiche biome curations."""

from django.contrib import admin
from unfold.admin import ModelAdmin

from .models import CurationMixin, TrapicheBiomeCuration


class MappedFilter(admin.SimpleListFilter):
    """Filter curation records by whether their lineage was mapped."""

    title = "mapped lineage"
    parameter_name = "mapped"

    def lookups(self, request, model_admin):
        return (("yes", "Mapped"), ("no", "Unmapped"))

    def queryset(self, request, queryset):
        if self.value() == "yes":
            return queryset.filter(biome__isnull=False)
        if self.value() == "no":
            return queryset.filter(biome__isnull=True)
        return queryset


@admin.register(TrapicheBiomeCuration)
class TrapicheBiomeCurationAdmin(ModelAdmin):
    """Admin presentation and review actions for Trapiche curations."""

    list_display = (
        "__str__",
        "raw_lineage",
        "confidence",
        "status",
        "is_mapped",
        "source_version",
        "updated_at",
    )
    list_filter = ("status", "source_version", "biome", MappedFilter)
    search_fields = (
        "raw_lineage",
        "study__ena_accessions",
        "sample__ena_accessions",
    )
    autocomplete_fields = ("biome", "curator", "evidence")
    actions = ("approve", "reject")

    @admin.action(description="Approve selected curations")
    def approve(self, request, queryset):
        queryset.update(status=CurationMixin.Status.APPROVED, curator=request.user)

    @admin.action(description="Reject selected curations")
    def reject(self, request, queryset):
        queryset.update(status=CurationMixin.Status.REJECTED, curator=request.user)
