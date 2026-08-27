"""Django admin configuration for biome predictions."""

from django.contrib import admin
from unfold.admin import ModelAdmin

from .models import (
    BiomePredictionMixin,
    BiomeSampleBiomePrediction,
    BiomeStudyBiomePrediction,
)


class MappedFilter(admin.SimpleListFilter):
    """Filter prediction records by whether their lineage mapped to a biome."""

    title = "mapped lineage"
    parameter_name = "mapped"

    def lookups(self, request, model_admin):
        """Return the available mapped/unmapped choices."""
        return (("yes", "Mapped"), ("no", "Unmapped"))

    def queryset(self, request, queryset):
        """Filter the queryset according to the selected mapping state."""
        if self.value() == "yes":
            return queryset.filter(predicted_biome__isnull=False)
        if self.value() == "no":
            return queryset.filter(predicted_biome__isnull=True)
        return queryset


class PredictionAdmin(ModelAdmin):
    """Shared admin presentation and review actions for predictions."""

    list_display = (
        "__str__",
        "raw_predicted_lineage",
        "confidence",
        "status",
        "is_mapped",
        "method",
        "predicted_at",
    )
    list_filter = ("status", "method", "source", "predicted_biome", MappedFilter)
    search_fields = (
        "raw_predicted_lineage",
        "study__accession",
        "sample__ena_accessions",
    )
    autocomplete_fields = ("predicted_biome", "curator", "evidence")
    actions = ("approve", "reject")

    @admin.action(description="Approve selected predictions")
    def approve(self, request, queryset):
        """Approve selected predictions and record the reviewing user."""
        queryset.update(
            status=BiomePredictionMixin.Status.APPROVED, curator=request.user
        )

    @admin.action(description="Reject selected predictions")
    def reject(self, request, queryset):
        """Reject selected predictions and record the reviewing user."""
        queryset.update(
            status=BiomePredictionMixin.Status.REJECTED, curator=request.user
        )


@admin.register(BiomeStudyBiomePrediction)
class BiomeStudyPredictionAdmin(PredictionAdmin):
    """Admin configuration for study-level biome predictions."""

    list_filter = PredictionAdmin.list_filter + ("study",)
    search_fields = ("raw_predicted_lineage", "study__accession")


@admin.register(BiomeSampleBiomePrediction)
class BiomeSamplePredictionAdmin(PredictionAdmin):
    """Admin configuration for sample-level biome predictions."""

    list_filter = PredictionAdmin.list_filter + ("sample",)
    search_fields = ("raw_predicted_lineage", "sample__ena_accessions")
