"""Django admin configuration for Trapiche biome curations."""

from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html, format_html_join
from unfold.admin import ModelAdmin

from .models import (
    CurationLifecycleMixin,
    TrapicheBiomeCuration,
    TrapicheSampleReview,
    TrapicheStudyReview,
)


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
        "analysis__accession",
        "analysis__study__ena_accessions",
        "analysis__sample__ena_accessions",
    )
    autocomplete_fields = ("analysis", "biome", "curator")
    actions = ("approve", "reject")

    @admin.action(description="Approve selected curations")
    def approve(self, request, queryset):
        queryset.update(
            status=CurationLifecycleMixin.Status.APPROVED, curator=request.user
        )

    @admin.action(description="Reject selected curations")
    def reject(self, request, queryset):
        queryset.update(
            status=CurationLifecycleMixin.Status.REJECTED, curator=request.user
        )


class TrapicheReviewAdmin(ModelAdmin):
    """Common read-only admin behaviour for entity-level Trapiche review."""

    actions = ("approve_predictions", "reject_predictions")
    list_filter = ("analyses__trapiche_biome_curations__status",)
    list_select_related = True

    @admin.action(description="Approve Trapiche predictions for selected entities")
    def approve_predictions(self, request, queryset):
        count = self._evidence_queryset(queryset).update(
            status=CurationLifecycleMixin.Status.APPROVED, curator=request.user
        )
        self.message_user(request, f"Approved {count} Trapiche predictions.")

    @admin.action(description="Reject Trapiche predictions for selected entities")
    def reject_predictions(self, request, queryset):
        count = self._evidence_queryset(queryset).update(
            status=CurationLifecycleMixin.Status.REJECTED, curator=request.user
        )
        self.message_user(request, f"Rejected {count} Trapiche predictions.")

    def _evidence_queryset(self, queryset):
        raise NotImplementedError

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def _analysis_link(self, analysis):
        url = reverse("admin:analyses_analysis_change", args=[analysis.pk])
        return format_html('<a href="{}">{}</a>', url, analysis.accession)

    def _evidence(self, analyses):
        return (
            format_html_join(
                "<br>",
                "{} — {} (confidence: {}; {})",
                (
                    (
                        self._analysis_link(analysis),
                        curation.raw_lineage or "unclassified",
                        curation.confidence if curation.confidence is not None else "—",
                        curation.get_status_display(),
                    )
                    for analysis in analyses
                    for curation in [analysis.trapiche_biome_curations]
                ),
            )
            or "—"
        )


@admin.register(TrapicheStudyReview)
class TrapicheStudyReviewAdmin(TrapicheReviewAdmin):
    """Review one study at a time, using its analyses as evidence."""

    list_display = ("accession", "title", "evidence", "updated_at")
    search_fields = ("accession", "title", "ena_accessions", "analyses__accession")
    ordering = ("accession",)

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .filter(analyses__trapiche_biome_curations__isnull=False)
            .prefetch_related("analyses__trapiche_biome_curations")
            .distinct()
        )

    def _evidence_queryset(self, queryset):
        return TrapicheBiomeCuration.objects.filter(analysis__study__in=queryset)

    @admin.display(description="Analyses / predictions")
    def evidence(self, study):
        return self._evidence(
            study.analyses.filter(trapiche_biome_curations__isnull=False).order_by(
                "accession"
            )
        )


@admin.register(TrapicheSampleReview)
class TrapicheSampleReviewAdmin(TrapicheReviewAdmin):
    """Review one sample at a time, grouped/filterable by study."""

    list_display = ("first_accession", "studies_display", "evidence", "updated_at")
    search_fields = ("ena_accessions", "studies__accession", "analyses__accession")
    list_filter = ("studies", "analyses__trapiche_biome_curations__status")
    ordering = ("studies__accession", "ena_accessions")

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .filter(analyses__trapiche_biome_curations__isnull=False)
            .prefetch_related("studies", "analyses__trapiche_biome_curations")
            .distinct()
        )

    def _evidence_queryset(self, queryset):
        return TrapicheBiomeCuration.objects.filter(analysis__sample__in=queryset)

    @admin.display(description="Study")
    def studies_display(self, sample):
        return ", ".join(study.accession for study in sample.studies.all()) or "—"

    @admin.display(description="Analyses / predictions")
    def evidence(self, sample):
        return self._evidence(
            sample.analyses.filter(trapiche_biome_curations__isnull=False).order_by(
                "study__accession", "accession"
            )
        )
