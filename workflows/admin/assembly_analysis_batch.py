from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin, TabularInline
from unfold.contrib.filters.admin import AutocompleteSelectMultipleFilter
from unfold.decorators import display, action

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from workflows.admin.utils import STATUS_LABELS
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)


class AssemblyAnalysisBatchAnalysisInline(TabularInline):
    """
    Inline editor for batch assembly analyses.
    Allows managing which analyses are in the batch and their pipeline status.
    """

    model = AssemblyAnalysisBatchAnalysis
    fk_name = "batch"
    extra = 0
    fields = [
        "analysis_link",
        "assembly_link",
        "asa_status",
        "virify_status",
        "map_status",
        "disabled",
        "disabled_reason",
        "order",
    ]
    readonly_fields = ["analysis_link", "assembly_link"]
    can_delete = True

    @display(description="Analysis")
    def analysis_link(self, obj):
        """Link to the analysis admin page."""
        url = reverse("admin:analyses_analysis_change", args=[obj.analysis.id])
        return format_html('<a href="{}">{}</a>', url, obj.analysis.accession)

    @display(description="Assembly")
    def assembly_link(self, obj):
        """Link to the assembly admin page."""
        if obj.analysis.assembly:
            url = reverse(
                "admin:analyses_assembly_change", args=[obj.analysis.assembly.id]
            )
            return format_html(
                '<a href="{}">{}</a>', url, obj.analysis.assembly.first_accession
            )
        return "-"


@admin.register(AssemblyAnalysisBatch)
class AssemblyAnalysisBatchAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    """
    Admin interface for AssemblyAnalysisBatch.

    Features:
    - Inline editing of batch analyses
    - Action to regenerate samplesheets when analyses change
    - Display of pipeline states and versions
    """

    list_filter_submit = True

    list_display = [
        "id_short",
        "study_link",
        "total_analyses",
        "asa_status_display",
        "virify_status_display",
        "map_status_display",
        "updated_at",
    ]

    list_filter = (
        ["study", AutocompleteSelectMultipleFilter],
        "asa_status",
        "virify_status",
        "map_status",
        "created_at",
    )

    search_fields = [
        "id",
        "study__accession",
        "study__title",
    ]

    list_select_related = ["study"]

    readonly_fields = [
        "id",
        "batch_type",
        "total_analyses",
        "created_at",
        "updated_at",
    ]

    inlines = [AssemblyAnalysisBatchAnalysisInline]

    actions = ["mark_for_rerun"]

    def id_short(self, obj):
        """Short batch ID for display."""
        return str(obj.id)[:8]

    id_short.short_description = "Batch ID"

    @display(description="Study")
    def study_link(self, obj):
        """Link to the study admin page."""
        url = reverse("admin:analyses_study_change", args=[obj.study.accession])
        return format_html('<a href="{}">{}</a>', url, obj.study.accession)

    @display(
        description="ASA",
        label=STATUS_LABELS,
    )
    def asa_status_display(self, obj):
        """Display ASA pipeline status."""
        return obj.asa_status

    @display(
        description="VIRify",
        label=STATUS_LABELS,
    )
    def virify_status_display(self, obj):
        """Display VIRify pipeline status."""
        return obj.virify_status

    @display(
        description="MAP",
        label=STATUS_LABELS,
    )
    def map_status_display(self, obj):
        """Display MAP pipeline status."""
        return obj.map_status

    @action(description="Reset selected batches to PENDING for rerun")
    def mark_for_rerun(self, request, queryset):
        """
        Reset selected batches to PENDING status for all pipelines.
        Useful for rerunning failed batches.
        """
        for batch in queryset:
            batch.asa_status = AssemblyAnalysisPipelineStatus.PENDING
            batch.virify_status = AssemblyAnalysisPipelineStatus.PENDING
            batch.map_status = AssemblyAnalysisPipelineStatus.PENDING
            batch.save()

            # Reset all batch assembly analyses to PENDING
            # This doesn't account for the disabled ones.
            batch.batch_analyses.objects.all().update(
                asa_status=AssemblyAnalysisPipelineStatus.PENDING,
                virify_status=AssemblyAnalysisPipelineStatus.PENDING,
                map_status=AssemblyAnalysisPipelineStatus.PENDING,
            )

        self.message_user(
            request,
            f"Successfully reset {queryset.count()} batch(es) to PENDING status.",
        )

    fieldsets = (
        (
            "Batch Information",
            {
                "fields": [
                    "id",
                    "study",
                    "batch_type",
                    "total_analyses",
                    "workspace_dir",
                ]
            },
        ),
        (
            "ASA Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "asa_status",
                    "asa_samplesheet_path",
                ],
            },
        ),
        (
            "VIRify Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "virify_status",
                    "virify_samplesheet_path",
                ],
            },
        ),
        (
            "MAP Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "map_status",
                    "map_samplesheet_path",
                ],
            },
        ),
        (
            "Pipeline Versions",
            {
                "classes": ["tab"],
                "fields": [
                    "pipeline_versions",
                ],
            },
        ),
        (
            "Errors",
            {
                "classes": ["tab"],
                "fields": [
                    "last_error",
                ],
            },
        ),
        (
            "Metadata",
            {
                "classes": ["tab"],
                "fields": [
                    "created_at",
                    "updated_at",
                ],
            },
        ),
    )
