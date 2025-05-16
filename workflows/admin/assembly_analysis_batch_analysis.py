from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin
from unfold.contrib.filters.admin import AutocompleteSelectFilter
from unfold.decorators import display, action

from workflows.models import (
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)

STATUS_LABELS = {
    AssemblyAnalysisPipelineStatus.PENDING: "info",
    AssemblyAnalysisPipelineStatus.RUNNING: "warning",
    AssemblyAnalysisPipelineStatus.COMPLETED: "success",
    AssemblyAnalysisPipelineStatus.FAILED: "danger",
}

# TODO: this whole file needs to be reviewd. Claude generated this one and I didn't have time for a proper review


@admin.register(AssemblyAnalysisBatchAnalysis)
class AssemblyAnalysisBatchAnalysisAdmin(ModelAdmin):
    """
    Admin interface for batch assembly analyses (through-table).

    Features:
    - View all batch assembly analysis relations
    - Filter by pipeline status, batch, and disabled state
    - Search by assembly accession or analysis accession
    - Bulk actions to disable/enable relations
    """

    list_display = [
        "id",
        "batch_link",
        "analysis_link",
        "assembly_link",
        "asa_status_badge",
        "virify_status_badge",
        "map_status_badge",
        "disabled_badge",
        "order",
    ]

    list_filter = [
        "disabled",
        "asa_status",
        "virify_status",
        "map_status",
        ("batch", AutocompleteSelectFilter),
        ("analysis__assembly", AutocompleteSelectFilter),
        "created_at",
    ]

    search_fields = [
        "analysis__accession",
        "analysis__assembly__first_accession",
        "analysis__assembly__accessions",
        "batch__study__accession",
        "batch__id",
    ]

    list_select_related = ["batch", "analysis", "analysis__assembly"]

    readonly_fields = ["created_at", "updated_at"]

    ordering = ["-created_at"]

    actions = ["disable_selected_relations", "enable_selected_relations"]

    fieldsets = (
        (
            "Relationship",
            {
                "fields": [
                    "batch",
                    "analysis",
                    "order",
                ]
            },
        ),
        (
            "Pipeline Status",
            {
                "fields": [
                    "asa_status",
                    "virify_status",
                    "map_status",
                ]
            },
        ),
        (
            "Control",
            {
                "fields": [
                    "disabled",
                    "disabled_reason",
                ]
            },
        ),
        (
            "Metadata",
            {
                "classes": ["collapse"],
                "fields": [
                    "created_at",
                    "updated_at",
                ],
            },
        ),
    )

    @display(description="Batch")
    def batch_link(self, obj):
        """Link to the batch admin page."""
        url = reverse(
            "admin:workflows_assemblyanalysisbatch_change", args=[obj.batch.id]
        )
        return format_html('<a href="{}">{}</a>', url, str(obj.batch.id)[:8] + "...")

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

    @display(
        description="ASA",
        label=STATUS_LABELS,
    )
    def asa_status_badge(self, obj):
        """Display ASA pipeline status as a badge."""
        return obj.asa_status

    @display(
        description="VIRify",
        label=STATUS_LABELS,
    )
    def virify_status_badge(self, obj):
        """Display VIRify pipeline status as a badge."""
        return obj.virify_status

    @display(
        description="MAP",
        label=STATUS_LABELS,
    )
    def map_status_badge(self, obj):
        """Display MAP pipeline status as a badge."""
        return obj.map_status

    @display(
        description="Disabled",
        label={
            True: "danger",
            False: "success",
        },
    )
    def disabled_badge(self, obj):
        """Display disabled status."""
        return "Yes" if obj.disabled else "No"

    @action(description="Disable selected batch assembly analyses")
    def disable_selected_relations(self, request, queryset):
        """
        Bulk action to disable selected batch assembly analyses.
        Disabled relations will be excluded from pipeline processing.
        """
        count = queryset.update(disabled=True)
        self.message_user(
            request,
            f"Successfully disabled {count} batch assembly analysis/analyses.",
        )

    @action(description="Enable selected batch assembly analyses")
    def enable_selected_relations(self, request, queryset):
        """
        Bulk action to enable selected batch assembly analyses.
        """
        count = queryset.update(disabled=False)
        self.message_user(
            request,
            f"Successfully enabled {count} batch assembly analysis/analyses.",
        )
