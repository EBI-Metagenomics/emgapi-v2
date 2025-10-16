import os

from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin, TabularInline
from unfold.decorators import display, action

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from analyses.models import AssemblyAnalysisBatch, Analysis, PipelineState


class AnalysisInline(TabularInline):
    """
    Inline editor for analyses in a batch.
    Allows adding/removing analyses from the batch.
    """

    model = Analysis
    fk_name = "assembly_analysis_batch"
    extra = 0
    fields = ["accession", "experiment_type", "assembly_link", "status_display"]
    readonly_fields = [
        "accession",
        "experiment_type",
        "assembly_link",
        "status_display",
    ]
    can_delete = True

    @display(description="Assembly")
    def assembly_link(self, obj):
        """Link to the assembly admin page."""
        if obj.assembly:
            url = reverse("admin:analyses_assembly_change", args=[obj.assembly.id])
            return format_html('<a href="{}">{}</a>', url, obj.assembly.first_accession)
        return "-"

    @display(description="Status")
    def status_display(self, obj):
        """Display key status flags."""
        if obj.status.get("analysis_annotations_imported"):
            return format_html('<span style="color: green;">✓ Imported</span>')
        elif obj.status.get("analysis_completed"):
            return format_html('<span style="color: blue;">✓ Completed</span>')
        elif obj.status.get("analysis_failed"):
            return format_html('<span style="color: red;">✗ Failed</span>')
        return format_html('<span style="color: gray;">Pending</span>')


@admin.register(AssemblyAnalysisBatch)
class AssemblyAnalysisBatchAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    """
    Admin interface for AssemblyAnalysisBatch.

    Features:
    - Inline editing of batch analyses
    - Action to regenerate samplesheets when analyses change
    - Display of pipeline states and versions
    """

    list_display = [
        "id",
        "study_link",
        "total_analyses",
        "asa_status_display",
        "virify_status_display",
        "map_status_display",
        "updated_at",
    ]

    list_filter = [
        "batch_type",
        "asa_state",
        "virify_state",
        "map_state",
        "created_at",
    ]

    search_fields = [
        "study__accession",
        "study__title",
    ]

    readonly_fields = [
        "id",
        "batch_type",
        "total_analyses",
        "created_at",
        "updated_at",
        "prefect_flow_run_link",
        "asa_flow_run_link",
        "virify_flow_run_link",
        "map_flow_run_link",
        "asa_started_at",
        "asa_completed_at",
        "virify_started_at",
        "virify_completed_at",
        "map_started_at",
        "map_completed_at",
    ]

    inlines = [AnalysisInline]

    actions = ["regenerate_samplesheets"]

    @display(description="Study")
    def study_link(self, obj):
        """Link to the study admin page."""
        url = reverse("admin:analyses_study_change", args=[obj.study.accession])
        return format_html('<a href="{}">{}</a>', url, obj.study.accession)

    @display(
        description="ASA",
        label={
            PipelineState.PENDING: "info",
            PipelineState.IN_PROGRESS: "warning",
            PipelineState.READY: "success",
            PipelineState.FAILED: "danger",
        },
    )
    def asa_status_display(self, obj):
        """Display ASA pipeline status."""
        return obj.asa_state

    @display(
        description="VIRify",
        label={
            PipelineState.PENDING: "info",
            PipelineState.IN_PROGRESS: "warning",
            PipelineState.READY: "success",
            PipelineState.FAILED: "danger",
        },
    )
    def virify_status_display(self, obj):
        """Display VIRify pipeline status."""
        return obj.virify_state

    @display(
        description="MAP",
        label={
            PipelineState.PENDING: "info",
            PipelineState.IN_PROGRESS: "warning",
            PipelineState.READY: "success",
            PipelineState.FAILED: "danger",
        },
    )
    def map_status_display(self, obj):
        """Display MAP pipeline status."""
        return obj.map_state

    @display(description="Prefect Flow Run")
    def prefect_flow_run_link(self, obj):
        """Clickable link to main Prefect flow run."""
        if obj.prefect_flow_run_id:
            prefect_ui_url = os.getenv("PREFECT_UI_URL", "")
            if prefect_ui_url:
                flow_url = (
                    f"{prefect_ui_url}/flow-runs/flow-run/{obj.prefect_flow_run_id}"
                )
                return format_html(
                    '<a href="{}" target="_blank" rel="noopener noreferrer">'
                    'View in Prefect <span style="font-size: 0.8em;">↗</span></a>',
                    flow_url,
                )
            return obj.prefect_flow_run_id
        return "-"

    @display(description="ASA Flow Run")
    def asa_flow_run_link(self, obj):
        """Clickable link to ASA pipeline Prefect flow run."""
        if obj.asa_flow_run_id:
            prefect_ui_url = os.getenv("PREFECT_UI_URL", "")
            if prefect_ui_url:
                flow_url = f"{prefect_ui_url}/flow-runs/flow-run/{obj.asa_flow_run_id}"
                return format_html(
                    '<a href="{}" target="_blank" rel="noopener noreferrer">'
                    'View ASA Flow <span style="font-size: 0.8em;">↗</span></a>',
                    flow_url,
                )
            return obj.asa_flow_run_id
        return "-"

    @display(description="VIRify Flow Run")
    def virify_flow_run_link(self, obj):
        """Clickable link to VIRify pipeline Prefect flow run."""
        if obj.virify_flow_run_id:
            prefect_ui_url = os.getenv("PREFECT_UI_URL", "")
            if prefect_ui_url:
                flow_url = (
                    f"{prefect_ui_url}/flow-runs/flow-run/{obj.virify_flow_run_id}"
                )
                return format_html(
                    '<a href="{}" target="_blank" rel="noopener noreferrer">'
                    'View VIRify Flow <span style="font-size: 0.8em;">↗</span></a>',
                    flow_url,
                )
            return obj.virify_flow_run_id
        return "-"

    @display(description="MAP Flow Run")
    def map_flow_run_link(self, obj):
        """Clickable link to MAP pipeline Prefect flow run."""
        if obj.map_flow_run_id:
            prefect_ui_url = os.getenv("PREFECT_UI_URL", "")
            if prefect_ui_url:
                flow_url = f"{prefect_ui_url}/flow-runs/flow-run/{obj.map_flow_run_id}"
                return format_html(
                    '<a href="{}" target="_blank" rel="noopener noreferrer">'
                    'View MAP Flow <span style="font-size: 0.8em;">↗</span></a>',
                    flow_url,
                )
            return obj.map_flow_run_id
        return "-"

    @action(description="Regenerate samplesheets for selected batches")
    def regenerate_samplesheets(self, request, queryset):
        """
        Regenerate samplesheets for selected batches.
        Useful after adding/removing analyses from a batch.
        """
        count = 0
        for batch in queryset:
            # Refresh analysis count
            batch.refresh_analysis_count()

            # Regenerate ASA samplesheet
            batch.asa_samplesheet_path = None
            batch.save()
            batch.generate_asa_samplesheet()
            count += 1

        self.message_user(
            request,
            f"Successfully regenerated samplesheets for {count} batch(es).",
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
                    "results_dir",
                ]
            },
        ),
        (
            "ASA Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "asa_state",
                    "asa_flow_run_link",
                    "asa_samplesheet_path",
                    "asa_started_at",
                    "asa_completed_at",
                ],
            },
        ),
        (
            "VIRify Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "virify_state",
                    "virify_flow_run_link",
                    "virify_samplesheet_path",
                    "virify_started_at",
                    "virify_completed_at",
                ],
            },
        ),
        (
            "MAP Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "map_state",
                    "map_flow_run_link",
                    "map_samplesheet_path",
                    "map_started_at",
                    "map_completed_at",
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
                    "prefect_flow_run_link",
                    "created_at",
                    "updated_at",
                ],
            },
        ),
    )
