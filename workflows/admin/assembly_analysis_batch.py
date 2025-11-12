from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin, TabularInline
from unfold.contrib.filters.admin import AutocompleteSelectMultipleFilter
from unfold.decorators import display, action

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
    AssemblyAnalysisPipeline,
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
        "batch_link",
        "analysis_link",
        "assembly_link",
        "asa_status",
        "virify_status",
        "map_status",
        "disabled",
        "disabled_reason",
        "order",
    ]
    readonly_fields = ["batch_link", "analysis_link", "assembly_link"]
    can_delete = True

    @display(description="Batch")
    def batch_link(self, obj):
        """Link to the batch admin page."""
        url = reverse(
            "admin:workflows_assemblyanalysisbatch_change", args=[obj.batch.id]
        )
        return format_html('<a href="{}">{}</a>', url, str(obj.batch.id)[:8])

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
        "total_count",
        "completed_count",
        "pending_count",
        "running_count",
        "failed_count",
        "updated_at",
    ]

    list_filter = (
        ["study", AutocompleteSelectMultipleFilter],
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
        "error_log",
        "created_at",
        "updated_at",
    ]

    inlines = [AssemblyAnalysisBatchAnalysisInline]

    actions = [
        "mark_for_rerun",
        "retry_failed_asa",
        "retry_failed_virify",
        "retry_failed_map",
    ]

    @display(description="Batch ID")
    def id_short(self, obj):
        """Short batch ID for display."""
        return str(obj.id)[:8]

    @display(description="Study")
    def study_link(self, obj):
        """Link to the study admin page."""
        url = reverse("admin:analyses_study_change", args=[obj.study.accession])
        return format_html('<a href="{}">{}</a>', url, obj.study.accession)

    @display(description="Completed")
    def completed_count(self, obj):
        """
        Display completed count per pipeline (ASA, VIRify, MAP).
        """
        if not obj.pipeline_status_counts:
            return "-"

        asa = obj.pipeline_status_counts.asa.completed
        virify = obj.pipeline_status_counts.virify.completed
        map_count = obj.pipeline_status_counts.map.completed

        return f"A:{asa} V:{virify} M:{map_count}"

    @display(description="Failed")
    def failed_count(self, obj):
        """
        Display failed count per pipeline (A:ASA V:VIRify M:MAP).
        """
        if not obj.pipeline_status_counts:
            return "-"

        asa = obj.pipeline_status_counts.asa.failed
        virify = obj.pipeline_status_counts.virify.failed
        map_count = obj.pipeline_status_counts.map.failed

        return f"A:{asa} V:{virify} M:{map_count}"

    @display(description="Running")
    def running_count(self, obj):
        """
        Display running count per pipeline (A:ASA V:VIRify M:MAP).
        """
        if not obj.pipeline_status_counts:
            return "-"

        asa = obj.pipeline_status_counts.asa.running
        virify = obj.pipeline_status_counts.virify.running
        map_count = obj.pipeline_status_counts.map.running

        return f"A:{asa} V:{virify} M:{map_count}"

    @display(description="Pending")
    def pending_count(self, obj):
        """
        Display pending count per pipeline (A:ASA V:VIRify M:MAP).
        """
        if not obj.pipeline_status_counts:
            return "-"

        asa = obj.pipeline_status_counts.asa.pending
        virify = obj.pipeline_status_counts.virify.pending
        map_count = obj.pipeline_status_counts.map.pending

        return f"A:{asa} V:{virify} M:{map_count}"

    @display(description="Total")
    def total_count(self, obj):
        """
        Display sum of all statuses across all pipelines.
        """
        if not obj.pipeline_status_counts:
            return 0

        # Sum all statuses (completed, failed, running, pending) across all pipelines (ASA, VIRify, MAP)
        total = 0
        for pipeline in [
            obj.pipeline_status_counts.asa,
            obj.pipeline_status_counts.virify,
            obj.pipeline_status_counts.map,
        ]:
            total += pipeline.completed
            total += pipeline.failed
            total += pipeline.running
            total += pipeline.pending
        return total

    @action(description="Reset selected batches to PENDING")
    def mark_for_rerun(self, request, queryset):
        """
        Reset selected batches to PENDING status for all pipelines.
        Useful for rerunning failed batches.
        """
        for batch in queryset:
            # Reset all batch-analysis relationships to PENDING (excludes disabled)
            batch.batch_analyses.update(
                asa_status=AssemblyAnalysisPipelineStatus.PENDING,
                virify_status=AssemblyAnalysisPipelineStatus.PENDING,
                map_status=AssemblyAnalysisPipelineStatus.PENDING,
            )

            # Update counts to reflect a new status
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.ASA)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)

        self.message_user(
            request,
            f"Successfully reset {queryset.count()} batch(es) to PENDING status.",
        )

    def _retry_failed_pipeline(
        self, request, queryset, pipeline: AssemblyAnalysisPipeline, pipeline_name: str
    ):
        """
        Generic method to retry failed analyses for a specific pipeline.

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatch objects
        :param pipeline: Pipeline enum value (ASA, VIRIFY, MAP)
        :param pipeline_name: Human-readable pipeline name
        """
        status_field = f"{pipeline_name.lower()}_status"
        total_reset = 0

        for batch in queryset:
            count = batch.batch_analyses.filter(
                **{status_field: AssemblyAnalysisPipelineStatus.FAILED}
            ).update(**{status_field: AssemblyAnalysisPipelineStatus.PENDING})
            total_reset += count
            batch.update_pipeline_status_counts(pipeline)

        self.message_user(
            request,
            f"Reset {total_reset} FAILED {pipeline_name} analysis/analyses to PENDING across {queryset.count()} batch(es).",
        )

    @action(description="Retry FAILED ASA analyses only")
    def retry_failed_asa(self, request, queryset):
        """
        Reset only FAILED ASA analyses to PENDING status.

        This allows selective retry of failed ASA analyses without reprocessing
        successful ones. Useful when:
        - Transient failures occurred (network, disk space)
        - Input data was fixed for specific analyses
        - Validation rules were updated

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatch objects
        """
        self._retry_failed_pipeline(
            request, queryset, AssemblyAnalysisPipeline.ASA, "ASA"
        )

    @action(description="Retry FAILED VIRify analyses only")
    def retry_failed_virify(self, request, queryset):
        """
        Reset only FAILED VIRify analyses to PENDING status.

        This allows selective retry of failed VIRify analyses without reprocessing
        successful ones. Useful when:
        - Transient failures occurred (network, disk space)
        - Input data was fixed for specific analyses
        - Validation rules were updated

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatch objects
        """
        self._retry_failed_pipeline(
            request, queryset, AssemblyAnalysisPipeline.VIRIFY, "VIRify"
        )

    @action(description="Retry FAILED MAP analyses only")
    def retry_failed_map(self, request, queryset):
        """
        Reset only FAILED MAP analyses to PENDING status.

        This allows selective retry of failed MAP analyses without reprocessing
        successful ones. Useful when:
        - Transient failures occurred (network, disk space)
        - Input data was fixed for specific analyses
        - Validation rules were updated

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatch objects
        """
        self._retry_failed_pipeline(
            request, queryset, AssemblyAnalysisPipeline.MAP, "MAP"
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
            "Pipeline Status Counts",
            {
                "classes": ["tab"],
                "fields": [
                    "pipeline_status_counts",
                ],
            },
        ),
        (
            "ASA Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "asa_samplesheet_path",
                ],
            },
        ),
        (
            "VIRify Pipeline",
            {
                "classes": ["tab"],
                "fields": [
                    "virify_samplesheet_path",
                ],
            },
        ),
        (
            "MAP Pipeline",
            {
                "classes": ["tab"],
                "fields": [
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
                    "error_log",
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
