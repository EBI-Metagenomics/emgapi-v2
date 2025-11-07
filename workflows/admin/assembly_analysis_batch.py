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
        "total_analyses",
        "asa_counts_display",
        "virify_counts_display",
        "map_counts_display",
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
        "total_analyses",
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
        label={
            AssemblyAnalysisPipelineStatus.COMPLETED: "success",
            AssemblyAnalysisPipelineStatus.FAILED: "danger",
            AssemblyAnalysisPipelineStatus.RUNNING: "warning",
            AssemblyAnalysisPipelineStatus.PENDING: "info",
        },
    )
    def asa_counts_display(self, obj):
        """Display ASA pipeline status counts as badges."""
        if obj.pipeline_status_counts and hasattr(obj.pipeline_status_counts, "asa"):
            counts = obj.pipeline_status_counts.asa
            # TODO: this is repeated 3 times... I'll refactor this (mbc)
            badges = []
            if counts.completed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.COMPLETED, str(counts.completed))
                )
            if counts.failed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.FAILED, str(counts.failed))
                )
            if counts.running > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.RUNNING, str(counts.running))
                )
            if counts.pending > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.PENDING, str(counts.pending))
                )
            return badges if badges else "-"
        return "-"

    @display(
        description="VIRify",
        label={
            AssemblyAnalysisPipelineStatus.COMPLETED: "success",
            AssemblyAnalysisPipelineStatus.FAILED: "danger",
            AssemblyAnalysisPipelineStatus.RUNNING: "warning",
            AssemblyAnalysisPipelineStatus.PENDING: "info",
        },
    )
    def virify_counts_display(self, obj):
        """Display VIRify pipeline status counts as badges."""
        if obj.pipeline_status_counts and hasattr(obj.pipeline_status_counts, "virify"):
            counts = obj.pipeline_status_counts.virify
            badges = []
            if counts.completed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.COMPLETED, str(counts.completed))
                )
            if counts.failed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.FAILED, str(counts.failed))
                )
            if counts.running > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.RUNNING, str(counts.running))
                )
            if counts.pending > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.PENDING, str(counts.pending))
                )
            return badges if badges else "-"
        return "-"

    @display(
        description="MAP",
        label={
            AssemblyAnalysisPipelineStatus.COMPLETED: "success",
            AssemblyAnalysisPipelineStatus.FAILED: "danger",
            AssemblyAnalysisPipelineStatus.RUNNING: "warning",
            AssemblyAnalysisPipelineStatus.PENDING: "info",
        },
    )
    def map_counts_display(self, obj):
        """Display MAP pipeline status counts as badges."""
        if obj.pipeline_status_counts and hasattr(obj.pipeline_status_counts, "map"):
            counts = obj.pipeline_status_counts.map
            badges = []
            if counts.completed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.COMPLETED, str(counts.completed))
                )
            if counts.failed > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.FAILED, str(counts.failed))
                )
            if counts.running > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.RUNNING, str(counts.running))
                )
            if counts.pending > 0:
                badges.append(
                    (AssemblyAnalysisPipelineStatus.PENDING, str(counts.pending))
                )
            return badges if badges else "-"
        return "-"

    @action(description="Reset selected batches to PENDING for rerun")
    def mark_for_rerun(self, request, queryset):
        """
        Reset selected batches to PENDING status for all pipelines.
        Useful for rerunning failed batches.
        """
        from workflows.models import AssemblyAnalysisPipeline

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

    @action(description="Retry FAILED ASA analyses only")
    def retry_failed_asa(self, request, queryset):
        """
        Reset only FAILED ASA analyses to PENDING status.

        This allows selective retry of failed ASA analyses without reprocessing
        successful ones. Useful when:
        - Transient failures occurred (network, disk space)
        - Input data was fixed for specific analyses
        - Validation rules were updated

        TODO: the ASA, MAP and VIRify methods are repeated.. I'll refactor this (mbc)

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatch objects
        """
        total_reset = 0
        for batch in queryset:
            count = batch.batch_analyses.filter(
                asa_status=AssemblyAnalysisPipelineStatus.FAILED
            ).update(asa_status=AssemblyAnalysisPipelineStatus.PENDING)
            total_reset += count

            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.ASA)

        self.message_user(
            request,
            f"Reset {total_reset} FAILED ASA analysis/analyses to PENDING across {queryset.count()} batch(es).",
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
        total_reset = 0
        for batch in queryset:
            count = batch.batch_analyses.filter(
                virify_status=AssemblyAnalysisPipelineStatus.FAILED
            ).update(virify_status=AssemblyAnalysisPipelineStatus.PENDING)
            total_reset += count

            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)

        self.message_user(
            request,
            f"Reset {total_reset} FAILED VIRify analysis/analyses to PENDING across {queryset.count()} batch(es).",
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
        total_reset = 0
        for batch in queryset:
            count = batch.batch_analyses.filter(
                map_status=AssemblyAnalysisPipelineStatus.FAILED
            ).update(map_status=AssemblyAnalysisPipelineStatus.PENDING)
            total_reset += count

            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)

        self.message_user(
            request,
            f"Reset {total_reset} FAILED MAP analysis/analyses to PENDING across {queryset.count()} batch(es).",
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
