from django.contrib import admin
from django.db.models import Q
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin
from unfold.contrib.filters.admin import AutocompleteSelectMultipleFilter
from unfold.decorators import display, action

from workflows.admin.utils import STATUS_LABELS
from workflows.models import (
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisBatch,
)


class AssemblyBatchAnyPipelineStatusListFilter(admin.SimpleListFilter):
    title = "Batch pipelines status"
    parameter_name = "status"

    def lookups(self, request, model_admin):
        """Return available pipeline status values."""
        return AssemblyAnalysisPipelineStatus.choices

    def queryset(self, request, queryset):
        """Filter batches by pipeline status across all pipeline types."""
        status_filter = self.value()
        if status_filter:
            return queryset.filter(
                Q(asa_status=status_filter)
                | Q(virify_status=status_filter)
                | Q(map_status=status_filter)
            )
        return queryset


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

    list_filter_submit = True

    list_filter = [
        ("batch__study", AutocompleteSelectMultipleFilter),
        ("batch", AutocompleteSelectMultipleFilter),
        ("analysis__assembly", AutocompleteSelectMultipleFilter),
        AssemblyBatchAnyPipelineStatusListFilter,
        "asa_status",
        "virify_status",
        "map_status",
        "disabled",
        "created_at",
    ]

    search_fields = [
        "analysis__accession",
        "analysis__assembly__ena_accessions",
        "batch__study__accession",
        "batch__id",
    ]

    list_select_related = ["batch", "analysis", "analysis__assembly"]

    readonly_fields = ["created_at", "updated_at"]

    ordering = ["-created_at"]

    actions = [
        "disable_selected_relations",
        "enable_selected_relations",
        "reset_asa_to_pending",
        "reset_virify_to_pending",
        "reset_map_to_pending",
        "reset_all_pipelines_to_pending",
    ]

    def get_queryset(self, request):
        """
        Override to use all_objects manager to show disabled records in admin.

        :param request: The HTTP request
        :return: QuerySet including disabled batch assembly analyses
        :rtype: QuerySet
        """
        return self.model.all_objects.get_queryset()

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
            "Yes": "danger",
            "No": "success",
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

    @action(description="Reset ASA status to PENDING")
    def reset_asa_to_pending(self, request, queryset):
        """
        Reset ASA status to PENDING for selected analyses.

        Use this to retry specific analyses that failed ASA processing.
        The analyses will be reprocessed on the next flow run.

        TODO: these utility commands were created by Claude, they look OK to me but I need to refactor them (mbc)
              They are quite destructive, so probably should have some warnings or disabled them directly

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatchAnalysis objects
        """
        count = queryset.update(asa_status=AssemblyAnalysisPipelineStatus.PENDING)

        # Update counts for affected batches
        affected_batches = set(queryset.values_list("batch_id", flat=True))
        for batch_id in affected_batches:
            batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.ASA)

        self.message_user(
            request,
            f"Reset ASA status to PENDING for {count} analysis/analyses.",
        )

    @action(description="Reset VIRify status to PENDING")
    def reset_virify_to_pending(self, request, queryset):
        """
        Reset VIRify status to PENDING for selected analyses.

        Use this to retry specific analyses that failed VIRify processing.
        The analyses will be reprocessed on the next flow run.

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatchAnalysis objects
        """
        count = queryset.update(virify_status=AssemblyAnalysisPipelineStatus.PENDING)

        # Update counts for affected batches
        affected_batches = set(queryset.values_list("batch_id", flat=True))
        for batch_id in affected_batches:
            batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)

        self.message_user(
            request,
            f"Reset VIRify status to PENDING for {count} analysis/analyses.",
        )

    @action(description="Reset MAP status to PENDING")
    def reset_map_to_pending(self, request, queryset):
        """
        Reset MAP status to PENDING for selected analyses.

        Use this to retry specific analyses that failed MAP processing.
        The analyses will be reprocessed on the next flow run.

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatchAnalysis objects
        """
        count = queryset.update(map_status=AssemblyAnalysisPipelineStatus.PENDING)

        # Update counts for affected batches
        affected_batches = set(queryset.values_list("batch_id", flat=True))
        for batch_id in affected_batches:
            batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)

        self.message_user(
            request,
            f"Reset MAP status to PENDING for {count} analysis/analyses.",
        )

    @action(description="Reset ALL pipeline statuses to PENDING")
    def reset_all_pipelines_to_pending(self, request, queryset):
        """
        Reset all pipeline statuses (ASA, VIRify, MAP) to PENDING for selected analyses.

        Use this to completely reprocess selected analyses from scratch.

        :param request: Django admin request
        :param queryset: Selected AssemblyAnalysisBatchAnalysis objects
        """
        count = queryset.update(
            asa_status=AssemblyAnalysisPipelineStatus.PENDING,
            virify_status=AssemblyAnalysisPipelineStatus.PENDING,
            map_status=AssemblyAnalysisPipelineStatus.PENDING,
        )

        # Update counts for affected batches
        affected_batches = set(queryset.values_list("batch_id", flat=True))
        for batch_id in affected_batches:
            batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.ASA)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)
            batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)

        self.message_user(
            request,
            f"Reset all pipeline statuses to PENDING for {count} analysis/analyses.",
        )
