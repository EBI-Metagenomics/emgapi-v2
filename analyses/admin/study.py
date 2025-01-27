import json

from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.db import models
from django.db.models import Count
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy
from django.utils.html import format_html
from unfold.admin import ModelAdmin
from unfold.decorators import action, display

from analyses.admin.analysis import AnalysisStatusListFilter
from analyses.admin.base import (
    ENABrowserLinkMixin,
    JSONFieldWidgetOverridesMixin,
    StudyFilter,
    TabularInlinePaginatedWithTabSupport,
)
from analyses.models import Analysis, Assembly, Run, Study
from emgapiv2.widgets import StatusPathwayWidget


class StudyRunsInline(TabularInlinePaginatedWithTabSupport):
    model = Run
    pagination_key = "study_runs_page"
    per_page = 10

    show_change_link = True
    fields = [
        "first_accession",
        "experiment_type",
        "sample",
        "latest_analysis_status_display",
    ]
    readonly_fields = ["first_accession", "latest_analysis_status_display", "sample"]
    max_num = 0
    ordering = ["-updated_at"]

    def latest_analysis_status_display(self, obj: Run):
        if obj.latest_analysis_status:
            widget = StatusPathwayWidget(
                pathway=[
                    Analysis.AnalysisStates.ANALYSIS_STARTED,
                    Analysis.AnalysisStates.ANALYSIS_FAILED,
                    Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
                    Analysis.AnalysisStates.ANALYSIS_BLOCKED,
                    Analysis.AnalysisStates.ANALYSIS_COMPLETED,
                    Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                ]
            )
            return widget.render(
                "latest_analysis_status", json.dumps(obj.latest_analysis_status)
            )
        return "No Status"


class StudyAssembliesInline(TabularInlinePaginatedWithTabSupport):
    model = Assembly
    verbose_name = "Assembly in this study"
    verbose_name_plural = "Assemblies in this study"
    show_change_link = True
    fields = ["run", "status", "dir"]
    readonly_fields = ["run"]
    max_num = 0
    fk_name = "assembly_study"
    pagination_key = "study_assemblies_page"
    per_page = 10
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Assembly.AssemblyStates.ASSEMBLY_STARTED,
                    Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                ]
            )
        },
    }


class StudyReadsInline(TabularInlinePaginatedWithTabSupport):
    model = Assembly
    verbose_name = "Assembly of this studies' read"
    verbose_name_plural = "Assemblies of this studies' reads"
    show_change_link = True
    fields = ["run", "status", "dir"]
    readonly_fields = ["run"]
    max_num = 0
    fk_name = "reads_study"
    pagination_key = "study_assemblies_reads_page"
    per_page = 10
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Assembly.AssemblyStates.ASSEMBLY_STARTED,
                    Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                ]
            )
        },
    }


@admin.register(Study)
class StudyAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    def get_queryset(self, request):
        return self.model.all_objects.get_queryset()

    inlines = [StudyRunsInline, StudyAssembliesInline, StudyReadsInline]
    list_display = ["accession", "updated_at", "title", "display_accessions"]
    list_filter = ["updated_at", "created_at", "is_private"]
    search_fields = [
        "accession",
        "title",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "biome__biome_name",
    ]
    actions_detail = [
        "show_assembly_status_summary",
        "show_run_type_summary",
        "show_analysis_status_summary",
    ] + ENABrowserLinkMixin.actions_detail

    fieldsets = (
        (None, {"fields": ["title", "ena_study", "biome", "ena_accessions"]}),
        (
            "Status and ownership",
            {
                "classes": ["tab"],
                "fields": [
                    "is_ready",
                    "is_private",
                    "is_suppressed",
                    "webin_submitter",
                ],
            },
        ),
    )

    @display(description="ENA Accessions", label=True)
    def display_accessions(self, instance: Study):
        return instance.ena_accessions

    @action(
        description="Report: assembly statuses",
        url_path="study-assembly-status-summary",
    )
    def show_assembly_status_summary(self, request, object_id):
        study = get_object_or_404(Study, pk=object_id)
        assemblies_per_state = {
            state: study.assemblies_reads.filter(
                **{f"status__{state.value}": True}
            ).count()
            for state in Assembly.AssemblyStates
        }
        assemblies_total = study.assemblies_reads.count()

        def make_state_link(state: Assembly.AssemblyStates) -> str:
            url = reverse_lazy("admin:analyses_assembly_changelist")
            url += f"?status={state}&study_accession={study.accession}"
            return format_html(
                "<a class='flex items-center' href='{}'>"
                "<span>{}</span>"
                "<span class='material-symbols-outlined ml-2'>arrow_forward</span>"
                "</a>",
                url,
                state.value,
            )

        assemblies_status_table = {
            "headers": ["Assemblies with state", "Count"],
            "rows": [["Total", assemblies_total]]
            + [
                [make_state_link(state), count]
                for state, count in assemblies_per_state.items()
            ],
        }

        assemblies_progress = [
            {
                "state": state.value,
                "value": (100 * count / assemblies_total) if assemblies_total else 0,
            }
            for state, count in assemblies_per_state.items()
        ]

        return render(
            request,
            "admin/study_admin_assembly_status_summary.html",
            {
                "study": study,
                "assemblies_status_table": assemblies_status_table,
                "assemblies_progress": assemblies_progress,
                "title": f"Assemblies status summary for reads of {study.accession}",
                **self.admin_site.each_context(request),
            },
        )

    @action(
        description="Report: run types",
        url_path="study-run-type-summary",
    )
    def show_run_type_summary(self, request, object_id):
        study = get_object_or_404(Study, pk=object_id)

        runs_per_experiment_type = study.runs.values("experiment_type").annotate(
            count=Count("experiment_type")
        )

        runs_total = study.runs.count()

        def make_experiment_type_link(experiment_type_code: str) -> str:
            url = reverse_lazy("admin:analyses_run_changelist")
            url += f"?experiment_type__exact={experiment_type_code}&study_accession={study.accession}"
            return format_html(
                "<a class='flex items-center' href='{}'>"
                "<span>{}</span>"
                "<span class='material-symbols-outlined ml-2'>arrow_forward</span>"
                "</a>",
                url,
                Run.ExperimentTypes(experiment_type_code).label,
            )

        runs_types_table = {
            "headers": ["Runs of type", "Count"],
            "rows": [["Total", runs_total]]
            + [
                [
                    make_experiment_type_link(per_type.get("experiment_type")),
                    per_type.get("count"),
                ]
                for per_type in runs_per_experiment_type
            ],
        }

        return render(
            request,
            "admin/study_admin_run_type_summary.html",
            {
                "study": study,
                "run_types_table": runs_types_table,
                "title": f"Experiment types summary for runs of {study.accession}",
                **self.admin_site.each_context(request),
            },
        )

    @action(
        description="Report: analysis statuses",
        url_path="study-analysis-status-summary",
    )
    def show_analysis_status_summary(self, request, object_id):
        study = get_object_or_404(Study, pk=object_id)
        analyses_per_state = {
            state: study.analyses.filter(**{f"status__{state.value}": True}).count()
            for state in Analysis.AnalysisStates
        }
        analyses_total = study.analyses.count()

        def make_state_link(state: Analysis.AnalysisStates) -> str:
            url = reverse_lazy("admin:analyses_analysis_changelist")
            url += f"?{AnalysisStatusListFilter.parameter_name}={state.value}&{StudyFilter.parameter_name}={study.accession}"
            return format_html(
                "<a class='flex items-center' href='{}'>"
                "<span>{}</span>"
                "<span class='material-symbols-outlined ml-2'>arrow_forward</span>"
                "</a>",
                url,
                state.value,
            )

        analyses_status_table = {
            "headers": ["Analyses with state", "Count"],
            "rows": [["Total", analyses_total]]
            + [
                [make_state_link(state), count]
                for state, count in analyses_per_state.items()
            ],
        }

        analyses_progress = [
            {
                "state": state.value,
                "value": (100 * count / analyses_total) if analyses_total else 0,
            }
            for state, count in analyses_per_state.items()
        ]

        return render(
            request,
            "admin/study_admin_analysis_status_summary.html",
            {
                "study": study,
                "analyses_status_table": analyses_status_table,
                "analyses_progress": analyses_progress,
                "title": f"Analysis status summary for {study.accession}",
                **self.admin_site.each_context(request),
            },
        )


@staff_member_required
def jump_to_latest_study_admin(request):
    latest_study = Study.objects.order_by("-updated_at").first()
    return redirect(
        reverse_lazy(
            "admin:analyses_study_change", kwargs={"object_id": latest_study.pk}
        )
    )
