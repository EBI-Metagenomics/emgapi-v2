from typing import Iterable

from django.contrib import admin
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from unfold.admin import ModelAdmin
from unfold.decorators import action

from analyses.admin.base import (
    JSONFieldWidgetOverridesMixin,
    StatusListFilter,
    StudyFilter,
)
from analyses.models import Analysis


class AnalysisStatusListFilter(StatusListFilter):
    def get_statuses(self) -> Iterable[str]:
        return Analysis.AnalysisStates


@admin.register(Analysis)
class AnalysisAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    def get_queryset(self, request):
        return self.model.all_objects.get_queryset()

    list_display = [
        "__str__",
        "assembly_or_run",
        "created_at",
        "updated_at",
        "study",
        "status_summary",
    ]

    search_fields = [
        "id",
        "run__ena_accessions",
        "assembly__ena_accessions",
        "assembly__run__ena_accessions",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "study__accession",
        "study__title",
        "study__ena_accessions",
        "pipeline_version",
    ]

    readonly_fields = ["created_at", "accession"]

    fieldsets = (
        (None, {"fields": ["accession"]}),
        (
            "Analysed data",
            {
                "classes": ["tab"],
                "fields": [
                    "ena_study",
                    "study",
                    "sample",
                    "run",
                    "assembly",
                    "experiment_type",
                ],
            },
        ),
        (
            "Status and ownership",
            {
                "classes": ["tab"],
                "fields": [
                    "is_ready",
                    "is_private",
                    "webin_submitter",
                    "status",
                    "is_suppressed",
                ],
            },
        ),
        ("Files", {"classes": ["tab"], "fields": ["downloads", "results_dir"]}),
        ("QC", {"classes": ["tab"], "fields": ["quality_control"]}),
    )

    class StudyFilterForAnalysis(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "study__accession",
        ]

    list_filter = [
        StudyFilterForAnalysis,
        "pipeline_version",
        "experiment_type",
        "updated_at",
        "created_at",
        "is_private",
        AnalysisStatusListFilter,
    ]
    list_filter_submit = True

    def status_summary(self, obj):
        if (not obj.status) or (type(obj.status) is not dict):
            return None
        return " — ".join(
            [
                status.upper()
                for status, is_set in obj.status.items()
                if is_set and not status.endswith("reason")
            ]
        )

    actions_detail = ["get_annotations_json"]

    @action(
        description="Download: annotations JSON",
        url_path="analysis-annotations-json",
    )
    def get_annotations_json(self, request, object_id):
        analysis = get_object_or_404(Analysis, pk=object_id)
        response = JsonResponse(analysis.annotations)
        response["Content-Disposition"] = (
            f'attachment; filename="{analysis.accession}_annotations.json"'
        )
        return response
