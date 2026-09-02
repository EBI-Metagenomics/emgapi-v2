from django.contrib import admin, messages
from django.contrib.admin import helpers
from django.shortcuts import render
from unfold.admin import ModelAdmin
from unfold.decorators import display

from analyses.admin.base import (
    ENABrowserLinkMixin,
    JSONFieldWidgetOverridesMixin,
    StudyFilter,
)
from analyses.models import Run


@admin.register(Run)
class RunAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    actions = ["set_experiment_type"]

    class StudyFilterForRun(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "study__accession",
            "study__ena_accessions",
        ]

    list_display = [
        "id",
        "display_accessions",
        "updated_at",
        "experiment_type",
        "study",
    ]
    list_filter = [StudyFilterForRun, "experiment_type", "updated_at"]
    list_filter_submit = True
    search_fields = [
        "ena_accessions",
        "experiment_type",
        "id",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "sample__ena_accessions",
        "study__accession",
    ]
    autocomplete_fields = ["ena_study", "study", "sample"]
    readonly_fields = ["instrument_model", "instrument_platform"]

    @display(description="Accessions", label=True)
    def display_accessions(self, instance: Run):
        return instance.ena_accessions

    fieldsets = (
        (None, {"fields": ["ena_accessions", "experiment_type"]}),
        (
            "Related",
            {
                "classes": ["tab"],
                "fields": [
                    "ena_study",
                    "study",
                    "sample",
                ],
            },
        ),
        (
            "Status and ownership",
            {
                "classes": ["tab"],
                "fields": [
                    "is_private",
                    "webin_submitter",
                    "is_suppressed",
                ],
            },
        ),
        (
            "Metadata",
            {
                "classes": ["tab"],
                "fields": ["metadata", "instrument_model", "instrument_platform"],
            },
        ),
    )

    @admin.action(description="Set experiment type on selected runs")
    def set_experiment_type(self, request, queryset):
        if request.POST.get("apply") == "1":
            experiment_type = request.POST.get("experiment_type")
            if experiment_type not in Run.ExperimentTypes.values:
                self.message_user(
                    request, "Select an experiment type.", messages.WARNING
                )
                return None

            updated = queryset.update(experiment_type=experiment_type)
            self.message_user(
                request,
                f"Set experiment type on {updated} run(s).",
                messages.SUCCESS,
            )
            return None

        select_across = request.POST.get("select_across") == "1"
        selected_count = queryset.count()
        return render(
            request,
            "admin/run_set_experiment_type_confirmation.html",
            {
                **self.admin_site.each_context(request),
                "title": "Set experiment type",
                "runs": queryset.order_by("id")[:100],
                "selected_count": selected_count,
                "experiment_type_choices": Run.ExperimentTypes.choices,
                "action_checkbox_name": helpers.ACTION_CHECKBOX_NAME,
                "selected_ids": (
                    [] if select_across else queryset.values_list("pk", flat=True)
                ),
                "select_across": select_across,
            },
        )
