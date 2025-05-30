from django.contrib import admin
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
