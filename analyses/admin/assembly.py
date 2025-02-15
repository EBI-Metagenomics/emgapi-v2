from typing import Iterable

from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import (
    ENABrowserLinkMixin,
    JSONFieldWidgetOverridesMixin,
    StatusListFilter,
    StudyFilter,
)
from analyses.models import Assembler, Assembly


class AssemblyStatusListFilter(StatusListFilter):
    def get_statuses(self) -> Iterable[str]:
        return Assembly.AssemblyStates


@admin.register(Assembly)
class AssemblyAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    def get_queryset(self, request):
        return self.model.all_objects.get_queryset()

    class StudyFilterForAssembly(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "reads_study__accession",
            "reads_study__ena_accessions",
            "assembly_study__accession",
            "assembly_study__ena_accessions",
        ]

    list_display = [
        "__str__",
        "created_at",
        "updated_at",
        "ena_study",
        "status_summary",
    ]
    list_filter = [
        StudyFilterForAssembly,
        "updated_at",
        "created_at",
        AssemblyStatusListFilter,
    ]
    list_filter_submit = True
    search_fields = [
        "id",
        "run__ena_accessions",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "assembly_study__accession",
        "assembly_study__ena_study__accession",
        "assembly_study__ena_study__additional_accessions",
        "reads_study__accession",
        "reads_study__title",
        "reads_study__ena_study__accession",
        "reads_study__ena_study__additional_accessions",
    ]

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


@admin.register(Assembler)
class AssemblerAdmin(ModelAdmin):
    pass
