from django.contrib import admin
from django.shortcuts import redirect
from unfold.admin import ModelAdmin
from unfold.decorators import action
from unfold.enums import ActionVariant

from analyses.admin.base import (
    JSONFieldWidgetOverridesMixin,
    TabularInlinePaginatedWithTabSupport,
    AutoCompleteInlineForm,
    detail_action_error,
)
from analyses.models import Publication, StudyPublication


class PublicationStudyInlineForm(AutoCompleteInlineForm):
    autocomplete_fields = ["study"]

    class Meta:
        model = StudyPublication
        fields = "__all__"


class StudyPublicationInline(TabularInlinePaginatedWithTabSupport):
    model = StudyPublication
    form = PublicationStudyInlineForm
    autocomplete_fields = ["study"]
    extra = 0


@admin.register(Publication)
class PublicationAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    list_display = ["pubmed_id", "title", "published_year"]
    search_fields = ["title", "pubmed_id", "metadata__authors", "metadata__doi"]
    inlines = [StudyPublicationInline]

    actions_detail = ["read_publication"]

    @action(
        description="View publication",
        variant=ActionVariant.INFO,
    )
    def read_publication(self, request, object_id):
        instance: Publication = self.model.objects.get(pk=object_id)
        if not instance.metadata.doi:
            return detail_action_error(request, "No DOI known for this publication")
        return redirect(f"https://doi.org/{instance.metadata.doi}")

    fieldsets = (
        (None, {"fields": ["pubmed_id", "title", "published_year"]}),
        (
            "Additional Information",
            {
                "fields": ["metadata"],
            },
        ),
    )
