from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import (
    TabularInlinePaginatedWithTabSupport,
    AutoCompleteInlineForm,
)
from analyses.models import SuperStudy, SuperStudyStudy


class SuperStudyStudyInlineForm(AutoCompleteInlineForm):
    autocomplete_fields = ["study"]

    class Meta:
        model = SuperStudyStudy
        fields = "__all__"


class SuperStudyStudyInline(TabularInlinePaginatedWithTabSupport):
    model = SuperStudyStudy
    form = SuperStudyStudyInlineForm
    autocomplete_fields = ["study"]
    extra = 0


@admin.register(SuperStudy)
class SuperStudyAdmin(ModelAdmin):
    inlines = [SuperStudyStudyInline]
    search_fields = ["title", "slug"]
