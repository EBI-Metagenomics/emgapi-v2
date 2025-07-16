from django.contrib import admin
from unfold.admin import ModelAdmin
from analyses.admin.base import TabularInlinePaginatedWithTabSupport

from django import forms

from analyses.models import SuperStudy, SuperStudyStudy


class SuperStudyStudyInlineForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        study_field = self.fields.get("study")
        study_field.widget.attrs["field-name"] = "study"

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
