from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import (
    TabularInlinePaginatedWithTabSupport,
    AutoCompleteInlineForm,
)
from analyses.models import SuperStudy, SuperStudyStudy, SuperStudyGenomeCatalogue


class SuperStudyStudyInlineForm(AutoCompleteInlineForm):
    autocomplete_fields = ["study"]

    class Meta:
        model = SuperStudyStudy
        fields = "__all__"


class SuperStudyGenomeCatalogueInlineForm(AutoCompleteInlineForm):
    autocomplete_fields = ["genome_catalogue"]

    class Meta:
        model = SuperStudyGenomeCatalogue
        fields = "__all__"


class SuperStudyStudyInline(TabularInlinePaginatedWithTabSupport):
    model = SuperStudyStudy
    form = SuperStudyStudyInlineForm
    autocomplete_fields = ["study"]
    extra = 0


class SuperStudyGenomeCatalogueInline(TabularInlinePaginatedWithTabSupport):
    model = SuperStudyGenomeCatalogue
    form = SuperStudyGenomeCatalogueInlineForm
    autocomplete_fields = ["genome_catalogue"]
    extra = 0


@admin.register(SuperStudy)
class SuperStudyAdmin(ModelAdmin):
    inlines = [SuperStudyStudyInline, SuperStudyGenomeCatalogueInline]
    search_fields = ["title", "slug"]
