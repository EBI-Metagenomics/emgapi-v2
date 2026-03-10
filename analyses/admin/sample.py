from django.contrib import admin
from unfold.admin import ModelAdmin
from unfold.decorators import display

from analyses.admin.base import (
    ENABrowserLinkMixin,
    JSONFieldWidgetOverridesMixin,
    TabularInlinePaginatedWithTabSupport,
)
from analyses.models import Sample, SampleRelatedSample


class SampleRelatedSamplesInline(TabularInlinePaginatedWithTabSupport):
    model = SampleRelatedSample
    autocomplete_fields = ["declaring_sample", "related_sample"]
    fk_name = "declaring_sample"
    extra = 0


@admin.register(Sample)
class SampleAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = [
        "id",
        "ena_accessions",
        "studies__accession",
        "studies__ena_accessions",
    ]
    autocomplete_fields = ["ena_sample", "ena_study", "studies"]
    list_display = ["first_accession", "updated_at", "display_accessions"]
    list_filter = ["updated_at", "created_at", "is_private"]
    inlines = [SampleRelatedSamplesInline]

    @display(description="ENA Accessions", label=True)
    def display_accessions(self, instance: Sample):
        return instance.ena_accessions
