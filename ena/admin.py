from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin, ENABrowserLinkMixin
from .models import Sample, Study


@admin.register(Sample)
class SampleAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["accession", "additional_accessions"]
    autocomplete_fields = ["study"]


@admin.register(Study)
class StudyAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = ["accession", "additional_accessions"]
