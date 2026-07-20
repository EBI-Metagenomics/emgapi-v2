from django.contrib import admin
from unfold.admin import ModelAdmin

from genomes.models import SourmashSearchJob, SourmashSearchJobItem


@admin.register(SourmashSearchJob)
class SourmashSearchJobAdmin(ModelAdmin):
    list_display = ("id", "status", "created_at", "started_at", "finished_at")
    readonly_fields = (
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
        "raw_results_archive_path",
        "error_summary",
    )
    search_fields = ("id", "submitted_by")
    list_filter = ("status",)


@admin.register(SourmashSearchJobItem)
class SourmashSearchJobItemAdmin(ModelAdmin):
    list_display = (
        "id",
        "job",
        "query_original_name",
        "search_index",
        "status",
        "created_at",
        "finished_at",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
        "task_result_id",
        "raw_csv_path",
        "result_summary",
        "error_message",
    )
    search_fields = (
        "id",
        "job__id",
        "query_original_name",
        "search_index__catalogue__catalogue_id",
    )
    list_filter = ("status", "search_index__backend")
