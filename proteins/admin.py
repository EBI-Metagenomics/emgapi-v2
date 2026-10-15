from django.contrib import admin
from unfold.admin import ModelAdmin

from proteins.models import AssemblyProteins, DatasetOwner, DatasetUpdate

# There is no admin for Protein: the admin counts all rows of a table, and it has billions.


class ReadOnlyAdmin(ModelAdmin):
    """The MGYP database is written by the flows only."""

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(AssemblyProteins)
class AssemblyProteinsAdmin(ReadOnlyAdmin):
    list_display = (
        "assembly_accession",
        "source",
        "pipeline_version",
        "analysis_accession",
        "ingested_at",
    )
    list_filter = ("source",)
    search_fields = ("assembly_accession", "analysis_accession")
    show_full_result_count = False


@admin.register(DatasetUpdate)
class DatasetUpdateAdmin(ReadOnlyAdmin):
    list_display = (
        "day",
        "status",
        "assembly_proteins",
        "proteins",
        "occurrences",
        "finished_at",
    )
    list_filter = ("status",)
    ordering = ("-id",)


@admin.register(DatasetOwner)
class DatasetOwnerAdmin(ReadOnlyAdmin):
    list_display = ("flow", "slurm_job_id", "slurm_submit_at", "acquired_at")
