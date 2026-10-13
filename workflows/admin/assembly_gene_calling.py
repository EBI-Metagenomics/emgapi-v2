from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from unfold.admin import ModelAdmin
from unfold.decorators import display

from workflows.admin.utils import STATUS_LABELS
from workflows.models import AssemblyGeneCalling


@admin.register(AssemblyGeneCalling)
class AssemblyGeneCallingAdmin(ModelAdmin):
    """Gene calling + assembly analysis: the gene calling pipeline, the protein accessioning, and the start of the assembly analysis."""

    list_display = [
        "id",
        "assembly_link",
        "pipeline_version",
        "gene_calling_status_badge",
        "accessioning_status_badge",
        "analysis_link",
        "created_at",
    ]
    list_filter = [
        "gene_calling_status",
        "accessioning_status",
        "pipeline_version",
        "created_at",
    ]
    search_fields = ["assembly__ena_accessions", "analysis__accession"]
    list_select_related = ["assembly", "analysis"]
    autocomplete_fields = ["assembly", "analysis"]
    readonly_fields = ["assembly_proteins_id", "created_at", "updated_at"]
    ordering = ["-created_at"]

    @display(description="Assembly")
    def assembly_link(self, obj):
        url = reverse("admin:analyses_assembly_change", args=[obj.assembly.id])
        return format_html('<a href="{}">{}</a>', url, obj.assembly.first_accession)

    @display(description="Analysis")
    def analysis_link(self, obj):
        url = reverse("admin:analyses_analysis_change", args=[obj.analysis.id])
        return format_html('<a href="{}">{}</a>', url, obj.analysis.accession)

    @display(description="Gene calling", label=STATUS_LABELS)
    def gene_calling_status_badge(self, obj):
        return obj.gene_calling_status

    @display(description="Accessioning", label=STATUS_LABELS)
    def accessioning_status_badge(self, obj):
        return obj.accessioning_status
