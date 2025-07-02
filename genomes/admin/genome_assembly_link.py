from django.contrib import admin

from genomes.models.genome_assembly_link import GenomeAssemblyLink


class GenomeAssemblyLinkAdmin(admin.ModelAdmin):
    list_display = ["genome", "assembly", "created_at"]
    search_fields = ["genome__name", "assembly__name"]
    readonly_fields = ["created_at"]

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("genome", "assembly")

    def genome(self, obj):
        return obj.genome.name if obj.genome else "N/A"

    def assembly(self, obj):
        return obj.assembly.name if obj.assembly else "N/A"
admin.site.register(GenomeAssemblyLink, GenomeAssemblyLinkAdmin)