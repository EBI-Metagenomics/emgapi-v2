from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.models import ComputeResourceHeuristic


@admin.register(ComputeResourceHeuristic)
class ComputeResourceHeuristicAdmin(ModelAdmin):
    search_fields = ["biome__path", "assembler__name", "process"]
    list_filter = [
        "assembler",
        "process",
    ]
    autocomplete_fields = ["biome"]
