from django.contrib import admin, messages
from django.core.exceptions import ValidationError
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import reverse
from unfold.admin import ModelAdmin
from unfold.decorators import action
from unfold.enums import ActionVariant

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from genomes.models import GenomeCatalogue


@admin.register(GenomeCatalogue)
class GenomeCatalogueAdmin(JSONFieldWidgetOverridesMixin, ModelAdmin):
    search_fields = [
        "name",
        "catalogue_id",
        "description",
        "series__name",
        "series__catalogue_biome_label",
    ]
    list_display = [
        "catalogue_id",
        "name",
        "series",
        "version",
        "status",
        "calculate_genome_count",
    ]
    list_filter = ["status", "series__catalogue_type", "series"]
    actions = ["publish_selected_releases"]
    actions_detail = ["publish_release"]
    readonly_fields = [
        "status",
        "published_at",
        "retired_at",
        "catalogue_biome_label",
        "catalogue_type",
        "biome",
        "calculate_genome_count",
    ]

    fieldsets = (
        (
            None,
            {
                "fields": [
                    "catalogue_id",
                    "series",
                    "name",
                    "version",
                    "status",
                    "published_at",
                    "retired_at",
                ]
            },
        ),
        (
            "Metadata",
            {
                "fields": [
                    "calculate_genome_count",
                    "unclustered_genome_count",
                    "catalogue_biome_label",
                    "catalogue_type",
                    "biome",
                    "updated_at",
                    "description",
                    "other_stats",
                ],
                "classes": ["tab"],
            },
        ),
        (
            "Files",
            {
                "classes": ["tab"],
                "fields": ["downloads", "result_directory", "ftp_url"],
            },
        ),
        (
            "Protein catalogue",
            {
                "classes": ["tab"],
                "fields": [
                    "protein_catalogue_name",
                    "protein_catalogue_description",
                ],
            },
        ),
        (
            "Pipeline",
            {
                "fields": ["pipeline_version_tag"],
                "classes": ["tab"],
            },
        ),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("series__biome")

    def get_actions_detail(self, request, object_id):
        catalogue = self.get_queryset(request).filter(pk=object_id).first()
        if catalogue is None or catalogue.status != GenomeCatalogue.Status.READY:
            return []
        return super().get_actions_detail(request, object_id)

    def _publish(self, request, catalogue_ids):
        try:
            result = GenomeCatalogue.publish_ready(catalogue_ids)
        except ValidationError as exc:
            self.message_user(request, " ".join(exc.messages), level=messages.ERROR)
            return None

        retired = len(result["retired"])
        published = len(result["published"])
        self.message_user(
            request,
            f"Published {published} catalogue release(s) and retired {retired} previous release(s) atomically.",
            level=messages.SUCCESS,
        )
        return result

    @action(
        description="Publish catalogue",
        permissions=["change"],
        icon="publish",
        variant=ActionVariant.PRIMARY,
    )
    def publish_release(self, request, object_id):
        catalogue = get_object_or_404(self.get_queryset(request), pk=object_id)
        change_url = reverse(
            "admin:genomes_genomecatalogue_change", args=[catalogue.pk]
        )

        if request.method == "POST":
            self._publish(request, [catalogue.pk])
            return redirect(change_url)

        context = {
            **self.admin_site.each_context(request),
            "title": f"Publish {catalogue.name}?",
            "opts": self.model._meta,
            "original": catalogue,
            "catalogue": catalogue,
            "change_url": change_url,
        }
        return TemplateResponse(
            request,
            "admin/genomes/genomecatalogue/publish_confirmation.html",
            context,
        )

    @admin.action(
        description="Publish selected ready releases and retire their current versions"
    )
    def publish_selected_releases(self, request, queryset):
        self._publish(request, queryset.values_list("pk", flat=True))
