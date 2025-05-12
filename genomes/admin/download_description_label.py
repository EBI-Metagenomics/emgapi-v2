from genomes.models import DownloadDescriptionLabel


@admin.register(DownloadDescriptionLabel)
class DownloadDescriptionLabelAdmin(admin.ModelAdmin):
    readonly_fields = [
        'description_id'
    ]
    search_fields = [
        'description',
        'description_label'
    ]
    list_display = [
        'description_id',
        'description',
        'description_label'
    ]