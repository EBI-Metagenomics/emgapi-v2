from django.contrib import admin
from django.db.models import JSONField
from unfold.admin import ModelAdmin

from analyses.admin.widgets import JSONTreeWidget

from .models import KeyValueStore


@admin.register(KeyValueStore)
class KeyValueStoreAdmin(ModelAdmin):
    list_display = ("key", "updated_at")
    search_fields = ("key",)
    readonly_fields = ("updated_at",)

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        if isinstance(db_field, JSONField):
            kwargs["widget"] = JSONTreeWidget
        return super().formfield_for_dbfield(db_field, request, **kwargs)
