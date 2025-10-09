from typing import Iterable, Optional

from django import forms
from django.contrib import admin, messages
from django.contrib.postgres.fields import ArrayField
from django.core.validators import EMPTY_VALUES
from django.db.models import JSONField, Q
from django.forms import Field
from django.http import HttpRequest
from django.shortcuts import redirect
from django.urls import reverse
from unfold.admin import ModelAdmin, TabularInline
from unfold.contrib.filters.admin import TextFilter
from unfold.contrib.forms.widgets import ArrayWidget
from unfold.decorators import action

from analyses.admin.widgets import ENAAccessionsListWidget, JSONTreeWidget
from analyses.base_models.base_models import ENADerivedModel


class TabularInlinePaginatedWithTabSupport(TabularInline):
    """
    Unfold tabular inline, plus support for paginating the inline.
    Tab = true gives the inline a whole tab of its own in unfold admin page.
    """

    tab = True
    per_page = 10


class StudyFilter(TextFilter):
    title = "study accession"
    parameter_name = "study_accession"

    study_accession_search_fields = ["ena_study__accession"]

    def queryset(self, request, queryset):
        if self.value() in EMPTY_VALUES:
            return queryset

        filters = Q()
        for field in self.study_accession_search_fields:
            filters |= Q(**{f"{field}__icontains": self.value()})

        return queryset.filter(filters)


class StatusListFilter(admin.SimpleListFilter):
    def get_statuses(self) -> Iterable[str]:
        raise NotImplementedError

    title = "status"  # title of the filter
    parameter_name = "status"  # url param for the filter

    status_field = "status"  # JSON field on the model containing statuses

    def lookups(self, request, model_admin):
        return [
            (state, state.replace("_", " ").title()) for state in self.get_statuses()
        ]

    def queryset(self, request, queryset):
        if self.value() is None:
            return queryset
        return queryset.filter(**{f"{self.status_field}__{self.value()}": True})

    # TODO: facet counts


class ENABrowserLinkMixin:
    actions_detail = ["view_on_ena_browser"]

    @action(
        description="View on ENA browser",
    )
    def view_on_ena_browser(self, request, object_id):
        instance: type[ENADerivedModel] = self.model.objects.get(pk=object_id)
        return redirect(instance.ena_browser_url)


class JSONFieldWidgetOverridesMixin(ModelAdmin):
    def formfield_for_dbfield(
        self, db_field: Field, request: HttpRequest, **kwargs
    ) -> Optional[Field]:
        if isinstance(db_field, JSONField) and db_field.name in [
            "ena_accessions",
            "additional_accessions",
        ]:
            kwargs["widget"] = ENAAccessionsListWidget
        elif isinstance(db_field, JSONField):
            kwargs["widget"] = JSONTreeWidget
        if isinstance(db_field, ArrayField):
            kwargs["widget"] = ArrayWidget
        return super().formfield_for_dbfield(db_field, request, **kwargs)

    def render_change_form(
        self, request, context, add=False, change=False, form_url="", obj=None
    ):
        form = context.get("adminform").form
        readonly_fields = self.get_readonly_fields(request, obj)
        for field in form.fields:
            if field in readonly_fields:
                form.fields["myfield"].widget = self.formfield_for_dbfield(
                    field, request
                ).widget
        return super().render_change_form(request, context, add, change, form_url, obj)


class AutoCompleteInlineForm(forms.ModelForm):
    """This is a workaround for a (probable?) bug in django unfold where the wrong field name is sent with autocomplete ajax requests"""

    autocomplete_fields: list[str] = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.autocomplete_fields:
            raise NotImplementedError("No autocomplete fields defined")
        for field_name in self.autocomplete_fields:
            field = self.fields.get(field_name)
            field.widget.attrs["field-name"] = field_name


def detail_action_error(request, message: str = "Something went wrong"):
    """
    Handle an error in an unfold detail action.
    Just posts a message to the admin user and handles the default redirect.
    """
    messages.add_message(request, messages.WARNING, message)
    referer = request.META.get("HTTP_REFERER", reverse("admin:index"))
    return redirect(referer)
