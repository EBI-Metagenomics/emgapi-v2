from typing import Iterable

from django.contrib import admin
from django.core.validators import EMPTY_VALUES
from django.db.models import Q
from django.shortcuts import redirect
from django_admin_inline_paginator.admin import InlinePaginated
from unfold.admin import TabularInline
from unfold.contrib.filters.admin import TextFilter
from unfold.decorators import action

from analyses.base_models.base_models import ENADerivedModel


class TabularInlinePaginatedWithTabSupport(InlinePaginated, TabularInline):
    """
    Unfold tabular inline, plus support for paginating the inline.
    Tab = true gives the inline a whole tab of its own in unfold admin page.
    """

    template = "admin/tabular_inline_paginated_tabbed.html"
    tab = True


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
        raise NotImplemented

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


class ENABrowserLinkMixin:
    actions_detail = ["view_on_ena_browser"]

    @action(
        description="View on ENA browser",
    )
    def view_on_ena_browser(self, request, object_id):
        instance: type[ENADerivedModel] = self.model.objects.get(pk=object_id)
        return redirect(instance.ena_browser_url)
