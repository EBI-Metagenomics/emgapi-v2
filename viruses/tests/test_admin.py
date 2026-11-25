"""Verify virus administration uses the shared Unfold and JSON widgets."""

import pytest
from django.contrib import admin
from django.urls import reverse
from unfold.admin import ModelAdmin

from analyses.admin.base import JSONFieldWidgetOverridesMixin
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment, ViralFragment


@pytest.mark.django_db
@pytest.mark.parametrize(
    "model", [ViralCatalogue, ViralFragment, ViralCatalogueViralFragment]
)
def test_virus_admin_uses_unfold(admin_client, model):
    """Render each list and creation form with Unfold support."""
    assert isinstance(admin.site._registry[model], ModelAdmin)
    if model != ViralFragment:
        assert isinstance(admin.site._registry[model], JSONFieldWidgetOverridesMixin)
    name = model._meta.model_name
    for action in ["changelist", "add"]:
        response = admin_client.get(reverse(f"admin:viruses_{name}_{action}"))
        assert response.status_code == 200
        assert "unfold/" in response.content.decode()
