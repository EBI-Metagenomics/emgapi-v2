"""Verify staff-issued Webin tokens and object destinations."""

from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest
from django.conf import settings
from django.contrib import admin
from django.contrib.admin.models import LogEntry
from django.contrib.messages import ERROR, get_messages
from django.test import RequestFactory
from django.urls import reverse
from ninja_jwt.tokens import SlidingToken

from analyses.models import Analysis, Assembly, Run, Sample, Study
from genomes.models import GenomeCatalogue
from viruses.models import ViralCatalogue


@pytest.mark.django_db
@pytest.mark.parametrize("account", [None, ""])
def test_missing_owner_shows_error(admin_client, webin_private_study, account):
    """Report missing owners without issuing a token."""
    webin_private_study.webin_submitter = account
    webin_private_study.save()
    with patch("emgapiv2.admin_mixins.create_webin_token") as create:
        response = admin_client.get(
            reverse(
                "admin:analyses_study_impersonate_owner", args=[webin_private_study.pk]
            )
        )
    create.assert_not_called()
    assert response.status_code == 302
    assert response.url == reverse(
        "admin:analyses_study_change", args=[webin_private_study.pk]
    )
    messages = list(get_messages(response.wsgi_request))
    assert len(messages) == 1
    assert messages[0].level == ERROR
    assert str(messages[0]) == "No Webin account is available for this object."
    assert not LogEntry.objects.exists()


@pytest.mark.django_db
@pytest.mark.parametrize(
    "fixture_name,model,path,field",
    [
        ("webin_private_study", Study, "studies", "accession"),
        ("private_analysis_with_download", Analysis, "analyses", "accession"),
        ("private_mgnify_sample", Sample, "samples", "first_accession"),
        ("private_run", Run, "runs", "first_accession"),
        ("mgnify_assemblies", Assembly, "assemblies", "first_accession"),
    ],
)
def test_staff_impersonates_object_owner(
    request,
    client,
    user,
    monkeypatch,
    ninja_api_client,
    fixture_name,
    model,
    path,
    field,
):
    """Permit staff without model permissions and preserve the canonical account."""
    obj = request.getfixturevalue(fixture_name)
    if isinstance(obj, list):
        obj = obj[0]
    obj.webin_submitter = "Webin-12345"
    obj.save()
    user.is_staff = True
    user.save()
    client.force_login(user)
    monkeypatch.setattr(
        settings.EMG_CONFIG.service_urls,
        "web_client_root",
        "https://example.org/metagenomics/",
    )
    url = reverse(
        f"admin:{model._meta.app_label}_{model._meta.model_name}_impersonate_owner",
        args=[obj.pk],
    )
    with patch("emgapiv2.api.auth.httpx.post") as ena:
        response = client.get(url)
    ena.assert_not_called()
    assert response.status_code == 302
    destination = urlsplit(response.url)
    assert destination.path == f"/metagenomics/{path}/{getattr(obj, field)}"
    token_text = parse_qs(destination.fragment)["impersonation_token"][0]
    token = SlidingToken(token_text)
    assert token["username"] == "Webin-12345"
    assert token.get("is_admin") is None
    assert "no-store" in response["Cache-Control"]
    entry = LogEntry.objects.latest("id")
    assert entry.user_id == user.pk
    assert "Webin-12345" in entry.change_message
    assert token_text not in entry.change_message
    if model == Study:
        private_data = ninja_api_client.get(
            "/my-data/studies/", headers={"Authorization": f"Bearer {token_text}"}
        )
        assert private_data.status_code == 200
        assert private_data.json()["items"][0]["accession"] == obj.accession


@pytest.mark.django_db
@pytest.mark.parametrize("staff,active", [(False, True), (True, False)])
def test_non_staff_or_inactive_cannot_impersonate(
    client, user, webin_private_study, staff, active
):
    """Reject direct action requests without an active staff session."""
    user.is_staff, user.is_active = staff, active
    user.save()
    client.force_login(user)
    response = client.get(
        reverse("admin:analyses_study_impersonate_owner", args=[webin_private_study.pk])
    )
    assert response.status_code == 302
    assert "/login/" in response.url
    assert not LogEntry.objects.exists()


@pytest.mark.django_db
def test_draft_catalogues_use_emg_account(
    admin_client, admin_user, genome_catalogues, monkeypatch
):
    """Keep preview actions available for draft catalogues with admin claims."""
    monkeypatch.setattr(settings.EMG_CONFIG.webin, "emg_webin_account", "Webin-460")
    genome = genome_catalogues[0]
    genome.status = GenomeCatalogue.Status.DRAFT
    genome.save()
    viral = ViralCatalogue.objects.create(
        catalogue_id="draft-viruses", name="Draft", version="1", is_public=False
    )
    request = RequestFactory().get("/")
    request.user = admin_user
    for obj, path in [(genome, "genome-catalogues"), (viral, "viruses")]:
        model_admin = admin.site._registry[type(obj)]
        actions = model_admin.get_actions_detail(request, str(obj.pk))
        assert any(action.method == model_admin.impersonate_owner for action in actions)
        response = admin_client.get(
            reverse(
                f"admin:{obj._meta.app_label}_{obj._meta.model_name}_impersonate_owner",
                args=[obj.pk],
            )
        )
        destination = urlsplit(response.url)
        assert destination.path.endswith(f"/{path}/{obj.catalogue_id}")
        token = SlidingToken(parse_qs(destination.fragment)["impersonation_token"][0])
        assert token["username"] == "Webin-460"
        assert token["is_admin"] is True


@pytest.mark.django_db
def test_anonymous_cannot_impersonate(client, webin_private_study):
    """Require an admin session for the action URL."""
    response = client.get(
        reverse("admin:analyses_study_impersonate_owner", args=[webin_private_study.pk])
    )
    assert response.status_code == 302
    assert "/login/" in response.url
    assert not LogEntry.objects.exists()
