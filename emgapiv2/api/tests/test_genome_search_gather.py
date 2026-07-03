from pathlib import Path
from unittest.mock import Mock

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import QueryDict
from kombu.exceptions import OperationalError

from analyses.models import Biome
from genomes.models import GenomeCatalogue

MOCKED_JOB_ID = "MOCKED_JOB_ID"


def _make_catalogue() -> GenomeCatalogue:
    biome, _ = Biome.objects.get_or_create(
        biome_name="Root",
        defaults={"path": "root"},
    )
    catalogue, _ = GenomeCatalogue.objects.get_or_create(
        catalogue_id="human-gut-v2-0",
        defaults={
            "version": "2.0",
            "name": "Human Gut v2.0",
            "catalogue_biome_label": "Human Gut",
            "catalogue_type": GenomeCatalogue.PROK,
            "biome": biome,
        },
    )
    return catalogue


class MockTaskResult:
    id = MOCKED_JOB_ID
    status = "SUCCESS"
    result = {
        "overlap": "3.2 Mbp",
        "p_query": "100.0%",
        "p_match": "100.0%",
        "match": "MGYG000000001",
        "catalog": "human-gut-v2-0",
        "query_filename": "query.sig",
        "md5_name": "query.sig",
        "matches": 1,
    }
    args = ["query.sig", "query.sig", "human-gut-v2-0"]


class MockGroupResult:
    id = MOCKED_JOB_ID
    results = [MockTaskResult()]


class MockSubmittedGroupResult:
    id = MOCKED_JOB_ID
    results = [Mock(id="child-1")]

    def save(self):
        pass


class MockInspect:
    def ping(self):
        return {"worker": "pong"}

    def query_task(self, _task_id):
        return {}

    def reserved(self):
        return {}


@pytest.mark.django_db
def test_genome_search_gather_submit_success(
    ninja_api_client, settings, monkeypatch, tmp_path
):
    _make_catalogue()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")

    monkeypatch.setattr(
        "emgapiv2.api.genome_search_gather._send_sourmash_jobs",
        lambda uploads, catalogues: (
            MOCKED_JOB_ID,
            {
                f"{upload['original_name']}:{catalogue}": "child-1"
                for upload in uploads
                for catalogue in catalogues
            },
        ),
    )

    qd = QueryDict(mutable=True)
    qd.setlist("mag_catalogues", ["human-gut-v2-0"])

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=qd,
    )

    assert response.status_code == 200, response.text
    body = response.json()["data"]
    assert body["job_id"] == MOCKED_JOB_ID
    assert body["signatures_received"] == ["query.sig"]
    assert body["status_url"].endswith(f"/genomes-search/status/{MOCKED_JOB_ID}/")
    saved_files = list((tmp_path / "queries").glob("*.sig"))
    assert len(saved_files) == 1


@pytest.mark.django_db
def test_genome_search_gather_submit_invalid_signature(ninja_api_client):
    _make_catalogue()

    qd = QueryDict(mutable=True)
    qd.setlist("mag_catalogues", ["human-gut-v2-0"])

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "bad.sig",
                b'{"type": "not a sourmash signature"}',
                content_type="application/json",
            )
        },
        data=qd,
    )

    assert response.status_code == 400
    assert "Unable to parse the uploaded file" in response.json()["detail"]


@pytest.mark.django_db
def test_genome_search_gather_submit_queue_unavailable(ninja_api_client, monkeypatch):
    _make_catalogue()

    def _raise_operational_error(*_args, **_kwargs):
        raise OperationalError("redis down")

    monkeypatch.setattr(
        "emgapiv2.api.genome_search_gather._send_sourmash_jobs",
        _raise_operational_error,
    )

    qd = QueryDict(mutable=True)
    qd.setlist("mag_catalogues", ["human-gut-v2-0"])

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=qd,
    )

    assert response.status_code == 503
    assert "Sourmash queue backend is unavailable" in response.json()["detail"]


@pytest.mark.django_db
def test_genome_search_gather_status(ninja_api_client, monkeypatch):
    monkeypatch.setattr(
        "emgapiv2.api.genome_search_gather.app.GroupResult.restore",
        lambda *args, **kwargs: MockGroupResult(),
    )
    monkeypatch.setattr(
        "emgapiv2.api.genome_search_gather.control.inspect",
        lambda: MockInspect(),
    )

    response = ninja_api_client.get(f"/genomes-search/status/{MOCKED_JOB_ID}/")

    assert response.status_code == 200, response.text
    body = response.json()["data"]
    assert body["group_id"] == MOCKED_JOB_ID
    assert body["worker_status"] == "OK"
    assert body["signatures"][0]["status"] == "SUCCESS"
    assert body["signatures"][0]["catalogue"] == "human-gut-v2-0"


@pytest.mark.django_db
def test_genome_search_gather_results_csv(ninja_api_client, settings, tmp_path):
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path)
    csv_path = Path(settings.EMG_CONFIG.sourmash.results_path) / f"{MOCKED_JOB_ID}.csv"
    csv_path.write_text("intersect_bp\n3158000\n")

    response = ninja_api_client.get(f"/genomes-search/results/{MOCKED_JOB_ID}/")

    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"].startswith("text/csv")
    assert "intersect_bp" in response.content.decode("utf-8")
