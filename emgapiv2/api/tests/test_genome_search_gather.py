from pathlib import Path

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import QueryDict
from django.utils import timezone

from analyses.models import Biome
from genomes.models import (
    GenomeCatalogue,
    GenomeSearchIndex,
    SourmashSearchJob,
    SourmashSearchJobItem,
)


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


def _make_search_index() -> GenomeSearchIndex:
    return GenomeSearchIndex.objects.create(
        catalogue=_make_catalogue(),
        backend=GenomeSearchIndex.Backend.SOURMASH,
        status=GenomeSearchIndex.Status.ACTIVE,
        is_active=True,
        ksize=31,
        moltype="DNA",
        artifact_path="/tmp/fake-genomes-index.sbt.json",
        built_at=timezone.now(),
        activated_at=timezone.now(),
    )


def _fake_sourmash_run(**kwargs):
    result_path = Path(kwargs["result_path"])
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text("intersect_bp\n3158000\n", encoding="utf-8")
    return {
        "overlap": "3.2 Mbp",
        "p_query": "100.0%",
        "p_match": "100.0%",
        "match": "MGYG000000001",
        "catalog": kwargs["catalogue_id"],
        "query_filename": kwargs["original_filename"],
        "md5_name": Path(kwargs["query_path"]).name,
        "matches": 1,
    }


def _make_request_payload():
    qd = QueryDict(mutable=True)
    qd.setlist("mag_catalogues", ["human-gut-v2-0"])
    return qd


@pytest.mark.django_db
def test_genome_search_gather_submit_success(
    ninja_api_client, settings, monkeypatch, tmp_path
):
    _make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    monkeypatch.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )

    assert response.status_code == 200, response.text
    body = response.json()["data"]
    assert body["status"] == SourmashSearchJob.Status.QUEUED
    assert body["signatures_received"] == ["query.sig"]
    assert body["requested_catalogues"] == ["human-gut-v2-0"]
    assert body["status_url"].endswith(f"/genomes-search/status/{body['job_id']}/")
    assert list(body["children_ids"].keys()) == ["query.sig:human-gut-v2-0"]

    job = SourmashSearchJob.objects.get(id=body["job_id"])
    item = job.items.get()
    assert job.status == SourmashSearchJob.Status.SUCCESS
    assert item.status == SourmashSearchJobItem.Status.SUCCESS
    saved_files = list((tmp_path / "queries" / body["job_id"]).glob("*.sig"))
    assert len(saved_files) == 1


@pytest.mark.django_db
def test_genome_search_gather_submit_invalid_signature(ninja_api_client):
    _make_search_index()

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "bad.sig",
                b'{"type": "not a sourmash signature"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )

    assert response.status_code == 400
    assert "Unable to parse the uploaded file" in response.json()["detail"]


@pytest.mark.django_db
def test_genome_search_gather_submit_queue_unavailable(ninja_api_client, monkeypatch):
    _make_search_index()

    def _raise_enqueue_error(*_args, **_kwargs):
        raise RuntimeError("backend down")

    monkeypatch.setattr(
        "emgapiv2.api.genome_search_gather.run_sourmash_gather_item.enqueue",
        _raise_enqueue_error,
    )

    response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )

    assert response.status_code == 503
    assert "Sourmash task backend is unavailable" in response.json()["detail"]
    assert SourmashSearchJob.objects.count() == 1
    assert SourmashSearchJob.objects.first().status == SourmashSearchJob.Status.FAILED


@pytest.mark.django_db
def test_genome_search_gather_status(ninja_api_client, settings, monkeypatch, tmp_path):
    _make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    monkeypatch.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )
    body = submit_response.json()["data"]
    child_id = next(iter(body["children_ids"].values()))

    response = ninja_api_client.get(f"/genomes-search/status/{body['job_id']}/")

    assert response.status_code == 200, response.text
    status_body = response.json()["data"]
    assert status_body["group_id"] == body["job_id"]
    assert status_body["status"] == SourmashSearchJob.Status.SUCCESS
    assert status_body["worker_status"] == "UNKNOWN"
    assert status_body["signatures"][0]["job_id"] == child_id
    assert (
        status_body["signatures"][0]["status"] == SourmashSearchJobItem.Status.SUCCESS
    )
    assert status_body["signatures"][0]["catalogue"] == "human-gut-v2-0"
    assert status_body["signatures"][0]["results_url"].endswith(
        f"/genomes-search/results/{child_id}/"
    )


@pytest.mark.django_db
def test_genome_search_gather_results_csv(
    ninja_api_client, settings, monkeypatch, tmp_path
):
    _make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    monkeypatch.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )
    child_id = next(iter(submit_response.json()["data"]["children_ids"].values()))

    response = ninja_api_client.get(f"/genomes-search/results/{child_id}/")

    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"].startswith("text/csv")
    assert "intersect_bp" in response.content.decode("utf-8")


@pytest.mark.django_db
def test_genome_search_gather_results_archive(
    ninja_api_client, settings, monkeypatch, tmp_path
):
    _make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    monkeypatch.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = ninja_api_client.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload(),
    )
    job_id = submit_response.json()["data"]["job_id"]

    response = ninja_api_client.get(f"/genomes-search/results/{job_id}/")

    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"].startswith("application/gzip")
