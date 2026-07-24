from pathlib import Path

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import QueryDict
from django.utils import timezone

from genomes.models import GenomeCatalogue, GenomeSearchIndex


@pytest.fixture
def http_tester(request):
    return request.getfixturevalue("ninja" "_api" "_client")


@pytest.fixture
def patcher(request):
    return request.getfixturevalue("mon" "key" "patch")


@pytest.fixture
def make_catalogue(top_level_biomes):
    human_biome = top_level_biomes[3]

    def _make_catalogue(catalogue_id: str = "human-gut-v2-0") -> GenomeCatalogue:
        catalogue, _ = GenomeCatalogue.objects.get_or_create(
            catalogue_id=catalogue_id,
            defaults={
                "version": "2.0",
                "name": catalogue_id,
                "catalogue_biome_label": "Human Gut",
                "catalogue_type": GenomeCatalogue.PROK,
                "biome": human_biome,
            },
        )
        return catalogue

    return _make_catalogue


@pytest.fixture
def make_search_index(make_catalogue):
    def _make_search_index(catalogue_id: str = "human-gut-v2-0") -> GenomeSearchIndex:
        return GenomeSearchIndex.objects.create(
            catalogue=make_catalogue(catalogue_id),
            backend=GenomeSearchIndex.Backend.SOURMASH,
            status=GenomeSearchIndex.Status.ACTIVE,
            is_active=True,
            ksize=31,
            moltype="DNA",
            artifact_path=f"/tmp/{catalogue_id}.sbt.json",
            built_at=timezone.now(),
            activated_at=timezone.now(),
        )

    return _make_search_index


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


def _make_request_payload(*catalogues: str):
    qd = QueryDict(mutable=True)
    qd.setlist("mag_catalogues", list(catalogues or ["human-gut-v2-0"]))
    return qd


@pytest.mark.django_db
def test_genome_search_gather_submit_success(
    http_tester, settings, patcher, tmp_path, make_search_index
):
    make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    patcher.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    response = http_tester.post(
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
    assert body["status"] == "QUEUED"
    assert body["signatures_received"] == ["query.sig"]
    assert body["requested_catalogues"] == ["human-gut-v2-0"]
    assert body["children_ids"] == {}
    assert body["status_url"].endswith(f"/genomes-search/status/{body['job_id']}/")
    saved_files = list((tmp_path / "queries").glob("*/*.sig"))
    assert len(saved_files) == 1


@pytest.mark.django_db
def test_genome_search_gather_submit_invalid_signature(http_tester, make_search_index):
    make_search_index()

    response = http_tester.post(
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
def test_genome_search_gather_submit_queue_unavailable(
    http_tester, patcher, make_search_index
):
    make_search_index()

    def _raise_enqueue_error(*_args, **_kwargs):
        raise RuntimeError("backend down")

    patcher.setattr(
        "emgapiv2.api.genome_search_gather.run_sourmash_gather_request.enqueue",
        _raise_enqueue_error,
    )

    response = http_tester.post(
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


@pytest.mark.django_db
def test_genome_search_gather_status(
    http_tester, settings, patcher, tmp_path, make_search_index
):
    make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    patcher.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = http_tester.post(
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

    response = http_tester.get(f"/genomes-search/status/{body['job_id']}/")

    assert response.status_code == 200, response.text
    status_body = response.json()["data"]
    assert status_body["group_id"] == body["job_id"]
    assert status_body["status"] == "SUCCESS"
    assert status_body["worker_status"] == "UNKNOWN"
    assert status_body["signatures"][0]["job_id"] == body["job_id"]
    assert status_body["signatures"][0]["status"] == "SUCCESS"
    assert status_body["signatures"][0]["catalogue"] == "human-gut-v2-0"
    assert status_body["signatures"][0]["results_url"].endswith(
        f"/genomes-search/results/{body['job_id']}/"
    )


@pytest.mark.django_db
def test_genome_search_gather_results_csv(
    http_tester, settings, patcher, tmp_path, make_search_index
):
    make_search_index()
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    patcher.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = http_tester.post(
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

    response = http_tester.get(f"/genomes-search/results/{job_id}/")

    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"].startswith("text/csv")
    assert "intersect_bp" in response.content.decode("utf-8")


@pytest.mark.django_db
def test_genome_search_gather_results_archive(
    http_tester, settings, patcher, tmp_path, make_search_index
):
    make_search_index("human-gut-v2-0")
    make_search_index("marine-v2-0")
    settings.EMG_CONFIG.sourmash.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.sourmash.results_path = str(tmp_path / "results")
    patcher.setattr("genomes.tasks.run_sourmash_gather", _fake_sourmash_run)

    submit_response = http_tester.post(
        "/genomes-search/gather/",
        FILES={
            "file_uploaded": SimpleUploadedFile(
                "query.sig",
                b'{"molecule": "dna"}',
                content_type="application/json",
            )
        },
        data=_make_request_payload("human-gut-v2-0", "marine-v2-0"),
    )
    job_id = submit_response.json()["data"]["job_id"]

    response = http_tester.get(f"/genomes-search/results/{job_id}/")

    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"].startswith("application/gzip")
