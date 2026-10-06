from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory
from django_tasks.exceptions import TaskResultDoesNotExist

from emgapiv2.api.genome_search import _parse_request
from genomes.models import CatalogueGenome, GenomeSearchIndex
from genomes.tasks import run_lexicmap_search

SEQUENCE = "ACGT" * 25
pytestmark = pytest.mark.django_db


@pytest.fixture
def search_index(genomes):
    return GenomeSearchIndex.objects.create(
        catalogue=CatalogueGenome.public_objects.first().catalogue,
        backend="lexicmap",
        status="ACTIVE",
        is_active=True,
        artifact_path="catalogue/index.lmi",
    )


def test_submit_and_poll(ninja_api_client, search_index, monkeypatch):
    hit = dict(
        genome=search_index.catalogue.genomes.first().accession,
        catalogue=search_index.catalogue_id,
        query_coverage=100,
        identity=100,
        bitscore=200,
        evalue=1e-20,
        sequence_id="contig",
        query_start=1,
        query_end=100,
        subject_start=10,
        subject_end=109,
        strand="+",
    )
    task = Mock()
    job = SimpleNamespace(id=str(uuid4()), status="READY")
    task.enqueue.return_value = task.get_result.return_value = job
    monkeypatch.setattr("emgapiv2.api.genome_search.run_lexicmap_search", task)
    response = ninja_api_client.post("/genome-search/", json={"sequence": SEQUENCE})
    assert response.status_code == 202
    assert response.json()["data"]["job_id"] == job.id
    payload = task.enqueue.call_args.kwargs["request_payload"]
    assert payload["indexes"] == [
        {"catalogue": search_index.catalogue_id, "path": search_index.artifact_path}
    ]
    status_path = f"/genome-search/status/{job.id}/"
    assert ninja_api_client.get(status_path).json()["data"]["status"] == "READY"

    with patch(
        "genomes.lexicmap.search",
        return_value=[hit, {**hit, "catalogue": "wrong-release"}],
    ):
        job.return_value = run_lexicmap_search.call(payload)
    job.status = "SUCCESSFUL"
    results = ninja_api_client.get(status_path).json()["data"]["results"]
    assert len(results) == 1
    assert results[0]["mgnify"]["accession"] == hit["genome"]

    search_index.catalogue.status = "retired"
    search_index.catalogue.save()
    assert ninja_api_client.get(status_path).json()["data"]["results"] == []
    ninja_api_client.post("/genome-search/", json={"sequence": SEQUENCE})
    assert task.enqueue.call_args.kwargs["request_payload"]["indexes"] == []


def test_failed_and_missing_job(ninja_api_client, monkeypatch):
    task = Mock()
    job_id = str(uuid4())
    task.get_result.return_value = SimpleNamespace(id=job_id, status="FAILED")
    monkeypatch.setattr("emgapiv2.api.genome_search.run_lexicmap_search", task)
    response = ninja_api_client.get(f"/genome-search/status/{job_id}/")
    assert response.json()["data"]["status"] == "FAILED"
    assert response.json()["data"]["results"] is None
    task.get_result.side_effect = TaskResultDoesNotExist
    assert ninja_api_client.get(f"/genome-search/status/{job_id}/").status_code == 404


def test_multipart():
    query = _parse_request(
        RequestFactory().post(
            "/genome-search/",
            {
                "sequence_file": SimpleUploadedFile(
                    "query.fa", f">q\n{SEQUENCE}".encode()
                ),
                "catalogues_filter": ["gut"],
                "min_identity": "80",
            },
        )
    )
    assert query.sequence == SEQUENCE
    assert query.catalogues_filter == ["gut"]
    assert query.min_identity == 80


def test_invalid_sequence(ninja_api_client):
    assert (
        ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"}).status_code
        == 400
    )
