import io
from unittest.mock import Mock

import pytest


@pytest.mark.django_db
def test_genome_search_json_success(ninja_api_client, genomes, monkeypatch):
    # Backend returns two hits, only one matches our DB genome accessions
    backend_payload = {
        "results": [
            {"genome": "MGYG000000001", "percent_kmers_found": 12.3},
            {"genome": "MGYG999999999", "percent_kmers_found": 99.9},
        ]
    }

    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = backend_payload
    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post", lambda *a, **k: mock_resp
    )

    resp = ninja_api_client.post(
        "/genome-search/",
        json={
            "sequence": ">seq1\nACGTACGTACGT",
            "kmer_size": 31,
            "max_results": 50,
            "threshold": 0.2,
            "catalogues_filter": ["human-gut-prokaryotes"],
        },
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "data" in body
    data = body["data"]
    assert data["query"] == ">seq1\nACGTACGTACGT"
    assert data["threshold"] == 0.2
    assert "results" in data
    # Only one annotated result should be present (the one matching MGYG000000001)
    assert len(data["results"]) == 1
    item = data["results"][0]
    assert "mgnify" in item and "cobs" in item
    assert item["cobs"]["genome"] == "MGYG000000001"
    assert item["mgnify"]["accession"] == "MGYG000000001"


@pytest.mark.django_db
def test_genome_search_multipart_success(ninja_api_client, genomes, monkeypatch):
    from django.http import QueryDict

    backend_payload = {
        "results": [
            {"genome": "MGYG000000001", "percent_kmers_found": 5.0},
            {"genome": "MGYG000000002", "percent_kmers_found": 10.0},
        ]
    }
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = backend_payload
    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post", lambda *a, **k: mock_resp
    )

    # Build a tiny FASTA file upload
    fasta_content = b">q\nACGTACGT\n"
    files = {"sequence_file": ("query.fa", io.BytesIO(fasta_content), "text/plain")}

    # Use QueryDict to support repeated fields in form data
    qd = QueryDict(mutable=True)
    qd["kmer_size"] = "31"
    qd.setlist("catalogues_filter", ["gut", "marine"])

    resp = ninja_api_client.post("/genome-search/", files=files, data=qd)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "data" in body
    results = body["data"]["results"]
    assert len(results) == 2
    # Sorted by percent_kmers_found descending: first should be MGYG000000002 (10.0)
    assert results[0]["cobs"]["genome"] == "MGYG000000002"
    assert results[1]["cobs"]["genome"] == "MGYG000000001"


@pytest.mark.django_db
def test_genome_search_backend_error(ninja_api_client, genomes, monkeypatch):
    import requests as req

    mock_resp = Mock(status_code=500, text="boom")
    mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError(
        response=mock_resp
    )
    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post", lambda *a, **k: mock_resp
    )

    resp = ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"})
    assert resp.status_code == 502  # Bad Gateway: upstream returned 5xx


@pytest.mark.django_db
def test_genome_search_backend_4xx_error(ninja_api_client, genomes, monkeypatch):
    import requests as req

    mock_resp = Mock(status_code=400, text="bad seq")
    mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError(
        response=mock_resp
    )
    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post", lambda *a, **k: mock_resp
    )

    resp = ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"})
    assert resp.status_code == 400  # Bad Request: client input rejected by backend


@pytest.mark.django_db
def test_genome_search_network_error(ninja_api_client, genomes, monkeypatch):
    import requests as req

    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post",
        Mock(side_effect=req.exceptions.ConnectionError("unreachable")),
    )

    resp = ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"})
    assert resp.status_code == 503  # Service Unavailable: couldn't reach backend


@pytest.mark.django_db
def test_genome_search_forwards_seq_key_to_backend(
    ninja_api_client, genomes, monkeypatch
):
    """The COBS backend expects 'seq', not 'sequence'. Verify the translation happens."""
    import io
    from django.http import QueryDict

    captured = {}

    def fake_post(url, **kwargs):
        captured["json"] = kwargs.get("json")
        captured["data"] = kwargs.get("data")
        mock = Mock(status_code=200)
        mock.json.return_value = {"results": []}
        return mock

    monkeypatch.setattr("emgapiv2.api.genome_search.requests.post", fake_post)

    # JSON path: client sends 'sequence', backend must receive 'seq'
    ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"})
    assert "seq" in (
        captured.get("json") or {}
    ), "JSON path must forward 'seq' not 'sequence'"
    assert "sequence" not in (captured.get("json") or {})

    # Multipart path: client sends 'seq' field (as browsers do), backend must receive 'seq'
    qd = QueryDict(mutable=True)
    qd["seq"] = "ACGT"
    files = {"sequence_file": ("q.fa", io.BytesIO(b">q\nACGT\n"), "text/plain")}
    ninja_api_client.post("/genome-search/", files=files, data=qd)
    assert "seq" in (captured.get("data") or {}), "Multipart path must forward 'seq'"
    assert "sequence" not in (captured.get("data") or {})


@pytest.mark.django_db
def test_genome_search_backend_invalid_json(ninja_api_client, genomes, monkeypatch):
    class BadResp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            raise ValueError("bad json")

    monkeypatch.setattr(
        "emgapiv2.api.genome_search.requests.post", lambda *a, **k: BadResp()
    )

    resp = ninja_api_client.post("/genome-search/", json={"sequence": "ACGT"})
    assert resp.status_code == 502  # Bad Gateway: backend returned unparseable response
