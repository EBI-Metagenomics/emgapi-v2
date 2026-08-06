import json
from unittest.mock import patch

import pytest


@pytest.mark.django_db
def test_branchwater_search_returns_enriched_results(
    ninja_api_client,
    raw_read_run,
):
    mgnify_run_accession = raw_read_run[0].first_accession

    with (
        patch(
            "emgapiv2.api.branchwater._search_branchwater",
            return_value=[
                {"acc": mgnify_run_accession, "containment": 0.42, "cANI": 0.96},
                {"acc": "SRR9999999", "containment": 0.31, "cANI": 0.95},
            ],
        ),
        patch(
            "emgapiv2.api.branchwater._join_branchwater_metadata",
            return_value=[
                {
                    "acc": mgnify_run_accession,
                    "containment": 0.42,
                    "cANI": 0.96,
                    "bioproject": "PRJEB12345",
                    "assay_type": "METAGENOMIC",
                    "collection_date_sam": "2024-01-01",
                    "geo_loc_name_country_calc": "United Kingdom",
                    "organism": "metagenome",
                    "lat_lon": "51.50 -0.12",
                },
                {
                    "acc": "SRR9999999",
                    "containment": 0.31,
                    "cANI": 0.95,
                    "bioproject": "PRJNA999999",
                    "assay_type": "METAGENOMIC",
                    "collection_date_sam": "2023-01-01",
                    "geo_loc_name_country_calc": "NP",
                    "organism": "metagenome",
                    "lat_lon": "NP",
                },
            ],
        ),
    ):
        response = ninja_api_client.post(
            "/services/branchwater/",
            json={"signatures": '{"molecule":"dna"}'},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, list)
    assert body[0]["acc"] == mgnify_run_accession
    assert body[0]["exists_on_mgnify"] is True
    assert body[0]["is_private"] is False
    assert body[1]["acc"] == "SRR9999999"
    assert body[1]["exists_on_mgnify"] is False
    assert body[1]["is_private"] is None


@pytest.mark.django_db
def test_branchwater_mags_search_reads_signature_and_returns_results(
    ninja_api_client,
    settings,
    tmp_path,
):
    settings.EMG_CONFIG.branchwater.signatures_root = str(tmp_path / "signatures")
    catalogue = "human-gut-v2-0"
    accession = "MGYG000304657"
    signature_dir = tmp_path / "signatures" / catalogue
    signature_dir.mkdir(parents=True, exist_ok=True)
    signature_path = signature_dir / f"{accession}.fna.sig"
    signature_path.write_text(json.dumps({"molecule": "dna"}), encoding="utf-8")

    with (
        patch(
            "emgapiv2.api.branchwater._search_branchwater",
            return_value=[
                {"acc": "SRR6180434", "containment": 0.55, "cANI": 0.97},
            ],
        ) as search_mock,
        patch(
            "emgapiv2.api.branchwater._join_branchwater_metadata",
            return_value=[
                {
                    "acc": "SRR6180434",
                    "containment": 0.55,
                    "cANI": 0.97,
                    "bioproject": "PRJEB00001",
                    "assay_type": "METAGENOMIC",
                    "collection_date_sam": "2024-04-04",
                    "geo_loc_name_country_calc": "NP",
                    "organism": "metagenome",
                    "lat_lon": "NP",
                }
            ],
        ),
    ):
        response = ninja_api_client.post(
            f"/services/branchwater/mags?accession={accession}&catalogue={catalogue}"
        )

    assert response.status_code == 200, response.text
    assert response.json()[0]["acc"] == "SRR6180434"
    assert search_mock.call_args.kwargs["use_precomputed_sketches"] is True
    assert search_mock.call_args.args[0] == {"molecule": "dna"}


def test_branchwater_mags_search_returns_404_when_signature_missing(
    ninja_api_client,
    settings,
    tmp_path,
):
    settings.EMG_CONFIG.branchwater.signatures_root = str(tmp_path / "signatures")

    response = ninja_api_client.post(
        "/services/branchwater/mags?accession=MGYG000000000&catalogue=human-gut-v2-0"
    )

    assert response.status_code == 404
    assert "No .sig file found" in response.json()["detail"]


def test_branchwater_health_endpoint(ninja_api_client):
    with patch("emgapiv2.api.branchwater._check_branchwater_health"):
        response = ninja_api_client.get("/services/branchwater/health")

    assert response.status_code == 200, response.text
    assert response.json() == {"status": "ok"}


def test_branchwater_health_endpoint_returns_503_on_backend_failure(ninja_api_client):
    from emgapiv2.api.branchwater import BranchwaterError

    with patch(
        "emgapiv2.api.branchwater._check_branchwater_health",
        side_effect=BranchwaterError(
            503, "Branchwater healthcheck is temporarily unavailable."
        ),
    ):
        response = ninja_api_client.get("/services/branchwater/health")

    assert response.status_code == 503
    assert "temporarily unavailable" in response.json()["detail"]
