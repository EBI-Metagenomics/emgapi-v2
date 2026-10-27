import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture
def queued_branchwater(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "QUEUES": ["default", "branchwater"],
        }
    }


def _complete_branchwater_job(response):
    from django_tasks.base import TaskResultStatus

    from emgapiv2.tasking import default_task_backend
    from genomes.tasks import run_branchwater_search

    assert response.status_code == 202, response.text
    result = default_task_backend.get_result(response.json()["data"]["job_id"])
    payload = run_branchwater_search.call(
        context=SimpleNamespace(task_result=result), **result.kwargs
    )
    object.__setattr__(result, "status", TaskResultStatus.SUCCESSFUL)
    object.__setattr__(result, "_return_value", payload)


@pytest.mark.parametrize("accession_column", ["SRA accession", "SRA_accession", "acc"])
def test_branchwater_csv_accepts_rust_accession_header(accession_column, settings):
    from emgapiv2.api.branchwater import _parse_branchwater_csv_results

    settings.EMG_CONFIG.branchwater.threshold = 0.1
    settings.EMG_CONFIG.branchwater.ksize = 21
    rows = _parse_branchwater_csv_results(
        f"{accession_column},containment\nSRR123,0.42\nSRR456,0.09\n"
    )

    assert len(rows) == 1
    assert rows[0]["acc"] == "SRR123"
    assert rows[0]["containment"] == 0.42
    assert rows[0]["cANI"] == pytest.approx(0.42 ** (1 / 21))


@pytest.mark.django_db
def test_branchwater_search_returns_enriched_results(
    ninja_api_client,
    raw_read_run,
    queued_branchwater,
    settings,
    tmp_path,
):
    settings.EMG_CONFIG.branchwater.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.branchwater.results_path = str(tmp_path / "results")
    mgnify_run_accession = raw_read_run[0].first_accession

    with (
        patch(
            "genomes.branchwater._search_branchwater",
            return_value=[
                {"acc": mgnify_run_accession, "containment": 0.42, "cANI": 0.96},
                {"acc": "SRR9999999", "containment": 0.31, "cANI": 0.95},
            ],
        ),
        patch(
            "genomes.branchwater._join_branchwater_metadata",
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
            json={
                "signatures": '{"signatures":[{"molecule":"dna","ksize":21,"seed":42,"max_hash":18446744073709552,"mins":[9007610945697915]}]}'
            },
        )
        _complete_branchwater_job(response)

    assert response.status_code == 202, response.text
    job_id = response.json()["data"]["job_id"]
    response = ninja_api_client.get(f"/services/branchwater/results/{job_id}/")
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
    queued_branchwater,
    settings,
    tmp_path,
):
    settings.EMG_CONFIG.branchwater.signatures_root = str(tmp_path / "signatures")
    settings.EMG_CONFIG.branchwater.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.branchwater.results_path = str(tmp_path / "results")
    catalogue = "human-gut-v2-0"
    accession = "MGYG000304657"
    signature_dir = tmp_path / "signatures" / catalogue
    signature_dir.mkdir(parents=True, exist_ok=True)
    signature_path = signature_dir / f"{accession}.fna.sig"
    signature = {
        "signatures": [
            {
                "molecule": "dna",
                "ksize": 21,
                "seed": 42,
                "max_hash": 18446744073709552,
                "mins": [9007610945697915],
            }
        ]
    }
    signature_path.write_text(json.dumps([signature]), encoding="utf-8")

    with (
        patch(
            "genomes.branchwater._search_branchwater",
            return_value=[
                {"acc": "SRR6180434", "containment": 0.55, "cANI": 0.97},
            ],
        ) as search_mock,
        patch(
            "genomes.branchwater._join_branchwater_metadata",
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
        _complete_branchwater_job(response)

    assert response.status_code == 202, response.text
    job_id = response.json()["data"]["job_id"]
    response = ninja_api_client.get(f"/services/branchwater/results/{job_id}/")
    assert response.status_code == 200, response.text
    assert response.json()[0]["acc"] == "SRR6180434"
    assert search_mock.call_args.kwargs["use_precomputed_sketches"] is True
    assert search_mock.call_args.args[0] == [signature]


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
