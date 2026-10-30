import gzip
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from django.test import RequestFactory
from django_tasks.base import TaskResultStatus

from emgapiv2.tasking import default_task_backend
from genomes.tasks import run_branchwater_search, run_sourmash_gather_request


@pytest.fixture(autouse=True)
def branchwater_jobs(settings, tmp_path):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "QUEUES": ["default", "branchwater"],
        }
    }
    settings.EMG_CONFIG.branchwater.queries_path = str(tmp_path / "queries")
    settings.EMG_CONFIG.branchwater.results_path = str(tmp_path / "results")
    settings.EMG_CONFIG.branchwater.threshold = 0.1
    settings.EMG_CONFIG.branchwater.ksize = 21
    settings.EMG_CONFIG.branchwater.search_timeout_seconds = 300


@pytest.fixture
def signature():
    return {
        "class": "sourmash_signature",
        "name": "query with spaces",
        "signatures": [
            {
                "molecule": "DNA",
                "ksize": 21,
                "seed": 42,
                "max_hash": 18446744073709552,
                "mins": [9007610945697915, 18445480136133269],
            }
        ],
    }


def submit(client, signature):
    response = client.post(
        "/services/branchwater/", json={"signatures": json.dumps(signature)}
    )
    assert response.status_code == 202, response.text
    return response.json()["data"]


def finish(task_result, payload, status=TaskResultStatus.SUCCESSFUL):
    object.__setattr__(task_result, "status", status)
    object.__setattr__(task_result, "_return_value", payload)


class ReturnValueNotReady:
    status = TaskResultStatus.SUCCESSFUL

    @property
    def return_value(self):
        raise ValueError("result not ready yet")


def test_submission_is_nonblocking_and_preserves_large_integers(
    ninja_api_client, signature
):
    with patch(
        "genomes.branchwater.client.httpx.post",
        side_effect=AssertionError("must run in worker"),
    ) as network:
        data = submit(ninja_api_client, signature)
    network.assert_not_called()
    assert data["status"] == "QUEUED"
    assert data["status_url"].endswith(
        f"/services/branchwater/status/{data['job_id']}/"
    )
    assert data["results_url"].endswith(
        f"/services/branchwater/results/{data['job_id']}/"
    )
    result = default_task_backend.get_result(data["job_id"])
    assert result.task.queue_name == "branchwater"
    assert set(result.kwargs) == {"query_path", "result_path"}
    assert json.loads(Path(result.kwargs["query_path"]).read_text()) == [signature]
    assert not Path(result.kwargs["result_path"]).exists()
    status = ninja_api_client.get(f"/services/branchwater/status/{data['job_id']}/")
    assert status.json()["data"] == {
        "job_id": data["job_id"],
        "status": "QUEUED",
        "results_url": None,
        "reason": None,
    }
    assert (
        ninja_api_client.get(
            f"/services/branchwater/results/{data['job_id']}/"
        ).status_code
        == 409
    )


@pytest.mark.django_db
def test_database_backend_submission_does_not_execute_search(
    ninja_api_client, signature, settings
):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_db.DatabaseBackend",
            "QUEUES": ["default", "branchwater"],
        }
    }
    with patch(
        "genomes.branchwater.client.httpx.post",
        side_effect=AssertionError("must run in worker"),
    ) as network:
        data = submit(ninja_api_client, signature)
    network.assert_not_called()
    assert data["job_id"].count("-") == 4
    result = default_task_backend.get_result(data["job_id"])
    assert result.status == TaskResultStatus.READY
    assert result.task.queue_name == "branchwater"
    assert Path(result.kwargs["query_path"]).is_file()
    status = ninja_api_client.get(f"/services/branchwater/status/{data['job_id']}/")
    assert status.status_code == 200, status.text
    assert status.json()["data"]["status"] == "QUEUED"
    assert (
        ninja_api_client.get(
            f"/services/branchwater/results/{data['job_id']}/"
        ).status_code
        == 409
    )


@pytest.mark.django_db
def test_worker_processes_search_metadata_and_run_flags(
    ninja_api_client, signature, settings, tmp_path, raw_read_run
):
    import duckdb

    accession = raw_read_run[0].first_accession
    metadata = tmp_path / "metadata.duckdb"
    with duckdb.connect(str(metadata)) as connection:
        connection.execute(
            "CREATE TABLE metadata (acc VARCHAR, bioproject VARCHAR, assay_type VARCHAR, collection_date_sam VARCHAR, geo_loc_name_country_calc VARCHAR, organism VARCHAR, lat_lon VARCHAR)"
        )
        connection.execute(
            "INSERT INTO metadata VALUES (?, 'PRJEB12345', 'METAGENOMIC', '2024-01-01', 'United Kingdom', 'metagenome', '51.50 -0.12')",
            [accession],
        )
    settings.EMG_CONFIG.branchwater.metadata_duckdb = str(metadata)
    data = submit(ninja_api_client, signature)
    result = default_task_backend.get_result(data["job_id"])
    finish(result, None, TaskResultStatus.RUNNING)
    assert (
        ninja_api_client.get(f"/services/branchwater/status/{data['job_id']}/").json()[
            "data"
        ]["status"]
        == "RUNNING"
    )
    request = httpx.Request("POST", "http://index-service/search")
    upstream = httpx.Response(
        200,
        request=request,
        text=f"SRA accession,containment\n{accession},0.42\nSRR9999999,0.31\nSRRLOW,0.09\n",
    )
    with patch(
        "genomes.branchwater.client.httpx.post", return_value=upstream
    ) as network:
        payload = run_branchwater_search.call(
            context=SimpleNamespace(task_result=result), **result.kwargs
        )
    assert json.loads(gzip.decompress(network.call_args.kwargs["content"])) == [
        signature
    ]
    assert network.call_args.kwargs["timeout"] == 300
    assert payload["result_count"] == 2
    assert len(json.dumps(payload)) < 1000
    assert not list(Path(result.kwargs["result_path"]).parent.glob(".pending-*"))
    finish(result, payload)
    status = ninja_api_client.get(
        f"/services/branchwater/status/{data['job_id']}/"
    ).json()["data"]
    assert status["status"] == "SUCCESS"
    assert status["results_url"] == data["results_url"]
    rows_response = ninja_api_client.get(
        f"/services/branchwater/results/{data['job_id']}/"
    )
    assert rows_response.status_code == 200, rows_response.text
    rows = rows_response.json()
    assert rows[0]["acc"] == accession
    assert rows[0]["bioproject"] == "PRJEB12345"
    assert rows[0]["exists_on_mgnify"] is True
    assert rows[0]["is_private"] is False
    assert rows[1]["acc"] == "SRR9999999"
    assert rows[1]["bioproject"] == "NP"
    assert rows[1]["exists_on_mgnify"] is False
    assert rows[1]["is_private"] is None


def test_no_results_job(ninja_api_client, signature):
    data = submit(ninja_api_client, signature)
    result = default_task_backend.get_result(data["job_id"])
    with patch("genomes.branchwater.service._search_branchwater", return_value=[]):
        payload = run_branchwater_search.call(
            context=SimpleNamespace(task_result=result), **result.kwargs
        )
    finish(result, payload)
    assert (
        ninja_api_client.get(f"/services/branchwater/status/{data['job_id']}/").json()[
            "data"
        ]["status"]
        == "NO_RESULTS"
    )
    assert (
        ninja_api_client.get(f"/services/branchwater/results/{data['job_id']}/").json()
        == []
    )


def test_successful_task_without_readable_return_value_is_still_running():
    from emgapiv2.api.branchwater import _branchwater_task_state

    assert _branchwater_task_state(ReturnValueNotReady()) == ("RUNNING", {})


@pytest.mark.parametrize("backend_failure", [False, True])
def test_failed_job_has_safe_reason(ninja_api_client, signature, backend_failure):
    data = submit(ninja_api_client, signature)
    result = default_task_backend.get_result(data["job_id"])
    if backend_failure:
        finish(result, None, TaskResultStatus.FAILED)
        object.__setattr__(
            result,
            "errors",
            [SimpleNamespace(traceback="SECRET traceback and database URL")],
        )
    else:
        with patch(
            "genomes.branchwater.service._search_branchwater",
            side_effect=RuntimeError("SECRET traceback and database URL"),
        ):
            payload = run_branchwater_search.call(
                context=SimpleNamespace(task_result=result), **result.kwargs
            )
        finish(result, payload)
    response = ninja_api_client.get(f"/services/branchwater/status/{data['job_id']}/")
    assert response.json()["data"]["status"] == "FAILED"
    assert "SECRET" not in response.text
    assert response.json()["data"]["results_url"] is None
    assert (
        ninja_api_client.get(
            f"/services/branchwater/results/{data['job_id']}/"
        ).status_code
        == 409
    )


def test_backend_failure_is_503(ninja_api_client, signature):
    with patch(
        "emgapiv2.api.branchwater.default_task_backend.get_result",
        side_effect=RuntimeError("SECRET"),
    ):
        response = ninja_api_client.get(f"/services/branchwater/status/{'A' * 32}/")
    assert response.status_code == 503
    assert "SECRET" not in response.text
    with patch(
        "emgapiv2.api.branchwater.run_branchwater_search",
        new=SimpleNamespace(
            enqueue=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("SECRET"))
        ),
    ):
        response = ninja_api_client.post(
            "/services/branchwater/", json={"signatures": json.dumps(signature)}
        )
    assert response.status_code == 503
    assert "SECRET" not in response.text


@pytest.mark.parametrize("job_id", ["malformed-id", "A" * 32])
def test_unknown_job_is_404(ninja_api_client, job_id):
    for route in ("status", "results"):
        assert (
            ninja_api_client.get(f"/services/branchwater/{route}/{job_id}/").status_code
            == 404
        )


def test_other_task_and_queue_are_not_exposed(ninja_api_client, signature):
    other = run_sourmash_gather_request.enqueue(request_payload={})
    wrong_queue = run_branchwater_search.using(queue_name="default").enqueue(
        query_path="unused", result_path="unused"
    )
    for result in [other, wrong_queue]:
        for route in ("status", "results"):
            assert (
                ninja_api_client.get(
                    f"/services/branchwater/{route}/{result.id}/"
                ).status_code
                == 404
            )


def test_result_paths_must_stay_in_configured_directory(
    ninja_api_client, signature, tmp_path
):
    data = submit(ninja_api_client, signature)
    result = default_task_backend.get_result(data["job_id"])
    outside = tmp_path / "private.json"
    outside.write_text('[{"secret":"value"}]')
    finish(result, {"status": "SUCCESS", "result_path": str(outside)})
    response = ninja_api_client.get(f"/services/branchwater/results/{data['job_id']}/")
    assert response.status_code == 503
    assert "secret" not in response.text


@pytest.mark.parametrize(
    "payload", ["not json", "{}", "[]", '{"signatures":[{"molecule":"protein"}]}']
)
def test_invalid_signature_rejected_before_enqueue(ninja_api_client, payload):
    response = ninja_api_client.post(
        "/services/branchwater/", json={"signatures": payload}
    )
    assert response.status_code == 400
    assert not default_task_backend.results


def test_branchwater_mags_stages_signature(
    ninja_api_client, settings, tmp_path, signature
):
    settings.EMG_CONFIG.branchwater.signatures_root = str(tmp_path / "signatures")
    catalogue = "human-gut-v2-0"
    accession = "MGYG000304657"
    directory = Path(settings.EMG_CONFIG.branchwater.signatures_root) / catalogue
    directory.mkdir(parents=True)
    (directory / f"{accession}.fna.sig").write_text(json.dumps([signature]))
    with patch("genomes.branchwater.client.httpx.post") as network:
        response = ninja_api_client.post(
            f"/services/branchwater/mags?accession={accession}&catalogue={catalogue}"
        )
    assert response.status_code == 202, response.text
    network.assert_not_called()
    result = default_task_backend.get_result(response.json()["data"]["job_id"])
    assert json.loads(Path(result.kwargs["query_path"]).read_text()) == [signature]


def test_branchwater_mags_search_returns_404_when_signature_missing(
    ninja_api_client, settings, tmp_path
):
    settings.EMG_CONFIG.branchwater.signatures_root = str(tmp_path / "signatures")
    response = ninja_api_client.post(
        "/services/branchwater/mags?accession=MGYG000000000&catalogue=human-gut-v2-0"
    )
    assert response.status_code == 404
    assert "No .sig file found" in response.json()["detail"]


def test_branchwater_mags_rejects_path_traversal(ninja_api_client):
    response = ninja_api_client.post(
        "/services/branchwater/mags?accession=../../secret&catalogue=human-gut-v2-0"
    )
    assert response.status_code == 400


def test_job_links_use_current_request_mount_and_host():
    from emgapiv2.api.branchwater import _job_url

    request = RequestFactory().get(
        "/development/api/v2/services/branchwater/mags", HTTP_HOST="localhost:9000"
    )
    assert (
        _job_url(request, "status", "A" * 32)
        == f"http://localhost:9000/development/api/v2/services/branchwater/status/{'A' * 32}/"
    )
