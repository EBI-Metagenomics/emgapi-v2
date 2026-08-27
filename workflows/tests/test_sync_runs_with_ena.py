import json
import re
from unittest.mock import patch

import pytest
from prefect.artifacts import Artifact

import analyses.models
from workflows.ena_utils.ena_api_requests import (
    RUN_METADATA_FIELDS,
    sync_run_metadata_from_ena,
)
from workflows.flows.housekeeping.sync_runs_with_ena import sync_runs_with_ena


@pytest.mark.django_db
def test_sync_run_metadata_from_ena_merges_metadata(httpx_mock, raw_read_run):
    run = raw_read_run[0]
    run.metadata = {
        analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS: [
            "ftp://example.org/read_1.fastq.gz",
            "ftp://example.org/read_2.fastq.gz",
        ],
        analyses.models.Run.CommonMetadataKeys.INFERRED_LIBRARY_LAYOUT: "PAIRED",
        analyses.models.Run.CommonMetadataKeys.INSTRUMENT_MODEL: "Old model",
    }
    run.save(update_fields=["metadata"])

    httpx_mock.add_response(
        url=re.compile(r".*result=read_run.*"),
        json=[
            {
                "library_strategy": "WGS",
                "library_layout": "PAIRED",
                "instrument_model": "Illumina NovaSeq 6000",
                "instrument_platform": "ILLUMINA",
            }
        ],
    )

    sync_run_metadata_from_ena(run)

    run.refresh_from_db()
    assert run.instrument_model == "Illumina NovaSeq 6000"
    assert run.instrument_platform == "ILLUMINA"
    assert run.metadata["fastq_ftps"] == [
        "ftp://example.org/read_1.fastq.gz",
        "ftp://example.org/read_2.fastq.gz",
    ]
    assert run.metadata["inferred_library_layout"] == "PAIRED"

    request = httpx_mock.get_request()
    assert request.url.params["query"] == f'"run_accession={run.first_accession}"'
    assert request.url.params["fields"] == ",".join(RUN_METADATA_FIELDS)


@patch("workflows.flows.housekeeping.sync_runs_with_ena.sync_run_metadata_from_ena")
@pytest.mark.django_db
def test_sync_runs_with_ena_by_accessions(mock_sync, prefect_harness, raw_read_run):
    successful_run, *failing_runs = raw_read_run

    def mock_sync_run(run: analyses.models.Run):
        if run in failing_runs:
            raise RuntimeError("ENA error")
        run.metadata["instrument_model"] = "Updated model"
        run.metadata["instrument_platform"] = "UPDATED_PLATFORM"
        run.save(update_fields=["metadata"])

    mock_sync.side_effect = mock_sync_run

    accessions = [run.first_accession for run in raw_read_run]
    failed = sync_runs_with_ena(accessions=accessions, batch_size=1)

    successful_run.refresh_from_db()
    assert successful_run.instrument_model == "Updated model"
    assert successful_run.instrument_platform == "UPDATED_PLATFORM"
    assert failed == [run.first_accession for run in failing_runs]
    assert mock_sync.call_count == 3

    failed_syncs_table = Artifact.get("failed-ena-run-syncs")
    assert failed_syncs_table.type == "table"
    assert json.loads(failed_syncs_table.data) == [
        {"accession": run.first_accession} for run in failing_runs
    ]


@patch("workflows.flows.housekeeping.sync_runs_with_ena.sync_run_metadata_from_ena")
@pytest.mark.django_db
def test_sync_runs_with_ena_resolves_secondary_accessions(
    mock_sync, prefect_harness, raw_read_run
):
    run = raw_read_run[0]
    run.ena_accessions.append("ERR0000001")
    run.save(update_fields=["ena_accessions"])

    failed = sync_runs_with_ena(accessions=["ERR0000001"], batch_size=1)

    assert failed == []
    mock_sync.assert_called_once_with(run)


@patch("workflows.flows.housekeeping.sync_runs_with_ena.sync_run_metadata_from_ena")
@pytest.mark.django_db
def test_sync_runs_with_ena_syncs_all_runs(mock_sync, prefect_harness, raw_read_run):
    failed = sync_runs_with_ena(all_runs=True, batch_size=2)

    assert failed == []
    assert mock_sync.call_count == len(raw_read_run)
    assert {call.args[0].pk for call in mock_sync.call_args_list} == {
        run.pk for run in raw_read_run
    }
