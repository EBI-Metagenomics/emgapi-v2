from unittest.mock import Mock, patch

import pytest

from analyses.models import Analysis
from workflows.flows.housekeeping.backfill_assembly_analysis_results_dirs import (
    backfill_assembly_analysis_results_dirs,
)
from workflows.models import AssemblyAnalysisBatch, AssemblyAnalysisBatchAnalysis


@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.housekeeping.backfill_assembly_analysis_results_dirs.create_table_artifact"
)
@patch(
    "workflows.flows.housekeeping.backfill_assembly_analysis_results_dirs.get_run_logger"
)
def test_backfill_updates_blank_results_dir_for_single_batch_only(
    mock_logger,
    mock_create_table_artifact,
    assembly_with_analyses,
    prefect_harness,
    tmp_path,
):
    mock_logger.return_value = Mock()
    study = assembly_with_analyses[0].study
    analyses = list(assembly_with_analyses)

    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )
    batch = batches[0]

    analyses[1].results_dir = "/existing/results"
    analyses[1].save(update_fields=["results_dir"])

    extra_batch = AssemblyAnalysisBatch.objects.create(
        study=study,
        batch_type="assembly_analysis",
        workspace_dir=str(tmp_path / "extra_batch"),
        total_analyses=1,
    )
    AssemblyAnalysisBatchAnalysis.objects.create(
        batch=extra_batch,
        analysis=analyses[2],
    )

    updated_rows = backfill_assembly_analysis_results_dirs()

    analyses[0].refresh_from_db()
    analyses[1].refresh_from_db()
    analyses[2].refresh_from_db()

    assert len(updated_rows) == 3
    assert updated_rows[0] == {
        "analysis_id": analyses[0].id,
        "analysis_accession": analyses[0].accession,
        "assembly_accession": analyses[0].assembly.first_accession or "",
        "batch_count": 1,
        "batch_ids": str(batch.id),
        "previous_results_dir": "",
        "expected_results_dir": str(batch.workspace_dir),
        "modified": True,
    }

    assert updated_rows[1] == {
        "analysis_id": analyses[1].id,
        "analysis_accession": analyses[1].accession,
        "assembly_accession": analyses[1].assembly.first_accession or "",
        "batch_count": 1,
        "batch_ids": str(batch.id),
        "previous_results_dir": "/existing/results",
        "expected_results_dir": str(batch.workspace_dir),
        "modified": False,
    }

    assert updated_rows[2]["analysis_id"] == analyses[2].id
    assert updated_rows[2]["batch_count"] == 2
    assert str(batch.id) in updated_rows[2]["batch_ids"]
    assert str(extra_batch.id) in updated_rows[2]["batch_ids"]
    assert updated_rows[2]["expected_results_dir"] == ""
    assert updated_rows[2]["modified"] is False

    assert analyses[0].results_dir == str(batch.workspace_dir)
    assert analyses[1].results_dir == "/existing/results"
    assert analyses[2].results_dir in (None, "")

    mock_create_table_artifact.assert_called_once_with(
        key="assembly-analysis-results-dir-backfill-updated",
        table=updated_rows,
        description=(
            "Assembly analyses reviewed for results_dir backfill from their "
            "assembly batch (3 analysis/analyses)"
        ),
    )


@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.housekeeping.backfill_assembly_analysis_results_dirs.create_table_artifact"
)
@patch(
    "workflows.flows.housekeeping.backfill_assembly_analysis_results_dirs.get_run_logger"
)
def test_backfill_can_be_limited_to_accessions(
    mock_logger,
    _mock_create_table_artifact,
    assembly_with_analyses,
    prefect_harness,
    tmp_path,
):
    mock_logger.return_value = Mock()
    study = assembly_with_analyses[0].study
    analyses = list(assembly_with_analyses)

    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )
    batch = batches[0]

    updated_rows = backfill_assembly_analysis_results_dirs([analyses[1].accession])

    analyses[0].refresh_from_db()
    analyses[1].refresh_from_db()
    analyses[2].refresh_from_db()

    assert [row["analysis_id"] for row in updated_rows] == [analyses[1].id]
    assert updated_rows[0]["expected_results_dir"] == str(batch.workspace_dir)
    assert analyses[0].results_dir in (None, "")
    assert analyses[1].results_dir == str(batch.workspace_dir)
    assert analyses[2].results_dir in (None, "")
