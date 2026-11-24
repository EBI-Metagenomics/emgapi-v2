"""
Characterisation tests for the ASA batch flows as they behave today (plan 0h).

Three things in the amplicon-batch plan touch shared ASA code: the
``related_name`` rename, the shared summary template, and — if the
recommendations in ``asa-batch-hardening-recommendations.md`` are adopted — the
ASA flows themselves. These tests pin the *current* behaviour of the defects
those recommendations target, so that changing any of it is a visible, reviewed
diff rather than a silent one.

Nothing here asserts that today's behaviour is desirable. Each test says what
the hardening change would make it instead.
"""

from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import Analysis
from workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch import (
    run_assembly_batch,
)
from workflows.flows.analysis.assembly.flows.run_map_batch import run_map_batch
from workflows.flows.analysis.assembly.flows.run_virify_batch import run_virify_batch
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException

ASA_FLOW = (
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch"
)
VIRIFY_FLOW = "workflows.flows.analysis.assembly.flows.run_virify_batch"
MAP_FLOW = "workflows.flows.analysis.assembly.flows.run_map_batch"


@pytest.fixture
def batch_with_one_analysis(
    raw_reads_mgnify_study, raw_reads_mgnify_sample, mgnify_assemblies, tmp_path
):
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )
    return AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )[0]


def _write_virify_samplesheet(batch):
    samplesheets_dir = (
        batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA.value)
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True, exist_ok=True)
    samplesheet = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    samplesheet.write_text("sample,assembly\ntest_sample,test_assembly.fasta")
    return samplesheet


@pytest.mark.django_db(transaction=True)
@patch(f"{ASA_FLOW}.copy_assembly_batch_results")
@patch(f"{ASA_FLOW}.generate_assembly_analysis_pipeline_batch_summary")
@patch(f"{ASA_FLOW}.run_map_batch")
@patch(f"{ASA_FLOW}.run_virify_batch")
@patch(f"{ASA_FLOW}.import_asa_batch")
@patch(f"{ASA_FLOW}.flow_run")
@patch(f"{ASA_FLOW}.run_cluster_job")
@patch(f"{ASA_FLOW}.make_samplesheet_assembly")
def test_asa_cluster_job_failure_leaves_the_flow_run_completed(
    mock_make_samplesheet,
    mock_run_cluster_job,
    mock_flow_run,
    mock_import_asa_batch,
    mock_run_virify_batch,
    mock_run_map_batch,
    mock_generate_summary,
    mock_copy_results,
    prefect_harness,
    batch_with_one_analysis,
    tmp_path,
):
    """
    ``run_assembly_batch`` catches the cluster-job failure, records it and
    ``return``s, so Prefect reports the flow run as **Completed** even though
    the batch row says FAILED — which also means the ``retries=2`` configured on
    the flow never fires.

    The hardening recommendation is to record the failure and then *raise*, at
    which point this assertion becomes ``state.is_failed()``.
    """
    mock_flow_run.id = "asa-flow-run-id"
    mock_make_samplesheet.return_value = (tmp_path / "samplesheet.csv", "test_hash")
    mock_run_cluster_job.side_effect = ClusterJobFailedException(
        "ASA pipeline failed", "FAILED"
    )

    state = run_assembly_batch(
        assembly_analyses_batch_id=batch_with_one_analysis.id, return_state=True
    )

    assert state.is_completed()
    assert not state.is_failed()

    batch_with_one_analysis.refresh_from_db()
    assert batch_with_one_analysis.last_error is not None
    assert (
        batch_with_one_analysis.pipeline_status_counts.asa.failed
        == batch_with_one_analysis.total_analyses
    )

    # The rest of the chain is skipped by the early return.
    mock_import_asa_batch.assert_not_called()
    mock_run_virify_batch.assert_not_called()
    mock_run_map_batch.assert_not_called()
    mock_generate_summary.assert_not_called()
    mock_copy_results.assert_not_called()


@pytest.mark.django_db(transaction=True)
@patch(f"{VIRIFY_FLOW}.import_virify_batch")
@patch(f"{VIRIFY_FLOW}.flow_run")
@patch(f"{VIRIFY_FLOW}.run_cluster_job")
def test_virify_failure_still_runs_the_virify_import(
    mock_run_cluster_job,
    mock_flow_run,
    mock_import_virify_batch,
    prefect_harness,
    batch_with_one_analysis,
):
    """
    ``run_virify_batch`` marks the members FAILED and then falls through to
    ``import_virify_batch`` anyway, because the import call sits after the
    try/except/else rather than inside the ``else``.
    """
    mock_flow_run.id = "virify-flow-run-id"
    mock_run_cluster_job.side_effect = ClusterJobFailedException(
        "VIRify failed", "FAILED"
    )
    batch_with_one_analysis.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.PENDING,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )
    _write_virify_samplesheet(batch_with_one_analysis)

    state = run_virify_batch(
        assembly_analyses_batch_id=batch_with_one_analysis.id, return_state=True
    )

    assert state.is_completed()
    batch_with_one_analysis.refresh_from_db()
    assert (
        batch_with_one_analysis.pipeline_status_counts.virify.failed
        == batch_with_one_analysis.total_analyses
    )
    mock_import_virify_batch.assert_called_once_with(
        assembly_analyses_batch_id=batch_with_one_analysis.id
    )


@pytest.mark.django_db(transaction=True)
@patch(f"{MAP_FLOW}.import_map_batch")
@patch(f"{MAP_FLOW}.make_samplesheet_for_map")
@patch(f"{MAP_FLOW}.flow_run")
@patch(f"{MAP_FLOW}.run_cluster_job")
def test_map_failure_skips_the_map_import(
    mock_run_cluster_job,
    mock_flow_run,
    mock_make_samplesheet,
    mock_import_map_batch,
    prefect_harness,
    batch_with_one_analysis,
    tmp_path,
):
    """
    The sibling of the test above, and the asymmetry worth knowing about:
    ``run_map_batch`` ``return``s from its ``except`` block, so the MAP import
    does *not* run after a MAP failure. Same defect class, opposite behaviour.
    """
    mock_flow_run.id = "map-flow-run-id"
    samplesheet = tmp_path / "samplesheets" / "map_samplesheet.csv"
    samplesheet.parent.mkdir(parents=True, exist_ok=True)
    samplesheet.write_text("sample,assembly,proteins,virify_gff\n")
    mock_make_samplesheet.return_value = (samplesheet, 1)
    mock_run_cluster_job.side_effect = ClusterJobFailedException("MAP failed", "FAILED")
    batch_with_one_analysis.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    state = run_map_batch(
        assembly_analyses_batch_id=batch_with_one_analysis.id, return_state=True
    )

    assert state.is_completed()
    batch_with_one_analysis.refresh_from_db()
    assert (
        batch_with_one_analysis.pipeline_status_counts.map.failed
        == batch_with_one_analysis.total_analyses
    )
    mock_import_map_batch.assert_not_called()
