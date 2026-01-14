from pathlib import Path
from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analysis.assembly.flows.run_map_batch import (
    run_map_batch,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.import_map_batch")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.make_samplesheet_for_map")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.flow_run")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
def test_run_map_batch_success(
    mock_run_cluster_job,
    mock_flow_run,
    mock_make_samplesheet,
    mock_import_map_batch,
    prefect_harness,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
):
    """Test running the MAP pipeline successfully."""
    # Set up mocks
    mock_flow_run.id = "test_flow_run_id"

    # Mock samplesheet creation
    samplesheet_path = tmp_path / "samplesheets" / "map_samplesheet.csv"
    samplesheet_path.parent.mkdir(parents=True, exist_ok=True)
    samplesheet_path.write_text("sample,assembly,proteins,virify_gff\n")
    mock_make_samplesheet.return_value = (samplesheet_path, 1)

    # Create analyses with assemblies using fixtures
    # One with VIRify COMPLETED, one with VIRify FAILED
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[1],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[1],
    )

    # Create batch using get_or_create_batches_for_study
    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set initial pipeline status on batch relations
    # First analysis: virify_status=COMPLETED
    # Second analysis: virify_status=FAILED
    batch_analyses = list(batch.batch_analyses.all())
    ba1 = batch_analyses[0]
    ba1.asa_status = AssemblyAnalysisPipelineStatus.COMPLETED
    ba1.virify_status = AssemblyAnalysisPipelineStatus.COMPLETED
    ba1.map_status = AssemblyAnalysisPipelineStatus.PENDING
    ba1.save()

    ba2 = batch_analyses[1]
    ba2.asa_status = AssemblyAnalysisPipelineStatus.COMPLETED
    ba2.virify_status = AssemblyAnalysisPipelineStatus.FAILED
    ba2.map_status = AssemblyAnalysisPipelineStatus.PENDING
    ba2.save()

    # Analysis 2 needs to have ANALYSIS_QC_FAILED status to be picked up
    ba2.analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
    ba2.analysis.save()

    # Call the flow function
    run_map_batch(assembly_analyses_batch_id=batch.id)

    # Asserts methods are called
    mock_run_cluster_job.assert_called_once()
    mock_import_map_batch.assert_called_once()

    # Verify that the task was called with the correct IDs
    mock_make_samplesheet.assert_called_once()
    args, kwargs = mock_make_samplesheet.call_args
    assert args[0] == batch.id
    assert set(kwargs["analysis_batch_job_ids"]) == {ba1.id, ba2.id}
    assert kwargs["output_dir"] == Path(batch.workspace_dir) / "samplesheets"

    # Verify that the batch state was updated to COMPLETED
    batch.refresh_from_db()
    assert batch.pipeline_status_counts.map.completed == batch.total_analyses
    assert (
        batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).count()
        == 2
    )


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.import_map_batch")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.make_samplesheet_for_map")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.flow_run")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
def test_run_map_batch_skips_failed_virify_without_qc_status(
    mock_run_cluster_job,
    mock_flow_run,
    mock_make_samplesheet,
    mock_import_map_batch,
    prefect_harness,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
):
    """Test that MAP skips analyses where VIRify failed but not due to QC failure."""
    # Set up mocks
    mock_flow_run.id = "test_flow_run_id"

    # Create analysis with assembly
    analysis = Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set VIRify status to FAILED but DON'T set ANALYSIS_QC_FAILED
    ba = batch.batch_analyses.get(analysis=analysis)
    ba.asa_status = AssemblyAnalysisPipelineStatus.COMPLETED
    ba.virify_status = AssemblyAnalysisPipelineStatus.FAILED
    ba.map_status = AssemblyAnalysisPipelineStatus.PENDING
    ba.save()

    # Call the flow function
    run_map_batch(assembly_analyses_batch_id=batch.id)

    # Asserts
    mock_run_cluster_job.assert_not_called()
    mock_make_samplesheet.assert_not_called()

    # Verify status remains PENDING
    ba.refresh_from_db()
    assert ba.map_status == AssemblyAnalysisPipelineStatus.PENDING


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.flow_run")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.make_samplesheet_for_map")
def test_run_map_batch_cluster_job_failed(
    mock_make_samplesheet,
    mock_flow_run,
    mock_run_cluster_job,
    prefect_harness,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
):
    """Test running the MAP pipeline when the cluster job fails."""
    # Set up mocks
    mock_flow_run.id = "test_flow_run_id"

    # Mock samplesheet creation
    samplesheet_path = tmp_path / "samplesheets" / "map_samplesheet.csv"
    samplesheet_path.parent.mkdir(parents=True, exist_ok=True)
    samplesheet_path.write_text("sample,assembly,proteins,virify_gff\n")
    mock_make_samplesheet.return_value = (samplesheet_path, 1)

    mock_run_cluster_job.side_effect = ClusterJobFailedException(
        "Cluster job failed", "FAILED"
    )

    # Create analyses with assemblies using fixtures
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[1],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[1],
    )

    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set initial pipeline status on batch relations
    batch.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    run_map_batch(assembly_analyses_batch_id=batch.id)

    # Verify that the task was called with the correct IDs
    mock_make_samplesheet.assert_called_once()
    args, kwargs = mock_make_samplesheet.call_args
    assert args[0] == batch.id
    assert len(kwargs["analysis_batch_job_ids"]) == batch.total_analyses

    # Verify that the batch state was updated to FAILED
    batch.refresh_from_db()
    assert batch.pipeline_status_counts.map.failed == batch.total_analyses


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.make_samplesheet_for_map")
def test_run_map_batch_no_samplesheet(
    mock_make_samplesheet,
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """Test running the MAP pipeline when the samplesheet doesn't exist."""
    # Mock samplesheet creation to return a non-existent path
    nonexistent_path = tmp_path / "nonexistent" / "map_samplesheet.csv"
    mock_make_samplesheet.return_value = (nonexistent_path, 0)

    # Create analysis with assembly using fixtures
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    # Create batch using get_or_create_batches_for_study
    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set initial pipeline status on batch relations
    batch.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    # Call the flow-function-batch results dir won't have samplesheet
    run_map_batch(assembly_analyses_batch_id=batch.id)

    # Verify that the task was called with the correct IDs
    mock_make_samplesheet.assert_called_once()
    args, kwargs = mock_make_samplesheet.call_args
    assert args[0] == batch.id
    # All analyses should be processed because they are COMPLETED for VIRify
    assert len(kwargs["analysis_batch_job_ids"]) == batch.total_analyses

    # Verify that the batch state shows failure (no samplesheet means it can't run)
    batch.refresh_from_db()
    assert batch.pipeline_status_counts.map.failed == batch.total_analyses
