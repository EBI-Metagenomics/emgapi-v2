from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analysis.assembly.flows.run_map_batch import (
    run_map_batch,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipelineStatus,
    AssemblyAnalysisPipeline,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.make_samplesheet_for_map")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.flow_run")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
def test_run_map_batch_success(
    mock_run_cluster_job,
    mock_flow_run,
    mock_make_samplesheet,
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
    batch.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    # Call the flow function
    run_map_batch(assembly_analyses_batch_id=batch.id)

    # Verify that the cluster job was run
    mock_run_cluster_job.assert_called_once()

    # Verify that the batch state was updated to COMPLETED
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)
    assert batch.pipeline_status_counts.map.completed == batch.total_analyses
    assert (
        batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).count()
        == 2
    )


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

    # Verify that the batch state was updated to FAILED
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)
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

    # Verify that the batch state shows failure (no samplesheet means it can't run)
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.MAP)
    assert batch.pipeline_status_counts.map.failed == batch.total_analyses
