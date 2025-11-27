from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import Analysis
from workflows.flows.analysis.assembly.flows.run_virify_batch import (
    run_virify_batch,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


@pytest.fixture
def mock_virify_outdir(tmp_path):
    """Create a mock virify output directory with GFF files."""
    # Create the final GFF folder
    gff_dir = tmp_path / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True)

    # Create a mock GFF file
    gff_file = gff_dir / "test_virify.gff"
    gff_file.write_text("Mock GFF content")

    return tmp_path


@pytest.fixture
def mock_assembly_pipeline_outdir(tmp_path):
    """Create a mock assembly pipeline output directory with a virify samplesheet."""
    # Create the downstream samplesheets folder
    samplesheets_dir = (
        tmp_path
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True)

    # Create a mock virify samplesheet
    samplesheet_path = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    samplesheet_path.write_text("sample,assembly\ntest_sample,test_assembly.fasta")

    return tmp_path


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.run_cluster_job")
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.flow_run")
def test_run_virify_batch_success(
    mock_flow_run,
    mock_run_cluster_job,
    prefect_harness,
    mock_assembly_pipeline_outdir,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
):
    """Test running the virify pipeline successfully."""
    # Set up mocks
    mock_flow_run.id = "test_flow_run_id"

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
        virify_status=AssemblyAnalysisPipelineStatus.PENDING,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    # Create the VIRify samplesheet in the expected location
    asa_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA.value)
    samplesheets_dir = (
        asa_outdir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True, exist_ok=True)
    samplesheet_path = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    samplesheet_path.write_text("sample,assembly\ntest_sample,test_assembly.fasta")

    # GoGoFlow
    run_virify_batch(assembly_analyses_batch_id=batch.id)

    mock_run_cluster_job.assert_called_once()

    # Verify that the batch state was updated to COMPLETED
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)
    assert batch.pipeline_status_counts.virify.completed == batch.total_analyses


@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.run_cluster_job")
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.flow_run")
def test_run_virify_batch_cluster_job_failed(
    mock_flow_run,
    mock_run_cluster_job,
    prefect_harness,
    mock_assembly_pipeline_outdir,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
    tmp_path,
):
    """Test running the virify pipeline when the cluster job fails."""
    # Set up mocks
    mock_flow_run.id = "test_flow_run_id"
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
        virify_status=AssemblyAnalysisPipelineStatus.PENDING,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    # Create the VIRify samplesheet in the expected location
    asa_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA.value)
    samplesheets_dir = (
        asa_outdir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True, exist_ok=True)
    samplesheet_path = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    samplesheet_path.write_text("sample,assembly\ntest_sample,test_assembly.fasta")

    # Call the flow function - should not raise by default
    run_virify_batch(assembly_analyses_batch_id=batch.id)

    # Verify batch status is FAILED
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)
    assert batch.pipeline_status_counts.virify.failed == batch.total_analyses


@pytest.mark.django_db(transaction=True)
def test_run_virify_batch_no_samplesheet(
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """Test running the virify pipeline when the samplesheet doesn't exist."""
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
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set initial pipeline status on batch relations
    batch.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        virify_status=AssemblyAnalysisPipelineStatus.PENDING,
        map_status=AssemblyAnalysisPipelineStatus.PENDING,
    )

    # GoGoFlow
    run_virify_batch(assembly_analyses_batch_id=batch.id)

    # Verify that the batch state shows failure (no samplesheet means it can't run)
    batch.refresh_from_db()
    batch.update_pipeline_status_counts(AssemblyAnalysisPipeline.VIRIFY)
    assert batch.pipeline_status_counts.virify.failed == batch.total_analyses
