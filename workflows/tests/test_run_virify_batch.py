import shutil
import pytest
from pathlib import Path, PosixPath
from unittest.mock import patch

from django.conf import settings
from pydantic import BaseModel

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.data_io_utils.schemas import PipelineValidationError
from workflows.flows.analyse_study_tasks.assembly.run_virify_batch import (
    add_virify_gff_to_analysis_downloads,
    run_virify_batch,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


class VirifyTestScenario(BaseModel):
    """VIRify test scenario configuration."""

    study_accession: str
    assembly_accession: str
    sample_accession: str
    base_virify_dir: Path

    class Config:
        frozen = True


@pytest.fixture
def virify_test_scenario():
    """Default VIRify test scenario."""
    return VirifyTestScenario(
        study_accession="PRJEB24849",
        assembly_accession="ERZ857107",
        sample_accession="SAMN08514017",
        base_virify_dir=Path("/app/data/tests/virify_v3_output/"),
    )


def setup_virify_fixtures(virify_outdir: PosixPath, scenario: VirifyTestScenario):
    """
    Copy VIRify test fixtures to the expected location.

    Creates structure: virify_outdir / assembly_accession / 08-final/gff/

    :param virify_outdir: Base directory for VIRify output
    :param scenario: Test scenario configuration
    """
    # Create an assembly-specific directory
    assembly_dir = virify_outdir / scenario.assembly_accession
    assembly_dir.mkdir(parents=True, exist_ok=True)

    # Copy VIRify fixtures (08-final/gff/ structure)
    src = scenario.base_virify_dir / "08-final"
    dst = assembly_dir / "08-final"

    if src.exists():
        shutil.copytree(src, dst, dirs_exist_ok=True)
        print(f"Set up VIRify fixtures in: {dst}")


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


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads(
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    assemblers,
    raw_read_run,
    virify_test_scenario,
):
    """Test adding virify GFF file to analysis downloads."""
    # Create an accessioned assembly
    assembly = analyses.models.Assembly.objects.create(
        run=raw_read_run[0],
        ena_accessions=[virify_test_scenario.assembly_accession],
        reads_study=raw_reads_mgnify_study,
        ena_study=raw_reads_mgnify_study.ena_study,
        assembler=assemblers.first(),
    )

    # Create analysis with an accessioned assembly
    analysis = analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=assembly,
        results_dir=str(tmp_path / virify_test_scenario.assembly_accession),
    )

    # Set up VIRify fixtures in the expected location
    setup_virify_fixtures(tmp_path, virify_test_scenario)

    # Call the task function
    print(tmp_path)
    add_virify_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that the download was added
    analysis.refresh_from_db()
    downloads = analysis.downloads_as_objects
    assert len(downloads) == 1
    download = downloads[0]
    assert "virify" in download.alias.lower()
    assert download.download_type == DownloadType.GENOME_ANALYSIS
    assert download.download_group == "virify"


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads_no_gff_dir(
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    assemblers,
    raw_read_run,
    virify_test_scenario,
):
    """Test adding virify GFF file when GFF directory doesn't exist."""
    assembly = analyses.models.Assembly.objects.create(
        run=raw_read_run[0],
        ena_accessions=[virify_test_scenario.assembly_accession],
        reads_study=raw_reads_mgnify_study,
        ena_study=raw_reads_mgnify_study.ena_study,
        assembler=assemblers.first(),
    )
    analysis = analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=assembly,
        results_dir=str(tmp_path / virify_test_scenario.assembly_accession),
    )

    # Don't set up fixtures - test with empty directory
    # Call the task function with a directory that doesn't have the GFF folder
    with pytest.raises(PipelineValidationError):
        add_virify_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no download was added
    analysis.refresh_from_db()
    assert len(analysis.downloads_as_objects) == 0


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads_file_exists_error(
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    assemblers,
    raw_read_run,
    virify_test_scenario,
):
    """Test adding virify GFF file when it already exists."""
    # Create accessioned assembly
    assembly = analyses.models.Assembly.objects.create(
        run=raw_read_run[0],
        ena_accessions=[virify_test_scenario.assembly_accession],
        reads_study=raw_reads_mgnify_study,
        ena_study=raw_reads_mgnify_study.ena_study,
        assembler=assemblers.first(),
    )

    # Create analysis with accessioned assembly
    analysis = analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=assembly,
        results_dir=str(tmp_path / virify_test_scenario.assembly_accession),
    )

    # Set up VIRify fixtures
    setup_virify_fixtures(tmp_path, virify_test_scenario)

    # Add a download with the same alias first
    download = DownloadFile(
        path="virify/ERZ857107_virify.gff.gz",
        alias="virify_ERZ857107_virify.gff.gz",
        download_type=DownloadType.GENOME_ANALYSIS,
        file_type=DownloadFileType.GFF,
        long_description="Viral genome annotation from VIRify pipeline",
        short_description="Viral annotations",
        download_group="virify",
    )
    analysis.add_download(download)

    # Call the task function
    add_virify_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no additional download was added (still 1)
    analysis.refresh_from_db()
    assert len(analysis.downloads_as_objects) == 1


@pytest.mark.django_db
@patch("workflows.flows.analyse_study_tasks.assembly.run_virify_batch.run_cluster_job")
@patch("workflows.flows.analyse_study_tasks.assembly.run_virify_batch.flow_run")
@patch(
    "workflows.flows.analyse_study_tasks.assembly.run_virify_batch.sanity_check_pipeline_results"
)
@patch(
    "workflows.flows.analyse_study_tasks.assembly.run_virify_batch.VirifyResultSchema"
)
def test_run_virify_batch_success(
    mock_virify_schema,
    mock_sanity_check,
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

    # Mock schema validation to pass
    mock_sanity_check.return_value = None  # Validation succeeds

    # Mock schema to return empty downloads (no files to process)
    mock_schema_instance = mock_virify_schema.return_value
    mock_schema_instance.generate_downloads.return_value = []

    # Create analyses with assemblies using fixtures
    # Note: results_dir doesn't matter here since we're mocking add_virify_gff_to_analysis_downloads
    analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )
    analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[1],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[1],
    )

    # Create batch using create_batches_for_study
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=raw_reads_mgnify_study,
        base_results_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Create the VIRify samplesheet in the expected location
    asa_outdir = batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA.value
    )
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

    # Call the flow function
    run_virify_batch(assembly_batch_id=batch.id)

    # Verify that the cluster job was run
    mock_run_cluster_job.assert_called_once()

    # Verify that the batch state was updated to READY
    batch.refresh_from_db()
    assert batch.virify_state == analyses.models.PipelineState.READY


@pytest.mark.django_db
@patch("workflows.flows.analyse_study_tasks.assembly.run_virify_batch.run_cluster_job")
@patch("workflows.flows.analyse_study_tasks.assembly.run_virify_batch.flow_run")
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
    analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )
    analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[1],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[1],
    )

    # Create batch using create_batches_for_study
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=raw_reads_mgnify_study,
        base_results_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Create the VIRify samplesheet in the expected location
    asa_outdir = batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA.value
    )
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

    # Call the flow function - should raise
    with pytest.raises(ClusterJobFailedException):
        run_virify_batch(assembly_batch_id=batch.id)


@pytest.mark.django_db
def test_run_virify_batch_no_samplesheet(
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """Test running the virify pipeline when the samplesheet doesn't exist."""
    # Create analysis with assembly using fixtures
    analyses.models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    # Create batch using create_batches_for_study
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=raw_reads_mgnify_study,
        base_results_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Call the flow-function-batch results dir won't have samplesheet
    run_virify_batch(assembly_batch_id=batch.id)

    # Verify that the batch state shows failure (no samplesheet means it can't run)
    batch.refresh_from_db()
    assert batch.virify_state == analyses.models.PipelineState.FAILED
