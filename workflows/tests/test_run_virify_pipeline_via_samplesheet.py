import pytest
from pathlib import Path
from unittest.mock import patch

from django.conf import settings

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet import (
    add_virify_gff_to_analysis_downloads,
    run_virify_pipeline_via_samplesheet,
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


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads(mock_virify_outdir):
    """Test adding virify GFF file to analysis downloads."""
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function
    add_virify_gff_to_analysis_downloads(analysis, mock_virify_outdir)

    # Verify that the download was added
    analysis.refresh_from_db()
    downloads = analysis.downloadfile_set.all()
    assert downloads.count() == 1
    download = downloads.first()
    assert download.alias == "virify_test_virify.gff"
    assert download.download_type == DownloadType.GENOME_ANALYSIS
    assert download.file_type == DownloadFileType.OTHER
    assert download.download_group == "virify"


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads_no_gff_dir(tmp_path):
    """Test adding virify GFF file when GFF directory doesn't exist."""
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function with a directory that doesn't have the GFF folder
    add_virify_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 0


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads_no_gff_files(tmp_path):
    """Test adding virify GFF file when no GFF files exist."""
    # Create the final GFF folder but don't add any files
    gff_dir = tmp_path / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True)

    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function
    add_virify_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 0


@pytest.mark.django_db
def test_add_virify_gff_to_analysis_downloads_file_exists_error(mock_virify_outdir):
    """Test adding virify GFF file when it already exists."""
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Add a download with the same alias
    download = DownloadFile(
        path="virify/test_virify.gff",
        alias="virify_test_virify.gff",
        download_type=DownloadType.GENOME_ANALYSIS,
        file_type=DownloadFileType.OTHER,
        long_description="Viral genome annotation from VIRify pipeline",
        short_description="Viral annotations",
        download_group="virify",
    )
    analysis.add_download(download)

    # Call the task function
    add_virify_gff_to_analysis_downloads(analysis, mock_virify_outdir)

    # Verify that no additional download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 1


@pytest.mark.django_db
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.run_cluster_job"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.next_enumerated_subdir"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.flow_run"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.add_virify_gff_to_analysis_downloads"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.generate_study_summary_for_pipeline_run"
)
def test_run_virify_pipeline_via_samplesheet_success(
    mock_generate_summary,
    mock_add_downloads,
    mock_flow_run,
    mock_next_enumerated_subdir,
    mock_run_cluster_job,
    mock_assembly_pipeline_outdir,
):
    """Test running the virify pipeline successfully."""
    # Set up mocks
    mock_flow_run.root_flow_run_id = "test_flow_run_id"
    mock_virify_outdir = Path("/mock/virify/outdir")
    mock_next_enumerated_subdir.return_value = mock_virify_outdir

    # Create mock study and analyses
    ena_study = analyses.models.ENAStudy.objects.create(accession="PRJEB12345")
    study = analyses.models.Study.objects.create(
        accession="MGYS00001234", ena_study=ena_study
    )
    analysis1 = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )
    analysis2 = analyses.models.Analysis.objects.create(
        accession="MGYA00001235", study=study
    )

    # Call the flow function
    run_virify_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis1, analysis2],
        assembly_pipeline_outdir=mock_assembly_pipeline_outdir,
    )

    # Verify that the cluster job was run
    mock_run_cluster_job.assert_called_once()

    # Verify that the downloads were added for each analysis
    assert mock_add_downloads.call_count == 2
    mock_add_downloads.assert_any_call(analysis1, mock_virify_outdir)
    mock_add_downloads.assert_any_call(analysis2, mock_virify_outdir)

    # Verify that the study summary was generated
    mock_generate_summary.assert_called_once_with(
        pipeline_outdir=mock_virify_outdir,
        mgnify_study_accession=study.accession,
        analysis_type="virify",
        completed_runs_filename="virify_completed_runs.csv",
    )


@pytest.mark.django_db
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.run_cluster_job"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.next_enumerated_subdir"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.flow_run"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.mark_analysis_as_failed"
)
def test_run_virify_pipeline_via_samplesheet_cluster_job_failed(
    mock_mark_failed,
    mock_flow_run,
    mock_next_enumerated_subdir,
    mock_run_cluster_job,
    mock_assembly_pipeline_outdir,
):
    """Test running the virify pipeline when the cluster job fails."""
    # Set up mocks
    mock_flow_run.root_flow_run_id = "test_flow_run_id"
    mock_virify_outdir = Path("/mock/virify/outdir")
    mock_next_enumerated_subdir.return_value = mock_virify_outdir
    mock_run_cluster_job.side_effect = ClusterJobFailedException("Cluster job failed")

    # Create mock study and analyses
    ena_study = analyses.models.ENAStudy.objects.create(accession="PRJEB12345")
    study = analyses.models.Study.objects.create(
        accession="MGYS00001234", ena_study=ena_study
    )
    analysis1 = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )
    analysis2 = analyses.models.Analysis.objects.create(
        accession="MGYA00001235", study=study
    )

    # Call the flow function
    run_virify_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis1, analysis2],
        assembly_pipeline_outdir=mock_assembly_pipeline_outdir,
    )

    # Verify that analyses were marked as failed
    assert mock_mark_failed.call_count == 2
    mock_mark_failed.assert_any_call(analysis1, reason="Virify pipeline failed")
    mock_mark_failed.assert_any_call(analysis2, reason="Virify pipeline failed")


@pytest.mark.django_db
def test_run_virify_pipeline_via_samplesheet_no_samplesheet(tmp_path):
    """Test running the virify pipeline when the samplesheet doesn't exist."""
    # Create mock study and analyses
    ena_study = analyses.models.ENAStudy.objects.create(accession="PRJEB12345")
    study = analyses.models.Study.objects.create(
        accession="MGYS00001234", ena_study=ena_study
    )
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the flow function with a directory that doesn't have the samplesheet
    result = run_virify_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis],
        assembly_pipeline_outdir=tmp_path,
    )

    # Verify that the function returned early
    assert result is None
