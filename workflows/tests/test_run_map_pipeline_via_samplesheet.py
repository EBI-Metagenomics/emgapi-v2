import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from django.conf import settings

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.flows.analyse_study_tasks.run_map_via_samplesheet import (
    add_map_gff_to_analysis_downloads,
    run_map_pipeline_via_samplesheet,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


@pytest.fixture
def mock_map_outdir(tmp_path):
    """
    Create a mock MAP output directory with GFF files.

    This fixture creates a temporary directory structure that mimics
    the output of the MAP pipeline, including a GFF file in the expected location.
    """
    # Create the final GFF folder
    # Note: Currently using virify_pipeline config as MAP doesn't have its own yet
    gff_dir = tmp_path / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True)

    # Create a mock GFF file
    gff_file = gff_dir / "test_map.gff"
    gff_file.write_text("Mock MAP GFF content")

    return tmp_path


@pytest.fixture
def mock_assembly_pipeline_outdir(tmp_path):
    """
    Create a mock assembly pipeline output directory with a MAP samplesheet.

    This fixture creates a temporary directory structure that mimics
    the output of the assembly pipeline, including a samplesheet for the MAP pipeline.
    """
    # Create the downstream samplesheets folder
    samplesheets_dir = (
        tmp_path
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True)

    # Create a mock MAP samplesheet (currently using virify_samplesheet as per the code)
    samplesheet_path = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    samplesheet_path.write_text(
        "sample,assembly,proteins,virify_gff\ntest_sample,test_assembly.fasta,test_proteins.faa,test_virify.gff"
    )

    return tmp_path


@pytest.mark.django_db
def test_add_map_gff_to_analysis_downloads(mock_map_outdir):
    """
    Test adding MAP GFF file to analysis downloads.

    This test verifies that the add_map_gff_to_analysis_downloads function
    correctly adds a GFF file to the analysis downloads when the file exists.
    """
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function
    add_map_gff_to_analysis_downloads(analysis, mock_map_outdir)

    # Verify that the download was added
    analysis.refresh_from_db()
    downloads = analysis.downloadfile_set.all()
    assert downloads.count() == 1
    download = downloads.first()
    assert download.alias == "map_test_map.gff"
    assert download.download_type == DownloadType.GENOME_ANALYSIS
    assert download.file_type == DownloadFileType.OTHER
    assert download.download_group == "map"
    assert (
        download.long_description
        == "Mobilome annotation from MAP (Mobilome Annotation Pipeline)"
    )
    assert download.short_description == "Mobilome annotations"


@pytest.mark.django_db
def test_add_map_gff_to_analysis_downloads_no_gff_dir(tmp_path):
    """
    Test adding MAP GFF file when GFF directory doesn't exist.

    This test verifies that the add_map_gff_to_analysis_downloads function
    handles the case where the GFF directory doesn't exist gracefully.
    """
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function with a directory that doesn't have the GFF folder
    add_map_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 0


@pytest.mark.django_db
def test_add_map_gff_to_analysis_downloads_no_gff_files(tmp_path):
    """
    Test adding MAP GFF file when no GFF files exist.

    This test verifies that the add_map_gff_to_analysis_downloads function
    handles the case where the GFF directory exists but contains no GFF files.
    """
    # Create the final GFF folder but don't add any files
    gff_dir = tmp_path / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True)

    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the task function
    add_map_gff_to_analysis_downloads(analysis, tmp_path)

    # Verify that no download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 0


@pytest.mark.django_db
def test_add_map_gff_to_analysis_downloads_file_exists_error(mock_map_outdir):
    """
    Test adding MAP GFF file when it already exists.

    This test verifies that the add_map_gff_to_analysis_downloads function
    handles the case where a download with the same alias already exists.
    """
    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Add a download with the same alias
    download = DownloadFile(
        path="map/test_map.gff",
        alias="map_test_map.gff",
        download_type=DownloadType.GENOME_ANALYSIS,
        file_type=DownloadFileType.OTHER,
        long_description="Mobilome annotation from MAP (Mobilome Annotation Pipeline)",
        short_description="Mobilome annotations",
        download_group="map",
    )
    analysis.add_download(download)

    # Call the task function
    add_map_gff_to_analysis_downloads(analysis, mock_map_outdir)

    # Verify that no additional download was added
    analysis.refresh_from_db()
    assert analysis.downloadfile_set.count() == 1


@pytest.mark.django_db
@patch("workflows.flows.analyse_study_tasks.run_map_via_samplesheet.run_cluster_job")
@patch(
    "workflows.flows.analyse_study_tasks.run_map_via_samplesheet.next_enumerated_subdir"
)
@patch("workflows.flows.analyse_study_tasks.run_map_via_samplesheet.flow_run")
@patch(
    "workflows.flows.analyse_study_tasks.run_map_via_samplesheet.add_map_gff_to_analysis_downloads"
)
def test_run_map_pipeline_via_samplesheet_success(
    mock_add_downloads,
    mock_flow_run,
    mock_next_enumerated_subdir,
    mock_run_cluster_job,
    mock_assembly_pipeline_outdir,
):
    """
    Test running the MAP pipeline successfully.

    This test verifies that the run_map_pipeline_via_samplesheet function
    correctly runs the MAP pipeline and adds downloads when successful.
    """
    # Set up mocks
    mock_flow_run.root_flow_run_id = "test_flow_run_id"
    mock_map_outdir = Path("/mock/map/outdir")

    def _return_next_subdir(parent_dir):
        return parent_dir / "001"

    mock_next_enumerated_subdir.side_effect = _return_next_subdir

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
    run_map_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis1, analysis2],
        assembly_pipeline_outdir=mock_assembly_pipeline_outdir,
    )

    # Verify that the cluster job was run with the correct parameters
    mock_run_cluster_job.assert_called_once()
    # Extract the call arguments
    call_args = mock_run_cluster_job.call_args[1]
    assert call_args["name"] == f"MAP pipeline for study {study.ena_study.accession}"
    assert call_args["working_dir"] == mock_map_outdir

    # Verify that the downloads were added for each analysis
    assert mock_add_downloads.call_count == 2
    mock_add_downloads.assert_any_call(analysis1, mock_map_outdir)
    mock_add_downloads.assert_any_call(analysis2, mock_map_outdir)


@pytest.mark.django_db
@patch("workflows.flows.analyse_study_tasks.run_map_via_samplesheet.run_cluster_job")
@patch(
    "workflows.flows.analyse_study_tasks.run_map_via_samplesheet.next_enumerated_subdir"
)
@patch("workflows.flows.analyse_study_tasks.run_map_via_samplesheet.flow_run")
@patch(
    "workflows.flows.analyse_study_tasks.run_map_via_samplesheet.mark_analysis_as_failed"
)
def test_run_map_pipeline_via_samplesheet_cluster_job_failed(
    mock_mark_failed,
    mock_flow_run,
    mock_next_enumerated_subdir,
    mock_run_cluster_job,
    mock_assembly_pipeline_outdir,
):
    """
    Test running the MAP pipeline when the cluster job fails.

    This test verifies that the run_map_pipeline_via_samplesheet function
    correctly handles the case where the cluster job fails.
    """
    # Set up mocks
    mock_flow_run.root_flow_run_id = "test_flow_run_id"
    mock_map_outdir = Path("/mock/map/outdir")
    mock_next_enumerated_subdir.return_value = mock_map_outdir
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
    run_map_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis1, analysis2],
        assembly_pipeline_outdir=mock_assembly_pipeline_outdir,
    )

    # Verify that analyses were marked as failed with the correct reason
    assert mock_mark_failed.call_count == 2
    mock_mark_failed.assert_any_call(analysis1, reason="MAP pipeline failed")
    mock_mark_failed.assert_any_call(analysis2, reason="MAP pipeline failed")


@pytest.mark.django_db
def test_run_map_pipeline_via_samplesheet_no_samplesheet(tmp_path):
    """
    Test running the MAP pipeline when the samplesheet doesn't exist.

    This test verifies that the run_map_pipeline_via_samplesheet function
    correctly handles the case where the samplesheet doesn't exist.
    """
    # Create mock study and analyses
    ena_study = analyses.models.ENAStudy.objects.create(accession="PRJEB12345")
    study = analyses.models.Study.objects.create(
        accession="MGYS00001234", ena_study=ena_study
    )
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Call the flow function with a directory that doesn't have the samplesheet
    result = run_map_pipeline_via_samplesheet(
        mgnify_study=study,
        assembly_analyses=[analysis],
        assembly_pipeline_outdir=tmp_path,
    )

    # Verify that the function returned early
    assert result is None


@pytest.mark.django_db
@patch("workflows.flows.analyse_study_tasks.run_map_via_samplesheet.get_run_logger")
def test_add_map_gff_to_analysis_downloads_logs_warnings(mock_get_logger, tmp_path):
    """
    Test that add_map_gff_to_analysis_downloads logs appropriate warnings.

    This test verifies that the add_map_gff_to_analysis_downloads function
    logs appropriate warnings when the GFF directory or files don't exist.
    """
    # Set up mock logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Create a mock analysis
    study = analyses.models.Study.objects.create(accession="MGYS00001234")
    analysis = analyses.models.Analysis.objects.create(
        accession="MGYA00001234", study=study
    )

    # Test with no GFF directory
    add_map_gff_to_analysis_downloads(analysis, tmp_path)
    mock_logger.warning.assert_called_once()
    assert "does not exist" in mock_logger.warning.call_args[0][0]

    # Reset mock
    mock_logger.reset_mock()

    # Test with empty GFF directory
    gff_dir = tmp_path / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True)
    add_map_gff_to_analysis_downloads(analysis, tmp_path)
    mock_logger.warning.assert_called_once()
    assert "No GFF files found" in mock_logger.warning.call_args[0][0]

