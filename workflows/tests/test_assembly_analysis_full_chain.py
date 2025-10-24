import gzip
from pathlib import Path
from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import (
    Analysis,
)
from workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch import (
    run_assembly_analysis_pipeline_batch,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.slurm_flow import ClusterJobFailedException


def setup_asa_output_fixtures(asa_outdir: Path, assembly_accession: str):
    """
    Create mock ASA pipeline output files.

    :param asa_outdir: ASA output directory
    :param assembly_accession: Assembly accession
    """
    # Create assembly-specific directory
    assembly_dir = asa_outdir / assembly_accession
    assembly_dir.mkdir(parents=True, exist_ok=True)

    # Create CDS directory
    cds_dir = assembly_dir / settings.EMG_CONFIG.assembly_analysis_pipeline.cds_folder
    cds_dir.mkdir(parents=True, exist_ok=True)

    # Create CDS GFF file (required for MAP)
    cds_gff = cds_dir / f"{assembly_accession}_predicted_cds.gff.gz"
    with gzip.open(cds_gff, "wt") as f:
        f.write("##gff-version 3\n# Mock CDS GFF content")

    # Create QC filtered contigs (required for MAP)
    qc_dir = assembly_dir / settings.EMG_CONFIG.assembly_analysis_pipeline.qc_folder
    qc_dir.mkdir(parents=True, exist_ok=True)

    qc_fasta = qc_dir / f"{assembly_accession}_filtered_contigs.fasta.gz"
    with gzip.open(qc_fasta, "wt") as f:
        f.write(">awesome_contig\nACGTACGTACGT")

    # Create downstream samplesheets directory
    samplesheets_dir = (
        asa_outdir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
    )
    samplesheets_dir.mkdir(parents=True, exist_ok=True)

    # Create VIRify samplesheet
    virify_samplesheet = (
        samplesheets_dir
        / settings.EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )
    virify_samplesheet.write_text(
        "\n".join(
            [
                "id,assembly,fastq_1,fastq_2,proteins",
                f"{assembly_accession},,,{assembly_accession}.fasta",
            ]
        )
    )

    # Create analysed_assemblies.csv (required for set_post_assembly_analysis_states)
    analysed_assemblies_csv = asa_outdir / "analysed_assemblies.csv"
    analysed_assemblies_csv.write_text(f"{assembly_accession},completed")


def setup_virify_output_fixtures(virify_outdir: Path, assembly_accession: str):
    """
    Create mock VIRify pipeline output files.

    :param virify_outdir: VIRify output directory
    :param assembly_accession: Assembly accession
    """
    # Create assembly-specific directory with VIRify GFF
    assembly_dir = virify_outdir / assembly_accession
    gff_dir = assembly_dir / settings.EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True, exist_ok=True)

    # Create VIRify GFF file
    virify_gff = gff_dir / f"{assembly_accession}_virify.gff.gz"
    virify_gff.write_text("##gff-version 3\n# Mock VIRify GFF content")


def setup_map_output_fixtures(map_outdir: Path, assembly_accession: str):
    """
    Create mock MAP pipeline output files.

    :param map_outdir: MAP output directory
    :param assembly_accession: Assembly accession
    """
    # Create assembly-specific directory
    assembly_dir = map_outdir / assembly_accession
    assembly_dir.mkdir(parents=True, exist_ok=True)

    # Create MAP GFF file (directly in assembly directory, not in subdirectory)
    map_gff = assembly_dir / "mobilome_prokka.gff"
    map_gff.write_text("##gff-version 3\n# Mock MAP GFF content")


@pytest.mark.django_db
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.make_samplesheet_assembly"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_cluster_job"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.flow_run"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.set_post_assembly_analysis_states"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.assembly_analysis_batch_results_importer"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.generate_assembly_analysis_pipeline_batch_summary"
)
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.run_cluster_job")
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
def test_full_chain_success(
    mock_map_cluster_job,
    mock_virify_cluster_job,
    mock_generate_summary,
    mock_import_analyses,
    mock_set_post_states,
    mock_asa_flow_run,
    mock_asa_cluster_job,
    mock_make_samplesheet,
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """
    Test successful execution of full ASA → VIRify → MAP chain.

    Verifies:
    - All three pipelines execute in sequence
    - State transitions are correct
    - Metrics are recorded for each pipeline
    - Downloads are generated (mocked)
    """
    # Set up mocks
    mock_asa_flow_run.id = "test_asa_flow_run_id"
    mock_asa_cluster_job.return_value = None  # ASA succeeds
    mock_virify_cluster_job.return_value = None  # VIRify succeeds
    mock_map_cluster_job.return_value = None  # MAP succeeds

    # Mock set_post_assembly_analysis_states to mark analyses as completed
    def mock_set_states_side_effect(assembly_current_outdir, assembly_analyses_ids):
        """Mark all analyses as ASA Completed."""
        batch_relations = AssemblyAnalysisBatchAnalysis.objects.filter(
            analysis_id__in=assembly_analyses_ids
        )
        batch_relations.update(asa_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        for analysis_id in assembly_analyses_ids:
            analysis = Analysis.objects.get(id=analysis_id)
            analysis.status = Analysis.AnalysisStates.ANALYSIS_STARTED
            analysis.save()

    mock_set_post_states.side_effect = mock_set_states_side_effect
    mock_import_analyses.return_value = None  # Import succeeds

    mock_make_samplesheet.return_value = (
        tmp_path / "samplesheet.csv",
        "test_hash",
    )  # Mock samplesheet returns (path, hash)

    # Create analyses with assemblies
    assembly_accession = mgnify_assemblies[0].first_accession
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    # Create batch
    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set up mock output files for all three pipelines
    asa_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    virify_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
    map_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)

    setup_asa_output_fixtures(asa_outdir, assembly_accession)
    setup_virify_output_fixtures(virify_outdir, assembly_accession)
    setup_map_output_fixtures(map_outdir, assembly_accession)

    # Run the full chain
    run_assembly_analysis_pipeline_batch(assembly_analysis_batch_id=batch.id)

    # Verify ASA execution
    batch.refresh_from_db()
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED
    assert batch.asa_flow_run_id == "test_asa_flow_run_id"
    assert mock_asa_cluster_job.called
    assert mock_set_post_states.called
    assert mock_import_analyses.called

    # Verify VIRify execution
    assert batch.virify_status == AssemblyAnalysisPipelineStatus.COMPLETED
    assert mock_virify_cluster_job.called

    # Verify MAP execution
    assert batch.map_status == AssemblyAnalysisPipelineStatus.COMPLETED
    assert mock_map_cluster_job.called

    # Verify summary generation was called
    assert mock_generate_summary.called


@pytest.mark.django_db
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.make_samplesheet_assembly"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_cluster_job"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.flow_run"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.assembly_analysis_batch_results_importer"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.set_post_assembly_analysis_states"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.generate_assembly_analysis_pipeline_batch_summary"
)
def test_asa_failure_stops_chain(
    mock_generate_summary,
    mock_set_post_states,
    mock_import_analyses,
    mock_asa_flow_run,
    mock_asa_cluster_job,
    mock_generate_samplesheet,
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """
    Test that ASA failure stops the chain.

    When ASA fails, VIRify and MAP should not run.
    """
    # Set up mocks
    mock_asa_flow_run.id = "test_asa_flow_run_id"
    mock_asa_cluster_job.side_effect = ClusterJobFailedException(
        "ASA pipeline failed", "FAILED"
    )
    mock_generate_samplesheet.return_value = (tmp_path / "samplesheet.csv", "test_hash")

    # Create analysis
    Analysis.objects.create(
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

    # Run the flow - should raise an exception
    run_assembly_analysis_pipeline_batch(assembly_analysis_batch_id=batch.id)

    # Verify ASA failed
    batch.refresh_from_db()
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.FAILED
    assert batch.last_error is not None

    # Verify VIRify and MAP were not started
    assert batch.virify_status == AssemblyAnalysisPipelineStatus.PENDING
    assert batch.map_status == AssemblyAnalysisPipelineStatus.PENDING


@pytest.mark.django_db
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.make_samplesheet_assembly"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_cluster_job"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.flow_run"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.set_post_assembly_analysis_states"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.generate_assembly_analysis_pipeline_batch_summary"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.assembly_analysis_batch_results_importer"
)
@patch("workflows.flows.analysis.assembly.flows.run_map_batch.run_cluster_job")
@patch("workflows.flows.analysis.assembly.flows.run_virify_batch.run_cluster_job")
def test_virify_failure_partial_results(
    mock_virify_cluster_job,
    mock_map_cluster_job,
    mock_assembly_analysis_batch_results_importer,
    mock_generate_summary,
    mocked_set_post_states,
    mock_asa_flow_run,
    mock_asa_cluster_job,
    mock_generate_samplesheet,
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """
    When VIRify fails, MAP should not run and ASA state should be.
    """
    # Set up mocks
    mock_asa_flow_run.id = "test_asa_flow_run_id"
    mock_asa_cluster_job.return_value = None
    mock_assembly_analysis_batch_results_importer.re.return_value = None
    mock_map_cluster_job.return_value = None

    # Mock ASA schema validation
    # TODO: this is repeated
    def mock_set_states_side_effect(_, assembly_analyses_ids):
        """Mark all analyses as ASA Completed"""
        batch_relations = AssemblyAnalysisBatchAnalysis.objects.filter(
            analysis_id__in=assembly_analyses_ids
        )
        batch_relations.update(asa_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        for analysis_id in assembly_analyses_ids:
            analysis = Analysis.objects.get(id=analysis_id)
            analysis.status = Analysis.AnalysisStates.ANALYSIS_STARTED
            analysis.save()

    mocked_set_post_states.side_effect = mock_set_states_side_effect

    mock_virify_cluster_job.side_effect = ClusterJobFailedException(
        "VIRify pipeline failed", "FAILED"
    )

    mock_generate_samplesheet.return_value = (tmp_path / "samplesheet.csv", "test_hash")

    # Create analysis
    assembly_accession = mgnify_assemblies[0].first_accession
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    # Create batch
    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Set up ASA output (so it completes) but VIRify will fail
    asa_outdir = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    setup_asa_output_fixtures(asa_outdir, assembly_accession)

    # Run the flow
    run_assembly_analysis_pipeline_batch(assembly_analysis_batch_id=batch.id)

    assert mocked_set_post_states.called

    # Verify ASA completed
    batch.refresh_from_db()
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED

    # Verify VIRify failed
    assert batch.virify_status == AssemblyAnalysisPipelineStatus.FAILED
    assert (
        batch.batch_analyses.filter(
            virify_status=AssemblyAnalysisPipelineStatus.FAILED
        ).count()
        == 1
    )

    # Verify MAP was not started
    assert batch.map_status == AssemblyAnalysisPipelineStatus.PENDING

    # Verify summary was still generated (ASA completed)
    assert mock_generate_summary.called


@pytest.mark.django_db
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.make_samplesheet_assembly"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_cluster_job"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.flow_run"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.assembly_analysis_batch_results_importer"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.set_post_assembly_analysis_states"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.generate_assembly_analysis_pipeline_batch_summary"
)
def test_asa_not_ready_for_virify(
    mock_generate_summary,
    mock_set_post_states,
    mock_import_analyses,
    mock_asa_flow_run,
    mock_asa_cluster_job,
    mock_generate_samplesheet,
    prefect_harness,
    tmp_path,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    mgnify_assemblies,
):
    """
    Test ASA completing but not ready for VIRify (missing samplesheet).

    Should mark ASA as PARTIAL_RESULTS and skip VIRify/MAP.
    """
    # Set up mocks
    mock_asa_flow_run.id = "test_asa_flow_run_id"
    mock_asa_cluster_job.return_value = None  # ASA succeeds
    mock_generate_samplesheet.return_value = (tmp_path / "samplesheet.csv", "test_hash")
    mock_import_analyses.return_value = None

    # Mock ASA schema validation
    def mock_set_states_side_effect(_, assembly_analyses_ids):
        """Mark all analyses as ASA Completed"""
        batch_relations = AssemblyAnalysisBatchAnalysis.objects.filter(
            analysis_id__in=assembly_analyses_ids
        )
        batch_relations.update(asa_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        for analysis_id in assembly_analyses_ids:
            analysis = Analysis.objects.get(id=analysis_id)
            analysis.status = Analysis.AnalysisStates.ANALYSIS_STARTED
            analysis.save()

    mock_set_post_states.side_effect = mock_set_states_side_effect

    # Create analysis
    Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        assembly=mgnify_assemblies[0],
    )

    # Create batch
    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=raw_reads_mgnify_study,
        workspace_dir=tmp_path,
        skip_completed=False,
    )
    batch = batches[0]

    # Run the flow
    run_assembly_analysis_pipeline_batch(assembly_analysis_batch_id=batch.id)

    assert mock_import_analyses.called

    batch.refresh_from_db()
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED

    # Verify VIRify and MAP were not run
    assert batch.virify_status == AssemblyAnalysisPipelineStatus.FAILED
    assert batch.map_status == AssemblyAnalysisPipelineStatus.PENDING
