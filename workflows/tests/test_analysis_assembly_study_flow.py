import csv
import gzip
import re
import shutil
from enum import Enum
from pathlib import Path
from typing import List
from unittest.mock import Mock, patch

import pytest
from django.conf import settings
from pydantic import BaseModel

from analyses.base_models.with_downloads_models import DownloadType
from analyses.models import Study, Analysis
from workflows.flows.analysis.assembly.flows.analysis_assembly_study import (
    analysis_assembly_study,
)
from workflows.flows.analysis.assembly.flows.finalize_assembly_study import (
    finalize_assembly_study,
)
from workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch import (
    run_assembly_analysis_pipeline_batch,
)
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.slurm_status import SlurmStatus
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
    generate_assembly_v6_pipeline_results,
)
from workflows.fixtures.assembly_analysis.conftest import AssemblyTestScenario

EMG_CONFIG = settings.EMG_CONFIG


def setup_assembly_batch_fixtures(scenario: AssemblyTestScenario):
    """
    Helper to copy test fixtures into the batch workspace after batch creation.
    This mimics what the actual ASA pipeline would produce, including the VIRify samplesheet.

    :param scenario: Test scenario with study and assembly details
    """
    # Find the batch that was created
    study = Study.objects.get_or_create_for_ena_study(scenario.study_accession)
    batches = AssemblyAnalysisBatch.objects.filter(study=study)

    # Get the most recent batch
    batch = batches.latest("created_at")

    # Get ASA workspace
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)

    # Use the reusable fixture factory
    generate_assembly_v6_pipeline_results(
        asa_workspace=asa_workspace,
        assemblies=[(scenario.assembly_accession_success, "success")],
        copy_from_fixtures=scenario.fixture_source_dir,
    )

    # Fix VIRify samplesheet paths to point to batch workspace
    # The samplesheet is copied from fixtures but has old paths that need updating
    fix_virify_samplesheet_paths(batch)

    print(f"Set up fixtures in batch workspace: {asa_workspace}")


def setup_assembly_batch_fixtures_missing_dir(scenario: AssemblyTestScenario):
    """
    Helper to create incomplete test fixtures with missing required directory.
    This tests validation error handling for missing directories.

    :param scenario: Test scenario with study and assembly details
    """

    # Find the batch that was created
    study = Study.objects.get_or_create_for_ena_study(scenario.study_accession)
    batches = AssemblyAnalysisBatch.objects.filter(study=study)

    # Get the most recent batch
    batch = batches.latest("created_at")

    # Get ASA workspace
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)

    # Copy fixtures normally
    generate_assembly_v6_pipeline_results(
        asa_workspace=asa_workspace,
        assemblies=[(scenario.assembly_accession_success, "success")],
        copy_from_fixtures=scenario.fixture_source_dir,
    )

    # Remove required directory to trigger validation error
    interpro_dir = (
        asa_workspace
        / scenario.assembly_accession_success
        / "functional-annotation"
        / "interpro"
    )
    if interpro_dir.exists():
        shutil.rmtree(interpro_dir)
        print(f"Removed {interpro_dir} to trigger validation error")

    print(f"Set up incomplete fixtures in batch workspace: {asa_workspace}")


def setup_assembly_batch_fixtures_invalid_content(scenario: AssemblyTestScenario):
    """
    Helper to create test fixtures with invalid file content.
    This tests Pandera schema validation error handling.

    :param scenario: Test scenario with study and assembly details
    """

    # Find the batch that was created
    study = Study.objects.get_or_create_for_ena_study(scenario.study_accession)
    batches = AssemblyAnalysisBatch.objects.filter(study=study)

    # Get the most recent batch
    batch = batches.latest("created_at")

    # Get ASA workspace
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)

    # Copy fixtures normally
    generate_assembly_v6_pipeline_results(
        asa_workspace=asa_workspace,
        assemblies=[(scenario.assembly_accession_success, "success")],
        copy_from_fixtures=scenario.fixture_source_dir,
    )

    # Corrupt the InterPro summary file with an invalid accession pattern
    interpro_file = (
        asa_workspace
        / scenario.assembly_accession_success
        / "functional-annotation"
        / "interpro"
        / f"{scenario.assembly_accession_success}_interpro_summary.tsv.gz"
    )

    if interpro_file.exists():
        # Read existing data
        with gzip.open(interpro_file, "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            rows = list(reader)

        # Modify first data row to have invalid accession pattern
        if len(rows) > 1:
            rows[1][0] = "XIPR027417"  # Invalid pattern (should be IPR######)

        # Write back corrupted data
        with gzip.open(interpro_file, "wt") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(rows)

        print(f"Corrupted {interpro_file} with invalid accession pattern")

    print(f"Set up fixtures with invalid content in batch workspace: {asa_workspace}")


def fix_virify_samplesheet_paths(batch: AssemblyAnalysisBatch):
    """
    Fix VIRify samplesheet paths to point to the batch workspace.

    The samplesheet is copied from fixtures but contains paths pointing to the
    fixture location. This updates them to point to the batch's ASA workspace.

    :param batch: The AssemblyAnalysisBatch with fixtures already copied
    """
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)

    # Path to the copied samplesheet
    virify_samplesheet = (
        asa_workspace
        / EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
        / EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )

    if not virify_samplesheet.exists():
        print(f"Warning: VIRify samplesheet not found at {virify_samplesheet}")
        return

    # Read the existing samplesheet
    with open(virify_samplesheet, "r") as f:
        lines = f.readlines()

    # Update paths in each data row (skip header)
    updated_lines = [lines[0]]  # Keep header
    for line in lines[1:]:
        # Extract assembly accession from the line and replace path dynamically
        # This is more robust than hardcoding the fixture path
        # Pattern: replace any path ending with assembly_accession with the workspace path
        parts = line.split(",")
        if len(parts) >= 2:
            # Second column typically contains the path
            old_path = parts[1].strip()
            # Extract just the filename/relative path after assembly accession
            if "/" in old_path:
                # Rebuild path using workspace
                for assembly_analysis in batch.analyses.all():
                    assembly_accession = (
                        assembly_analysis.assembly_or_run.first_accession
                    )
                    if assembly_accession in old_path:
                        # Replace everything before assembly_accession with workspace path
                        path_parts = old_path.split(assembly_accession)
                        new_path = (
                            str(asa_workspace / assembly_accession) + path_parts[1]
                        )
                        line = line.replace(old_path, new_path)
                        break
        updated_lines.append(line)

    # Write an updated samplesheet
    with open(virify_samplesheet, "w") as f:
        f.writelines(updated_lines)

    print(f"Fixed VIRify samplesheet paths at {virify_samplesheet}")


def setup_virify_batch_fixtures(virify_workspace: Path, scenario: AssemblyTestScenario):
    """
    Helper to copy VIRify test fixtures into the batch workspace.
    This mimics what the actual VIRify pipeline would produce.

    VIRify structure: {virify_workspace}/{assembly_accession}/08-final/gff/*.gff (decompressed)

    :param virify_workspace: The VIRify workspace path where results should be created
    :param scenario: Test scenario with study and assembly details
    """

    # Create the final GFF directory structure (includes assembly accession directory)
    assembly_dir = virify_workspace / scenario.assembly_accession_success
    gff_dir = assembly_dir / EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True, exist_ok=True)

    # Copy VIRify GFF fixtures (compressed format with index files)
    src_gff = (
        Path("/app/data/tests/virify_v3_output")
        / "08-final"
        / "gff"
        / f"{scenario.assembly_accession_success}_virify.gff.gz"
    )
    dst_gff = gff_dir / f"{scenario.assembly_accession_success}_virify.gff.gz"

    if src_gff.exists():
        # Copy the compressed GFF file as-is (schema expects .gff.gz format)
        shutil.copy2(src_gff, dst_gff)
        print(f"Copied VIRify GFF from {src_gff} to {dst_gff}")

        # Copy index files (.gzi and .csi)
        src_gzi = src_gff.with_suffix(".gz.gzi")
        src_csi = src_gff.with_suffix(".gz.csi")

        if src_gzi.exists():
            dst_gzi = dst_gff.with_suffix(".gz.gzi")
            shutil.copy2(src_gzi, dst_gzi)
            print(f"Copied VIRify GFF index (.gzi) from {src_gzi} to {dst_gzi}")

        if src_csi.exists():
            dst_csi = dst_gff.with_suffix(".gz.csi")
            shutil.copy2(src_csi, dst_csi)
            print(f"Copied VIRify GFF index (.csi) from {src_csi} to {dst_csi}")
    else:
        print(f"Warning: VIRify fixture not found at {src_gff}")

    print(f"Set up VIRify fixtures in batch workspace: {virify_workspace}")


def setup_map_batch_fixtures(map_workspace: Path, scenario: AssemblyTestScenario):
    """
    Helper to copy MAP test fixtures into the batch workspace.
    This mimics what the actual MAP pipeline would produce.

    MAP structure: {map_workspace}/{assembly_accession}/gff/mobilome_prokka.gff

    :param map_workspace: The MAP workspace path where results should be created
    :param scenario: Test scenario with study and assembly details
    """

    # Create MAP output directory (includes assembly accession directory and gff subdirectory)
    assembly_dir = map_workspace / scenario.assembly_accession_success
    gff_dir = assembly_dir / EMG_CONFIG.map_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True, exist_ok=True)

    # Create a placeholder GFF file to satisfy schema validation
    # MAP expects a {identifier}_user_mobilome_full.gff.gz file in the gff subdirectory
    placeholder_gff = (
        gff_dir / f"{scenario.assembly_accession_success}_user_mobilome_full.gff.gz"
    )
    if not placeholder_gff.exists():
        with gzip.open(placeholder_gff, "wt") as f:
            f.write("##gff-version 3\n# Placeholder MAP output\n")
        print(f"Created placeholder MAP GFF at {placeholder_gff}")
    gzi = placeholder_gff.with_suffix(".gz.gzi")
    gzi.touch(exist_ok=True)

    csi = placeholder_gff.with_suffix(".gz.csi")
    csi.touch(exist_ok=True)

    print(f"Set up MAP fixtures in batch workspace: {map_workspace}")


def setup_study_summary_fixtures(study: Study):
    """
    Helper to create mock study summary files for testing.

    This creates study summary files that would be generated by the
    merge_assembly_study_summaries flow.

    :param study: The Study to create summary files for
    """
    study_dir = study.results_dir_path
    study_dir.mkdir(parents=True, exist_ok=True)

    # Create mock study summary files for different analysis types
    summary_types = [
        "taxonomy",
        "ko",
        "pfam",
        "go",
        "goslim",
        "interpro",
        "antismash",
        "kegg_modules",
    ]

    for summary_type in summary_types:
        summary_file = (
            study_dir / f"{study.first_accession}_{summary_type}_study_summary.tsv"
        )
        summary_file.write_text(
            f"# Mock {summary_type} study summary\naccession\tcount\nMOCK001\t100\n"
        )
        print(f"Created mock study summary: {summary_file}")

    return study_dir


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
@patch("workflows.flows.analysis.assembly.flows.analysis_assembly_study.run_deployment")
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow(
    mock_run_deployment_analysis_assembly_study,
    mock_run_deployment,
    mock_queryset_hash,
    assembly_test_scenario,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    mock_suspend_flow_run,
    assembly_analysis_ena_study,
    assembly_analysis_ena_samples,
    admin_user,
    top_level_biomes,
    tmp_path,
):
    """
    Test the complete assembly analysis flow (ASA pipeline).

    This integration test verifies:
    1. Study and analysis creation from ENA data
    2. Batch creation and workspace setup
    3. Samplesheet generation
    4. Pipeline execution (mocked via cluster job)
    5. Result import and state propagation
    6. Batch state tracking (PENDING -> IN_PROGRESS -> READY)
    7. Pipeline version recording
    8. Taxonomy annotation import

    Test data:
    - Study: PRJEB24849 (ERP106708)
    - Assembly: ERZ857107 (successful analysis)
    - Expected state: ANALYSIS_ANNOTATIONS_IMPORTED
    - Biome: root.engineered
    """
    # Mock run_deployment to prevent actual deployment execution
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    # Mock run_deployment for batch submission to actually call the batch flow
    # This allows the test to verify batch processing while preventing actual deployment
    def run_batch_deployment(name, parameters, timeout):
        """Call the actual batch flow instead of deploying."""
        if EMG_CONFIG.assembly_analysis_pipeline.batch_runner_deployment_id in name:
            run_assembly_analysis_pipeline_batch(
                parameters["assembly_analyses_batch_id"]
            )
        return Mock(id="mock-batch-flow-run-id")

    mock_run_deployment_analysis_assembly_study.side_effect = run_batch_deployment

    mock_queryset_hash.return_value = "abc123"

    # Set upside effect to copy fixtures when a cluster job completes

    def check_job_and_setup_fixtures(*args, **kwargs):
        """Mock cluster job check that also sets up test fixtures."""
        # Set up ASA fixtures
        setup_assembly_batch_fixtures(assembly_test_scenario)

        # Set up VIRify and MAP fixtures (simulating that all pipelines have completed)
        study = Study.objects.get_or_create_for_ena_study(
            assembly_test_scenario.study_accession
        )
        batch = AssemblyAnalysisBatch.objects.filter(study=study).latest("created_at")
        virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
        setup_virify_batch_fixtures(virify_workspace, assembly_test_scenario)
        map_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)
        setup_map_batch_fixtures(map_workspace, assembly_test_scenario)

        return SlurmStatus.completed.value

    mock_check_cluster_job_all_completed.side_effect = check_job_and_setup_fixtures

    # Mock ENA response for assemblies
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(assembly_test_scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": assembly_test_scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": assembly_test_scenario.sample_accession,
                "run_accession": assembly_test_scenario.run_accession,
                "analysis_accession": assembly_test_scenario.assembly_accession_success,
                "completeness_score": "95.0",
                "contamination_score": "1.2",
                "scientific_name": "metagenome",
                "location": "hinxton",
                "lat": "52",
                "lon": "0",
                "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{assembly_test_scenario.assembly_accession_success}/contig.fa.gz",
            },
        ],
    )

    # Mock ENA response for runs
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=read_run&query=.*{re.escape(assembly_test_scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": assembly_test_scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": assembly_test_scenario.sample_accession,
                "run_accession": assembly_test_scenario.run_accession,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{assembly_test_scenario.run_accession}/{assembly_test_scenario.run_accession}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{assembly_test_scenario.run_accession}/{assembly_test_scenario.run_accession}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN08514018",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514018",
                "run_accession": "SRR123457",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR123457/SRR123457_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR123457/SRR123457_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
        ],
    )

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum(
        "BiomeChoices",
        {
            assembly_test_scenario.biome_path: f"Root:{assembly_test_scenario.biome_name}"
        },
    )
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices[assembly_test_scenario.biome_path],
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # GoGoFlow
    analysis_assembly_study(
        study_accession=assembly_test_scenario.study_accession,
        workspace_dir=str(assembly_test_scenario.workspace_dir),
    )

    # Verify mocks were called
    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()
    mock_suspend_flow_run.assert_called()

    # Get study for assertions
    study = Study.objects.get_or_create_for_ena_study(
        assembly_test_scenario.study_accession
    )

    # Verify study has expected analysis
    assert (
        study.analyses.filter(
            assembly__ena_accessions__contains=[
                assembly_test_scenario.assembly_accession_success
            ]
        ).count()
        == 1
    )

    # Verify biome and watchers set correctly
    assert study.biome.biome_name == assembly_test_scenario.biome_name
    assert admin_user == study.watchers.first()

    # Verify analysis completed
    assert (
        study.analyses.filter(status__analysis_annotations_imported=True).count() == 1
    )

    # Will run all the import bits required to "close" the study
    finalize_assembly_study(study_accession=study.accession)

    # Verify study has v6 analyses
    study.refresh_from_db()
    assert study.features.has_v6_analyses

    # Verify taxonomies were imported
    analysis_which_should_have_taxonomies_imported: Analysis = (
        Analysis.objects_and_annotations.get(
            assembly__ena_accessions__contains=[
                assembly_test_scenario.assembly_accession_success
            ]
        )
    )
    assert (
        Analysis.TAXONOMIES
        in analysis_which_should_have_taxonomies_imported.annotations
    )

    # BATCH-LEVEL ASSERTIONS #

    # Verify batch was created and state tracked correctly
    batch = AssemblyAnalysisBatch.objects.get(study=study)

    # Verify the pipeline version was recorded
    assert (
        batch.get_pipeline_version(AssemblyAnalysisPipeline.ASA)
        == settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision
    )

    # Verify the workspace structure
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    assert asa_workspace.exists()
    assert (asa_workspace / "analysed_assemblies.csv").exists()
    assert (asa_workspace / assembly_test_scenario.assembly_accession_success).exists()

    # Verify samplesheet was generated and stored
    assert batch.asa_samplesheet_path is not None
    assert Path(batch.asa_samplesheet_path).exists()

    # Verify analyses are linked to batch
    assert batch.analyses.count() == 1

    # Verify batch metadata
    assert batch.total_analyses == 1
    assert batch.batch_type == "assembly_analysis"
    assert batch.study == study

    # Verify VIRify pipeline state
    assert batch.pipeline_status_counts.virify.completed == batch.total_analyses
    assert batch.virify_samplesheet_path is not None
    assert Path(batch.virify_samplesheet_path).exists()
    assert (
        batch.get_pipeline_version(AssemblyAnalysisPipeline.VIRIFY)
        == settings.EMG_CONFIG.virify_pipeline.pipeline_git_revision
    )

    # Verify VIRify workspace and results
    virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
    assert virify_workspace.exists()
    virify_gff = (
        virify_workspace
        / assembly_test_scenario.assembly_accession_success
        / EMG_CONFIG.virify_pipeline.final_gff_folder
        / f"{assembly_test_scenario.assembly_accession_success}_virify.gff.gz"
    )
    assert virify_gff.exists()
    # Verify index files also exist
    assert virify_gff.with_suffix(".gz.gzi").exists()
    assert virify_gff.with_suffix(".gz.csi").exists()

    # Verify MAP pipeline state
    assert batch.pipeline_status_counts.map.completed == batch.total_analyses
    assert batch.map_samplesheet_path is not None
    # Verify MAP pipeline version
    assert (
        batch.get_pipeline_version(AssemblyAnalysisPipeline.MAP)
        == settings.EMG_CONFIG.map_pipeline.pipeline_git_revision
    )

    # Verify MAP workspace and results
    map_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)
    assert map_workspace.exists()
    map_gff = (
        map_workspace
        / assembly_test_scenario.assembly_accession_success
        / EMG_CONFIG.map_pipeline.final_gff_folder
        / f"{assembly_test_scenario.assembly_accession_success}_user_mobilome_full.gff.gz"
    )
    assert map_gff.exists()
    # Verify batch-analysis relation reflects all three pipelines
    analysis_with_status = study.analyses.get(
        assembly__ena_accessions__contains=[
            assembly_test_scenario.assembly_accession_success
        ]
    )
    batch_analysis_relation = batch.batch_analyses.get(analysis=analysis_with_status)
    assert (
        batch_analysis_relation.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED
    )
    assert (
        batch_analysis_relation.virify_status
        == AssemblyAnalysisPipelineStatus.COMPLETED
    )
    assert (
        batch_analysis_relation.map_status == AssemblyAnalysisPipelineStatus.COMPLETED
    )

    # STUDY SUMMARY ASSERTIONS #

    # Create mock study summary files (simulating merge_assembly_study_summaries)
    study.results_dir = str(tmp_path)
    study.save()
    setup_study_summary_fixtures(study)

    # Add study summaries to downloads
    added_count = add_assembly_study_summaries_to_downloads(study.accession)

    # Verify study summaries were added to downloads
    assert added_count == 8  # All 8 summary types

    # Refresh study to get updated downloads
    study.refresh_from_db()

    # Verify study has downloads
    assert len(study.downloads_as_objects) > 0

    # Verify taxonomy summary is in downloads with the correct type
    taxonomy_downloads = [
        d
        for d in study.downloads_as_objects
        if d.download_group == "study_summary.taxonomy"
    ]
    assert len(taxonomy_downloads) == 1
    assert taxonomy_downloads[0].download_type == DownloadType.TAXONOMIC_ANALYSIS
    assert "taxonomy_study_summary.tsv" in taxonomy_downloads[0].alias

    # Verify functional summaries are in downloads with a correct type
    functional_sources = [
        "ko",
        "pfam",
        "go",
        "goslim",
        "interpro",
        "antismash",
        "kegg_modules",
    ]
    for source in functional_sources:
        functional_downloads = [
            d
            for d in study.downloads_as_objects
            if d.download_group == f"study_summary.{source}"
        ]
        assert len(functional_downloads) == 1, f"Missing {source} summary"
        assert functional_downloads[0].download_type == DownloadType.FUNCTIONAL_ANALYSIS
        assert f"{source}_study_summary.tsv" in functional_downloads[0].alias

    # Test idempotency: run again and verify no duplicates
    added_count_2 = add_assembly_study_summaries_to_downloads(study.accession)
    assert added_count_2 == 8  # Same count
    study.refresh_from_db()
    # Should still have exactly 8 study summary downloads
    study_summary_downloads = [
        d
        for d in study.downloads_as_objects
        if d.download_group.startswith("study_summary")
    ]
    assert len(study_summary_downloads) == 8


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_virify_batch"
)
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
@patch("workflows.flows.analysis.assembly.flows.analysis_assembly_study.run_deployment")
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow_missing_directory(
    mock_run_deployment_analysis_assembly_study,
    mock_run_deployment,
    mock_queryset_hash,
    mock_run_virify_batch,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_analysis_ena_study,
    assembly_analysis_ena_samples,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
    test_workspace,
):
    """
    Test assembly analysis flow validation catches missing required directories.

    NOTE: This test only runs the ASA pipeline. VIRify and MAP are mocked to isolate
    ASA validation testing.

    This test verifies:
    1. Pipeline runs successfully but missing files cause validation failure during sanity check
    2. Analysis marked as ANALYSIS_QC_FAILED with reason "QC ERROR"
    3. Import is skipped for failed analysis
    4. Batch progresses to READY state (batch tracks pipeline completion, not analysis success)

    Test data:
    - Study: PRJEB24849 (ERP106708)
    - Assembly: ERZ857107 (with missing interpro directory)
    - Expected state: ANALYSIS_QC_FAILED
    """
    # Mock run_deployment to prevent actual deployment execution
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    # Mock run_deployment for batch submission to actually call the batch flow
    def run_batch_deployment(name, parameters, timeout):
        """Call the actual batch flow instead of deploying."""
        if EMG_CONFIG.assembly_analysis_pipeline.batch_runner_deployment_id in name:
            run_assembly_analysis_pipeline_batch(
                parameters["assembly_analyses_batch_id"]
            )
        return Mock(id="mock-batch-flow-run-id")

    mock_run_deployment_analysis_assembly_study.side_effect = run_batch_deployment

    # Use same study as original test but different hash for workspace isolation
    scenario = AssemblyTestScenario(
        study_accession="PRJEB24849",
        study_secondary="ERP106708",
        assembly_accession_success="ERZ857107",
        assembly_accession_failed="ERZ857108",
        sample_accession="SAMN08514017",
        run_accession="SRR123456",
        fixture_source_dir=Path("/app/data/tests/assembly_v6_output/ERP106708"),
        workspace_dir=test_workspace / "missing_dir_test",
        biome_path="root.engineered",
        biome_name="Engineered",
    )
    scenario.workspace_dir.mkdir(parents=True, exist_ok=True)

    mock_queryset_hash.return_value = "missing_dir_test"

    # Set up side effect to create incomplete fixtures

    def check_job_and_setup_incomplete_fixtures(*args, **kwargs):
        """Mock cluster job check that sets up incomplete test fixtures."""
        setup_assembly_batch_fixtures_missing_dir(scenario)
        return SlurmStatus.completed.value

    mock_check_cluster_job_all_completed.side_effect = (
        check_job_and_setup_incomplete_fixtures
    )

    # Mock ENA response for assemblies
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": scenario.sample_accession,
                "run_accession": scenario.run_accession,
                "analysis_accession": scenario.assembly_accession_success,
                "completeness_score": "95.0",
                "contamination_score": "1.2",
                "scientific_name": "metagenome",
                "location": "hinxton",
                "lat": "52",
                "lon": "0",
                "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{scenario.assembly_accession_success}/contig.fa.gz",
            },
        ],
    )

    # Mock ENA response for runs
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=read_run&query=.*{re.escape(scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": scenario.sample_accession,
                "run_accession": scenario.run_accession,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{scenario.run_accession}/{scenario.run_accession}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{scenario.run_accession}/{scenario.run_accession}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
        ],
    )

    # Mock biome picker
    BiomeChoices = Enum(
        "BiomeChoices", {scenario.biome_path: f"Root:{scenario.biome_name}"}
    )
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices[scenario.biome_path],
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # RUN MAIN FLOW
    analysis_assembly_study(
        study_accession=scenario.study_accession,
        workspace_dir=str(scenario.workspace_dir),
    )

    # Get study and analysis
    study = Study.objects.get_or_create_for_ena_study(scenario.study_accession)
    analysis = Analysis.objects.get(
        assembly__ena_accessions__contains=[scenario.assembly_accession_success]
    )

    # Verify analysis marked as QC failed (validation happens during sanity check)
    assert analysis.status[analysis.AnalysisStates.ANALYSIS_QC_FAILED]
    assert (
        "ASA Validation error: Pipeline validation failed for Assembly with 6 error"
        in analysis.status[f"{analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"]
    )

    # Verify batch progressed to READY (even though analysis failed QC)
    batch = AssemblyAnalysisBatch.objects.get(study=study)
    assert batch.pipeline_status_counts.asa.failed == batch.total_analyses


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_virify_batch"
)
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
@patch("workflows.flows.analysis.assembly.flows.analysis_assembly_study.run_deployment")
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow_invalid_schema(
    mock_run_deployment_analysis_assembly_study,
    mock_run_deployment,
    mock_queryset_hash,
    mock_run_virify_batch,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_analysis_ena_study,
    assembly_analysis_ena_samples,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
    test_workspace,
):
    """
    Test assembly analysis flow validates file content with Pandera schemas.

    NOTE: This test only runs the ASA pipeline. VIRify and MAP are mocked to isolate
    ASA schema validation testing.

    This test verifies:
    1. Pandera content validation catches invalid data patterns during sanity check
    2. Analysis marked as ANALYSIS_QC_FAILED with the reason "QC ERROR"
    3. Import is skipped for failed analysis
    4. Batch progresses to READY state (batch tracks pipeline completion, not analysis success)

    Test data:
    - Study: PRJEB24849 (ERP106708)
    - Assembly: ERZ857107 (with invalid InterPro accession pattern)
    - Invalid data: XIPR027417 (should match IPR######)
    - Expected state: ANALYSIS_QC_FAILED
    """
    # Mock run_deployment to prevent actual deployment execution
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    # Mock run_deployment for batch submission to actually call the batch flow
    def run_batch_deployment(name, parameters, timeout):
        """Call the actual batch flow instead of deploying."""
        if EMG_CONFIG.assembly_analysis_pipeline.batch_runner_deployment_id in name:
            run_assembly_analysis_pipeline_batch(
                parameters["assembly_analyses_batch_id"]
            )
        return Mock(id="mock-batch-flow-run-id")

    mock_run_deployment_analysis_assembly_study.side_effect = run_batch_deployment

    # Use same study as original test but different hash for workspace isolation
    scenario = AssemblyTestScenario(
        study_accession="PRJEB24849",
        study_secondary="ERP106708",
        assembly_accession_success="ERZ857107",
        assembly_accession_failed="ERZ857108",
        sample_accession="SAMN08514017",
        run_accession="SRR123456",
        fixture_source_dir=Path("/app/data/tests/assembly_v6_output/ERP106708"),
        workspace_dir=test_workspace / "invalid_schema_test",
        biome_path="root.engineered",
        biome_name="Engineered",
    )
    scenario.workspace_dir.mkdir(parents=True, exist_ok=True)

    mock_queryset_hash.return_value = "invalid_schema_test"

    def check_job_and_setup_invalid_fixtures(*args, **kwargs):
        """Mock cluster job check that sets up fixtures with invalid content."""
        setup_assembly_batch_fixtures_invalid_content(scenario)
        return SlurmStatus.completed.value

    mock_check_cluster_job_all_completed.side_effect = (
        check_job_and_setup_invalid_fixtures
    )

    # Mock ENA response for assemblies
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": scenario.sample_accession,
                "run_accession": scenario.run_accession,
                "analysis_accession": scenario.assembly_accession_success,
                "completeness_score": "95.0",
                "contamination_score": "1.2",
                "scientific_name": "metagenome",
                "location": "hinxton",
                "lat": "52",
                "lon": "0",
                "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{scenario.assembly_accession_success}/contig.fa.gz",
            },
        ],
    )

    # Mock ENA response for runs
    httpx_mock.add_response(
        url=re.compile(
            f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=read_run&query=.*{re.escape(scenario.study_accession)}.*"
        ),
        json=[
            {
                "sample_accession": scenario.sample_accession,
                "sample_title": "my data",
                "secondary_sample_accession": scenario.sample_accession,
                "run_accession": scenario.run_accession,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{scenario.run_accession}/{scenario.run_accession}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{scenario.run_accession}/{scenario.run_accession}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
        ],
    )

    # Mock biome picker
    BiomeChoices = Enum(
        "BiomeChoices", {scenario.biome_path: f"Root:{scenario.biome_name}"}
    )
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices[scenario.biome_path],
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # RUN MAIN FLOW
    analysis_assembly_study(
        study_accession=scenario.study_accession,
        workspace_dir=str(scenario.workspace_dir),
    )

    # Get study and analysis
    study = Study.objects.get_or_create_for_ena_study(scenario.study_accession)
    analysis = Analysis.objects.get(
        assembly__ena_accessions__contains=[scenario.assembly_accession_success]
    )

    # Verify analysis marked as QC failed (validation happens during sanity check)
    assert analysis.status[analysis.AnalysisStates.ANALYSIS_QC_FAILED]
    assert (
        "XIPR027417"
        in analysis.status[f"{analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"]
    )

    # Verify batch progressed to READY (even though analysis failed QC)
    batch = AssemblyAnalysisBatch.objects.get(study=study)
    assert batch.pipeline_status_counts.asa.failed == batch.total_analyses
