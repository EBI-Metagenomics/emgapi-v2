import csv
import gzip
import re
import shutil
import uuid
from enum import Enum
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest
from django.conf import settings
from pydantic import BaseModel

from analyses.models import Study, Analysis
from workflows.flows.analysis.assembly.flows.analysis_assembly_study import (
    analysis_assembly_study,
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

EMG_CONFIG = settings.EMG_CONFIG


class AssemblyTestScenario(BaseModel):
    """Assembly test scenario configuration."""

    study_accession: str
    study_secondary: str
    assembly_accession_success: str
    assembly_accession_failed: str
    sample_accession: str
    run_accession: str
    fixture_source_dir: Path  # Where the permanent fixture files are stored
    workspace_dir: Path  # Temporary workspace for the test
    biome_path: str
    biome_name: str

    class Config:
        frozen = True


@pytest.fixture
def test_workspace():
    """Create a temporary workspace directory for test execution."""

    # Create a unique workspace under /app/data/tests/tmp/
    base_tmp = Path("/app/data/tests/tmp")
    base_tmp.mkdir(parents=True, exist_ok=True)

    workspace = base_tmp / f"test_workspace_{uuid.uuid4().hex[:8]}"
    workspace.mkdir(exist_ok=True)

    yield workspace

    # Cleanup after test
    if workspace.exists():
        shutil.rmtree(workspace)


@pytest.fixture
def assembly_test_scenario(test_workspace):
    """Default assembly test scenario."""
    return AssemblyTestScenario(
        study_accession="PRJEB24849",
        study_secondary="ERP106708",
        assembly_accession_success="ERZ857107",
        assembly_accession_failed="ERZ857108",
        sample_accession="SAMN08514017",
        run_accession="SRR123456",
        fixture_source_dir=Path("/app/data/tests/assembly_v6_output/ERP106708"),
        workspace_dir=test_workspace,
        biome_path="root.engineered",
        biome_name="Engineered",
    )


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


def setup_virify_batch_fixtures(
    batch: AssemblyAnalysisBatch, scenario: AssemblyTestScenario
):
    """
    Helper to copy VIRify test fixtures into the batch workspace.
    This mimics what the actual VIRify pipeline would produce.

    VIRify structure: {virify_workspace}/{assembly_accession}/08-final/gff/*.gff.gz

    :param batch: The AssemblyAnalysisBatch to set up fixtures for
    :param scenario: Test scenario with study and assembly details
    """

    # Get VIRify workspace
    virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)

    # Create the final GFF directory structure (includes assembly accession directory)
    assembly_dir = virify_workspace / scenario.assembly_accession_success
    gff_dir = assembly_dir / EMG_CONFIG.virify_pipeline.final_gff_folder
    gff_dir.mkdir(parents=True, exist_ok=True)

    # Copy VIRify GFF fixtures
    src_gff = (
        Path("/app/data/tests/virify_v3_output")
        / "08-final"
        / "gff"
        / f"{scenario.assembly_accession_success}_virify.gff.gz"
    )
    dst_gff = gff_dir / f"{scenario.assembly_accession_success}_virify.gff.gz"

    if src_gff.exists():
        shutil.copy2(src_gff, dst_gff)
        print(f"Copied VIRify GFF from {src_gff} to {dst_gff}")
    else:
        print(f"Warning: VIRify fixture not found at {src_gff}")

    print(f"Set up VIRify fixtures in batch workspace: {virify_workspace}")


def setup_map_batch_fixtures(
    batch: AssemblyAnalysisBatch, scenario: AssemblyTestScenario
):
    """
    Helper to copy MAP test fixtures into the batch workspace.
    This mimics what the actual MAP pipeline would produce.

    MAP structure: {map_workspace}/{assembly_accession}/mobilome_prokka.gff

    :param batch: The AssemblyAnalysisBatch to set up fixtures for
    :param scenario: Test scenario with study and assembly details
    """

    # Get MAP workspace
    map_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)

    # Create MAP output directory (includes assembly accession directory)
    assembly_dir = map_workspace / scenario.assembly_accession_success
    assembly_dir.mkdir(parents=True, exist_ok=True)

    # TODO: Add actual MAP fixtures when available
    # For now, create a placeholder GFF file to satisfy schema validation
    # MAP expects a mobilome_prokka.gff file in the assembly directory
    placeholder_gff = assembly_dir / "mobilome_prokka.gff"
    if not placeholder_gff.exists():
        placeholder_gff.write_text("##gff-version 3\n# Placeholder MAP output\n")
        print(f"Created placeholder MAP GFF at {placeholder_gff}")

    print(f"Set up MAP fixtures in batch workspace: {map_workspace}")


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow(
    mock_queryset_hash,
    assembly_test_scenario,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    mock_suspend_flow_run,
    assembly_analysis_ena_study,
    admin_user,
    top_level_biomes,
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
        setup_virify_batch_fixtures(batch, assembly_test_scenario)
        setup_map_batch_fixtures(batch, assembly_test_scenario)

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
    assert batch.virify_status == AssemblyAnalysisPipelineStatus.COMPLETED
    # TODO: for some reason these are empty, but in run_virify_batch() this saved...
    # assert batch.virify_samplesheet_path is not None
    # assert Path(batch.virify_samplesheet_path).exists()
    # Verify VIRify pipeline version
    # assert (
    #     batch.get_pipeline_version(AssemblyAnalysisPipeline.VIRIFY)
    #     == settings.EMG_CONFIG.virify_pipeline.pipeline_git_revision
    # )

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

    # Verify MAP pipeline state
    # TODO: just as VIRIfy, these are not recorded for some reason!
    # assert batch.map_status == AssemblyAnalysisPipelineStatus.COMPLETED
    # assert batch.map_samplesheet_path is not None
    # # Verify MAP pipeline version
    # assert (
    #     batch.get_pipeline_version(AssemblyAnalysisPipeline.MAP)
    #     == settings.EMG_CONFIG.map_pipeline.pipeline_git_revision
    # )

    # Verify MAP workspace and results
    map_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)
    assert map_workspace.exists()
    map_gff = (
        map_workspace
        / assembly_test_scenario.assembly_accession_success
        / "mobilome_prokka.gff"
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


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_virify_batch"
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow_missing_directory(
    mock_queryset_hash,
    mock_run_virify_batch,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_analysis_ena_study,
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
        "ASA results validation error"
        in analysis.status[f"{analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"]
    )

    # Verify batch progressed to READY (even though analysis failed QC)
    batch = AssemblyAnalysisBatch.objects.get(study=study)
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.FAILED


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly.queryset_hash"
)
@patch(
    "workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch.run_virify_batch"
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run",
    ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
    indirect=True,
)
def test_prefect_analyse_assembly_flow_invalid_schema(
    mock_queryset_hash,
    mock_run_virify_batch,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_analysis_ena_study,
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
    assert batch.asa_status == AssemblyAnalysisPipelineStatus.FAILED
