import re
from pathlib import Path
from typing import List

import pytest
from django.conf import settings

from analyses.models import Study as AnalysisStudy
from workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results import (
    _validate_results_structure,
    import_out_of_production_assembly_analysis_results,
)
from workflows.prefect_utils.testing_utils import (
    generate_assembly_v6_pipeline_results,
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)
from workflows.tests.test_analysis_assembly_study_flow import (
    setup_map_batch_fixtures,
    setup_virify_batch_fixtures,
)

EMG_CONFIG = settings.EMG_CONFIG


# ============================================================================
# Unit Tests
# ============================================================================


@pytest.mark.django_db
class TestImportOutOfProductionAssemblyAnalysisResults:

    def test_validate_results_structure_missing_pipeline_dir_error(
        self, tmp_path, prefect_harness
    ):
        """Test warning when pipeline directories are missing."""
        accession = "ERZ18440741"
        results_path = tmp_path / "results"
        results_path.mkdir()
        for pipeline in ["asa", "virify"]:  # MAP directory is missing
            pipeline_path = results_path / pipeline / accession
            pipeline_path.mkdir(parents=True, exist_ok=True)

        with pytest.raises(
            FileNotFoundError, match="Pipeline directory map not found "
        ):
            _validate_results_structure(results_path, [accession])


# ============================================================================
# Setup Functions
# ============================================================================


def setup_fixtures_of_completed_assembly_analysis(assembly_test_scenario):
    """Create files and folders to simulate completed ASA, VIRify, and MAP analyses."""

    # Find a way to define some tmp folder
    workspace = assembly_test_scenario.workspace_dir
    # Create input fasta files
    fasta_dir = workspace / "assembly_fastas"
    create_input_fasta_files(
        fasta_dir,
        [
            assembly_test_scenario.assembly_accession_success,
        ],
    )
    # Create the ASA samplesheet
    samplesheet_dir = workspace / "samplesheets"
    samplesheet_dir.mkdir(parents=True, exist_ok=True)
    samplesheet_path = samplesheet_dir / "assembly_samplesheet.csv"
    create_samplesheet_with_assemblies(
        samplesheet_path,
        fasta_dir,
        [
            assembly_test_scenario.assembly_accession_success,
        ],
    )
    # Generate ASA pipeline results
    generate_assembly_v6_pipeline_results(
        asa_workspace=workspace / "asa",
        assemblies=[(assembly_test_scenario.assembly_accession_success, "success")],
        copy_from_fixtures=assembly_test_scenario.fixture_source_dir,
    )
    # Generate MAP and Virify pipeline results
    setup_virify_batch_fixtures(workspace / "virify", assembly_test_scenario)
    setup_map_batch_fixtures(workspace / "map", assembly_test_scenario)

    return workspace, samplesheet_path


def create_samplesheet_with_assemblies(
    samplesheet_path: Path, fasta_dir: Path, assembly_accessions: List[str]
):
    """Create a samplesheet CSV file with given assembly accessions."""
    with samplesheet_path.open("w") as f:
        f.write(
            "sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
        )
        for acc in assembly_accessions:
            fasta_path = fasta_dir / f"{acc}.fa.gz"
            f.write(f"{acc},{fasta_path},,,\n")


def create_input_fasta_files(fasta_dir: Path, assembly_accessions: List[str]):
    """Create dummy input fasta files for given assembly accessions."""
    for acc in assembly_accessions:
        fasta_path = fasta_dir / f"{acc}.fa.gz"
        fasta_path.parent.mkdir(parents=True, exist_ok=True)
        with fasta_path.open("w") as f:
            f.write(f">Dummy contig for {acc}\n")
            f.write("ATGC" * 10 + "\n")


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.django_db
class TestImportOutOfProductionAssemblyAnalysisResultsRealData:
    """Integration tests using real data from data/ folder."""

    @pytest.mark.django_db(transaction=True)
    @pytest.mark.httpx_mock(
        should_mock=should_not_mock_httpx_requests_to_prefect_server
    )
    @pytest.mark.parametrize(
        "mock_suspend_flow_run",
        ["workflows.flows.analysis.assembly.flows.analysis_assembly_study"],
        indirect=True,
    )
    @pytest.mark.prefect_harness
    def test_import_out_of_production_assembly_analysis_results_real_data(
        self,
        admin_user,
        httpx_mock,
        mock_suspend_flow_run,
        prefect_harness,
        mocker,
        top_level_biomes,
        assembly_test_scenario,
        biome_choices,
        user_choices,
        analyse_study_input_mocker,
    ):
        """
        Test complete import_out_of_production_assembly_analysis_results flow.

        What is tested here:
        1. Parsing of the samplesheet
        2. Validation of the results directory structure
        3. Creates Study, Analysis and related objects for each assembly
        4. Processes ASA/VIRify/MAP results
        5. Creates study summaries

        What is not tested here:
        - Actual ENA API calls (these are mocked)
        - Actual copying of results to production (it is mocked)

        Note: This test requires:
        - Database access to store results
        - Prefect context for flow execution
        """

        mocked_results_dir, samplesheet_path = (
            setup_fixtures_of_completed_assembly_analysis(assembly_test_scenario)
        )

        # Mock ENA response to fetch study accession
        httpx_mock.add_response(
            url=re.compile(
                f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(assembly_test_scenario.assembly_accession_success)}.*"
            ),
            json=[
                {
                    "study_accession": assembly_test_scenario.study_accession,
                },
            ],
        )

        # Mock ENA response for study fetching
        httpx_mock.add_response(
            url=re.compile(
                f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=study&query=.*{re.escape(assembly_test_scenario.study_accession)}.*"
            ),
            json=[
                {
                    "study_accession": assembly_test_scenario.study_accession,
                    "secondary_study_accession": assembly_test_scenario.study_secondary,
                    "study_title": "Metagenomic sequencing of honey bee samples",
                },
            ],
            is_reusable=True,
        )

        # Mock ENA response for fetching assemblies
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

        # Mock ENA response for fetching samples
        httpx_mock.add_response(
            url=re.compile(
                f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=sample&query=.*{re.escape(assembly_test_scenario.sample_accession)}.*"
            ),
            json=[
                {
                    "sample_accession": assembly_test_scenario.sample_accession,
                    "sample_title": "my data",
                    "secondary_sample_accession": assembly_test_scenario.sample_accession,
                    "host": "Homo sapiens",
                    "center_name": "Harvard T.H. Chan School of Public Health",
                },
            ],
        )

        # Mock ENA response for fetching runs
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
            ],
        )

        def suspend_side_effect(wait_for_input=None, **kwargs):
            if wait_for_input.__name__ == "AnalyseStudyInput":
                return analyse_study_input_mocker(
                    biome=biome_choices[assembly_test_scenario.biome_path],
                    watchers=[user_choices[admin_user.username]],
                    webin_owner=None,
                )

        mock_suspend_flow_run.side_effect = suspend_side_effect

        mock_copy_external = mocker.patch(
            "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results.copy_out_of_production_assembly_analysis_results"
        )
        mock_copy_summaries = mocker.patch(
            "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results.copy_v6_study_summaries"
        )

        # Wish me luck...
        try:
            result = run_flow_and_capture_logs(
                import_out_of_production_assembly_analysis_results,
                results_dir=str(mocked_results_dir),
                samplesheet_path=str(samplesheet_path),
            )
            print(
                "✓ import_out_of_production_assembly_analysis_results flow completed successfully"
            )
        except Exception as e:
            print(f"✗ Flow failed with error: {e}")
            raise

        # only to make linter happy
        mock_copy_external.assert_called_once()
        mock_copy_summaries.assert_called_once()
        assert result is not None

        # TODO: add more assertions on flow logs and database state
        # Verify study was created/updated
        study = AnalysisStudy.objects.filter(
            ena_study__accession__in=[assembly_test_scenario.study_accession]
        ).first()
        assert study is not None, "Study was not created in the database"
        assert study.results_dir == str(mocked_results_dir)
        assert study.biome is not None, "Study biome was not set"

        analysis = study.analyses.filter(
            assembly__ena_accessions__contains=[
                assembly_test_scenario.assembly_accession_success
            ]
        ).first()
        assert analysis is not None, "Analysis for assembly was not created"
