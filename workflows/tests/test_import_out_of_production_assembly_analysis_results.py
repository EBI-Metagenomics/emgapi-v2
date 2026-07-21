import re
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import Analysis
from analyses.models import Study as AnalysisStudy
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    CopyError,
)
from workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results import (
    _parse_and_validate_samplesheet,
    _validate_results_structure,
    copy_out_of_production_analysis_results_to_destination_folder,
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

    def test_flow_raises_when_results_dir_missing(self, tmp_path, prefect_harness):
        """Flow should fail fast if the results directory does not exist."""
        missing_results_dir = tmp_path / "does-not-exist"
        samplesheet_path = tmp_path / "samplesheet.csv"
        samplesheet_path.write_text(
            "sample,assembly_fasta\nERZ18440741,/path/to/ERZ18440741.fa.gz\n"
        )

        with pytest.raises(FileNotFoundError, match="Results directory does not exist"):
            import_out_of_production_assembly_analysis_results(
                results_dir=str(missing_results_dir),
                samplesheet_path=str(samplesheet_path),
            )

    def test_flow_raises_when_samplesheet_missing(self, tmp_path, prefect_harness):
        """Flow should fail fast if the samplesheet file does not exist."""
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        missing_samplesheet_path = tmp_path / "does-not-exist.csv"

        with pytest.raises(FileNotFoundError, match="Samplesheet file does not exist"):
            import_out_of_production_assembly_analysis_results(
                results_dir=str(results_dir),
                samplesheet_path=str(missing_samplesheet_path),
            )

    def test_parse_and_validate_samplesheet_missing_required_columns_error(
        self, tmp_path, prefect_harness
    ):
        """Test error when the samplesheet is missing a required column."""
        samplesheet_path = tmp_path / "samplesheet.csv"
        samplesheet_path.write_text(
            "sample,contaminant_reference,human_reference,phix_reference\n"
            "ERZ18440741,,,\n"
        )

        with pytest.raises(ValueError, match="missing required columns"):
            _parse_and_validate_samplesheet(samplesheet_path)

    def test_parse_and_validate_samplesheet_no_assembly_accessions_error(
        self, tmp_path, prefect_harness
    ):
        """Test error when the samplesheet has no rows with a sample accession."""
        samplesheet_path = tmp_path / "samplesheet.csv"
        samplesheet_path.write_text(
            "sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
            ",/path/to/some.fa.gz,,,\n"
        )

        with pytest.raises(ValueError, match="No assembly accessions found"):
            _parse_and_validate_samplesheet(samplesheet_path)


@pytest.mark.django_db
class TestCopyOutOfProductionAnalysisResultsToDestinationFolder:

    @pytest.fixture
    def setup_analysis(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
    ):
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
            pipeline_version="6.0",
        )
        return analysis

    @patch(
        "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results.copy_single_analysis_results"
    )
    def test_copy_success_returns_result(
        self,
        mock_copy_single,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Test that a successful copy is returned for the analysis."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"

        copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "ftp" / "analysis",
            success=True,
        )
        mock_copy_single.return_value = copy_result

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analyses=[analysis],
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == [copy_result]
        mock_copy_single.assert_called_once()
        assert mock_copy_single.call_args.kwargs["analysis"] == analysis
        assert (
            mock_copy_single.call_args.kwargs["results_workspace"] == results_workspace
        )
        assert mock_copy_single.call_args.kwargs["destination_root"] == tmp_path / "ftp"
        # Out-of-production copies do not go through the batch context
        assert "batch" not in mock_copy_single.call_args.kwargs
        assert "batch_analysis_job" not in mock_copy_single.call_args.kwargs

    @patch(
        "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results.copy_single_analysis_results"
    )
    def test_copy_failed_returns_result(
        self,
        mock_copy_single,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Test that a failed copy is returned rather than raised."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"

        copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "ftp" / "analysis",
            success=False,
            errors=[
                CopyError(
                    pipeline_name="asa",
                    source=results_workspace / "asa" / "ERZ000000",
                    message="ASA results are missing",
                )
            ],
        )
        mock_copy_single.return_value = copy_result

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analyses=[analysis],
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == [copy_result]
        assert results[0].success is False
        assert results[0].errors[0].message == "ASA results are missing"

    @patch(
        "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results.copy_single_analysis_results"
    )
    def test_copy_processes_all_analyses(
        self,
        mock_copy_single,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        prefect_harness,
        tmp_path,
    ):
        """Test that one copy result is returned per analysis, preserving order."""
        analyses = [
            Analysis.objects.create(
                study=raw_reads_mgnify_study,
                sample=raw_reads_mgnify_sample[0],
                ena_study=raw_reads_mgnify_study.ena_study,
                assembly=assembly,
                pipeline_version="6.0",
            )
            for assembly in mgnify_assemblies[:2]
        ]
        results_workspace = tmp_path / "results"

        copy_results = [
            BatchCopyResult(
                analysis_id=analysis.id,
                destination_folder=tmp_path / "ftp" / str(analysis.id),
                success=True,
            )
            for analysis in analyses
        ]
        mock_copy_single.side_effect = copy_results

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analyses=analyses,
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == copy_results
        assert mock_copy_single.call_count == 2
        for call, analysis in zip(mock_copy_single.call_args_list, analyses):
            assert call.kwargs["analysis"] == analysis
            assert call.kwargs["results_workspace"] == results_workspace


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
        [
            "workflows.flows.analysis.assembly.flows.import_out_of_production_assembly_analysis_results"
        ],
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
            if wait_for_input.__name__ == "ImportAssemblyAnalysisInput":
                return analyse_study_input_mocker(
                    biome=biome_choices[assembly_test_scenario.biome_path],
                    watchers=[user_choices[admin_user.username]],
                    webin_owner=None,
                )
            return None

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

    @pytest.mark.django_db(transaction=True)
    @pytest.mark.httpx_mock(
        should_mock=should_not_mock_httpx_requests_to_prefect_server
    )
    @pytest.mark.prefect_harness
    def test_import_out_of_production_assembly_analysis_results_multiple_studies_error(
        self, httpx_mock, prefect_harness, tmp_path
    ):
        """Flow should refuse a samplesheet whose assemblies belong to different studies."""
        assembly_accession_1 = "ERZ18440741"
        assembly_accession_2 = "ERZ18440742"
        study_accession_1 = "PRJEB00001"
        study_accession_2 = "PRJEB00002"

        results_dir = tmp_path / "results"
        for pipeline in ["asa", "virify", "map"]:
            for accession in [assembly_accession_1, assembly_accession_2]:
                (results_dir / pipeline / accession).mkdir(parents=True, exist_ok=True)

        samplesheet_path = tmp_path / "samplesheet.csv"
        samplesheet_path.write_text(
            "sample,assembly_fasta\n"
            f"{assembly_accession_1},/path/to/{assembly_accession_1}.fa.gz\n"
            f"{assembly_accession_2},/path/to/{assembly_accession_2}.fa.gz\n"
        )

        httpx_mock.add_response(
            url=re.compile(
                f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(assembly_accession_1)}.*"
            ),
            json=[{"study_accession": study_accession_1}],
        )
        httpx_mock.add_response(
            url=re.compile(
                f"{re.escape(EMG_CONFIG.ena.portal_search_api)}\\?result=analysis&query=.*{re.escape(assembly_accession_2)}.*"
            ),
            json=[{"study_accession": study_accession_2}],
        )

        with pytest.raises(
            ValueError,
            match="Samplesheet contains assemblies from multiple studies",
        ):
            import_out_of_production_assembly_analysis_results(
                results_dir=str(results_dir),
                samplesheet_path=str(samplesheet_path),
            )
