from enum import Enum
from typing import List
import re
import uuid
import shutil
from pathlib import Path

import pytest
from pydantic import BaseModel
from django.conf import settings

from analyses.models import Study as AnalysisStudy
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)
from workflows.tests.test_analysis_assembly_study_flow import AssemblyTestScenario
from workflows.flows.analysis.assembly.flows.external_assembly_ingestion import (
    _parse_and_validate_samplesheet,
    _validate_results_structure,
    external_assembly_ingestion,
)

EMG_CONFIG = settings.EMG_CONFIG


# ============================================================================
# Unit Tests
# ============================================================================


@pytest.mark.django_db
class TestExternalAssemblyIngestion:

    def test_parse_samplesheet_valid(self, tmp_path, prefect_harness):
        """Test parsing a valid samplesheet."""
        samplesheet = tmp_path / "samplesheet.csv"
        samplesheet.write_text(
            "sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
            "ERZ18440741,/path/to/ERZ18440741.fa.gz,ref1,ref2,ref3\n"
            "ERZ18545484,/path/to/ERZ18545484.fa.gz,ref1,ref2,ref3\n"
        )

        result = _parse_and_validate_samplesheet(samplesheet)
        assert result == ["ERZ18440741", "ERZ18545484"]

    def test_parse_samplesheet_empty_raises_error(self, tmp_path, prefect_harness):
        """Test that empty samplesheet raises ValueError."""
        samplesheet = tmp_path / "samplesheet.csv"
        samplesheet.write_text(
            "sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
        )

        with pytest.raises(ValueError, match="No assembly accessions"):
            _parse_and_validate_samplesheet(samplesheet)

    def test_parse_samplesheet_missing_columns_warns(
        self, tmp_path, caplog, prefect_harness
    ):
        """Test warning when expected columns are missing."""
        samplesheet = tmp_path / "samplesheet.csv"
        samplesheet.write_text(
            "sample,assembly_fasta\n"  # Missing required columns
            "ERZ18440741,/path/to/file.fa.gz\n"
        )

        result = _parse_and_validate_samplesheet(samplesheet)
        assert "ERZ18440741" in result
        assert "may not match expected" in caplog.text

    def test_validate_results_structure_missing_pipeline_warns(
        self, tmp_path, caplog, prefect_harness
    ):
        """Test warning when pipeline directories are missing."""
        results_path = tmp_path / "results"
        results_path.mkdir()

        _validate_results_structure(results_path, ["ERZ18440741"])

        assert "not found" in caplog.text.lower()


# ============================================================================
# Setup Functions
# ============================================================================


@pytest.fixture
def test_workspace():
    """Create a temporary workspace directory for test execution."""
    # Create a unique workspace under /app/data/tests/tmp/
    base_tmp = Path("/Users/sofia/mgnify/emgapi-v2/tmp")
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
        assembly_accession_success="ERZ26878882",
        assembly_accession_failed="ERZ857108",
        sample_accession="SAMN08514017",
        run_accession="SRR123456",
        fixture_source_dir=Path("/Users/sofia/mgnify/emgapi-v2/data"),
        workspace_dir=test_workspace,
        biome_path="root.engineered",
        biome_name="Engineered",
    )


# def setup_mock_asa_results(results_path: Path, assembly_accession: str):
#     """Create minimal mock ASA results for testing."""
#     asa_dir = results_path / "asa" / assembly_accession
#     asa_dir.mkdir(parents=True, exist_ok=True)

#     # Create required subdirectories
#     (asa_dir / "quality_control").mkdir()
#     (asa_dir / "taxonomy").mkdir()
#     (asa_dir / "annotation_summary").mkdir()
#     (asa_dir / EMG_CONFIG.assembly_analysis_pipeline.cds_folder).mkdir()
#     (asa_dir / EMG_CONFIG.assembly_analysis_pipeline.qc_folder).mkdir()

#     # Create required files
#     cds_gff = (
#         asa_dir
#         / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
#         / f"{assembly_accession}_predicted_cds.gff.gz"
#     )
#     with gzip.open(cds_gff, "wt") as f:
#         f.write("##gff-version 3\n")

#     qc_fasta = (
#         asa_dir
#         / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
#         / f"{assembly_accession}_filtered_contigs.fasta.gz"
#     )
#     with gzip.open(qc_fasta, "wt") as f:
#         f.write(">contig1\nACGTACGT\n")

#     # Create end-of-run CSVs
#     (results_path / "asa" / "analysed_assemblies.csv").write_text(
#         f"{assembly_accession},completed"
#     )

# def setup_mock_virify_results(results_path: Path, assembly_accession: str):
#     """Create minimal mock VIRify results for testing."""
#     virify_dir = results_path / "virify" / assembly_accession
#     gff_dir = virify_dir / EMG_CONFIG.virify_pipeline.final_gff_folder
#     gff_dir.mkdir(parents=True, exist_ok=True)

#     # Create VIRify GFF file
#     virify_gff = gff_dir / f"{assembly_accession}_virify.gff.gz"
#     with gzip.open(virify_gff, "wt") as f:
#         f.write("##gff-version 3\n")

#     # Create index files
#     virify_gff.with_suffix(".gz.gzi").touch()
#     virify_gff.with_suffix(".gz.csi").touch()


# def setup_mock_map_results(results_path: Path, assembly_accession: str):
#     """Create minimal mock MAP results for testing."""
#     map_dir = results_path / "map" / assembly_accession
#     gff_dir = map_dir / EMG_CONFIG.map_pipeline.final_gff_folder
#     gff_dir.mkdir(parents=True, exist_ok=True)

#     # Create MAP GFF file
#     map_gff = gff_dir / f"{assembly_accession}_user_mobilome_full.gff.gz"
#     with gzip.open(map_gff, "wt") as f:
#         f.write("##gff-version 3\n")

#     # Create index files
#     map_gff.with_suffix(".gz.gzi").touch()
#     map_gff.with_suffix(".gz.csi").touch()


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.django_db
class TestExternalAssemblyIngestionRealData:
    """Integration tests using real data from data/ folder."""

    @pytest.fixture
    def data_dir(self):
        """Get path to real data directory."""
        return Path(settings.BASE_DIR) / "data"

    @pytest.fixture
    def samplesheet_path(self, data_dir):
        """Get path to real samplesheet."""
        return data_dir / "samplesheets" / "PRJEB79850_samplesheet_assembly-v6.csv"

    @pytest.fixture
    def results_dir(self, data_dir):
        """Get path to real results directory."""
        return data_dir

    @pytest.mark.django_db(transaction=True)
    @pytest.mark.httpx_mock(
        should_mock=should_not_mock_httpx_requests_to_prefect_server
    )
    @pytest.mark.parametrize(
        "mock_suspend_flow_run",
        ["workflows.flows.analysis.assembly.flows.external_assembly_ingestion"],
        indirect=True,
    )
    @pytest.mark.prefect_harness
    def test_external_assembly_ingestion_real_data(
        self,
        samplesheet_path,
        results_dir,
        assembly_test_scenario,
        admin_user,
        httpx_mock,
        mock_suspend_flow_run,
        prefect_harness,
        mocker,
        top_level_biomes,
    ):
        """
        Test complete external_assembly_ingestion flow with real data.

        This is a full integration test that:
        1. Parses the real samplesheet
        2. Validates the results directory structure
        3. Fetches study and assembly data from ENA (mocked)
        4. Creates Analysis objects for each assembly
        5. Processes ASA/VIRify/MAP results
        6. Creates study summaries
        7. Copies results to production locations (mocked)

        Note: This test requires:
        - Database access to store results
        - Prefect context for flow execution
        - Mocked ENA API responses
        - Mock for suspending the flow to pick biome/watchers

        What is not tested here:
        - Actual ENA API calls (these are mocked)
        - Actual copying of results to production (these are mocked)
        """
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

        # Set biome for the study
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
            return AnalyseStudyInput(
                biome=BiomeChoices[assembly_test_scenario.biome_path],
                watchers=[UserChoices[admin_user.username]],
            )

        mock_suspend_flow_run.side_effect = suspend_side_effect

        mock_copy_external = mocker.patch(
            "workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_external_assembly_analysis_results"
        )
        mock_copy_summaries = mocker.patch(
            "workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_v6_study_summaries"
        )

        # Wish me luck...
        try:
            result = run_flow_and_capture_logs(
                external_assembly_ingestion,
                results_dir=str(results_dir),
                samplesheet_path=str(samplesheet_path),
            )
            print("✓ external_assembly_ingestion flow completed successfully")
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
        assert study.results_dir == str(results_dir)
        assert study.biome is not None, "Study biome was not set"

        analysis = study.analyses.filter(
            assembly__ena_accessions__contains=[
                assembly_test_scenario.assembly_accession_success
            ]
        ).first()
        assert analysis is not None, "Analysis for assembly was not created"


# @pytest.mark.django_db
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_accession_for_assembly")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_from_ena")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_assemblies_from_ena")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.process_external_pipeline_results")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.generate_assembly_analysis_pipeline_summary")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.add_assembly_study_summaries_to_downloads")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_v6_study_summaries")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_external_assembly_analysis_results")
# def test_ingestion_flow_with_existing_biome(
#     mock_copy_results,
#     mock_copy_v6_summaries,
#     mock_add_summaries,
#     mock_gen_summary,
#     mock_process_results,
#     mock_get_assemblies_ena,
#     mock_get_study_ena,
#     mock_get_study_accession,
#     tmp_path,
#     raw_reads_mgnify_study,
#     mgnify_assemblies,
#     prefect_harness,
#     assembly_analysis_ena_study,
# ):
#     """Test flow completes successfully when biome already set."""
#     # Setup
#     study_acc = "PRJEB24849"
#     assembly_acc = "ERZ857107"
#     biome, _ = Biome.objects.get_or_create(path="root.engineered", defaults={"biome_name": "Engineered"})
#     raw_reads_mgnify_study.biome = biome
#     raw_reads_mgnify_study.save()

#     # Create samplesheet
#     samplesheet = tmp_path / "samplesheet.csv"
#     samplesheet.write_text(
#         f"sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
#         f"{assembly_acc},/path/to/file.fa.gz,ref1,ref2,ref3\n"
#     )

#     # Create results directory
#     results_dir = tmp_path / "results"
#     for pipeline in ["asa", "virify", "map"]:
#         (results_dir / pipeline).mkdir(parents=True)

#     setup_mock_asa_results(results_dir, assembly_acc)

#     # Mock return values
#     mock_get_study_accession.return_value = study_acc
#     mock_get_study_ena.return_value = assembly_analysis_ena_study

#     # Run flow
#     external_assembly_ingestion(
#         results_dir=str(results_dir),
#         samplesheet_path=str(samplesheet),
#     )

#     # Verify key functions were called
#     mock_process_results.assert_called()
#     mock_gen_summary.assert_called_once()
#     mock_add_summaries.assert_called_once()
#     mock_copy_v6_summaries.assert_called_once()
#     mock_copy_results.assert_called_once()


# @pytest.mark.django_db
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_accession_for_assembly")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_from_ena")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.get_study_assemblies_from_ena")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.process_external_pipeline_results")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.generate_assembly_analysis_pipeline_summary")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.add_assembly_study_summaries_to_downloads")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_v6_study_summaries")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.copy_external_assembly_analysis_results")
# @patch("workflows.flows.analysis.assembly.flows.external_assembly_ingestion.suspend_flow_run")
# def test_ingestion_flow_biome_picker_required(
#     mock_suspend_flow_run,
#     mock_copy_results,
#     mock_copy_v6_summaries,
#     mock_add_summaries,
#     mock_gen_summary,
#     mock_process_results,
#     mock_get_assemblies_ena,
#     mock_get_study_ena,
#     mock_get_study_accession,
#     tmp_path,
#     raw_reads_mgnify_study,
#     mgnify_assemblies,
#     prefect_harness,
#     assembly_analysis_ena_study,
# ):
#     """Test flow requests biome when not set."""
#     study_acc = "PRJEB24849"
#     assembly_acc = "ERZ857107"

#     # Create samplesheet and results
#     samplesheet = tmp_path / "samplesheet.csv"
#     samplesheet.write_text(
#         f"sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\n"
#         f"{assembly_acc},/path/to/file.fa.gz,ref1,ref2,ref3\n"
#     )

#     results_dir = tmp_path / "results"
#     for pipeline in ["asa", "virify", "map"]:
#         (results_dir / pipeline).mkdir(parents=True)

#     setup_mock_asa_results(results_dir, assembly_acc)

#     # Mock return values
#     mock_get_study_accession.return_value = study_acc
#     mock_get_study_ena.return_value = assembly_analysis_ena_study

#     BiomeChoices = Enum("BiomeChoices", {"root.engineered": f"Root:Engineered"})
#     UserChoices = get_users_as_choices()

#     class AnalyseStudyInput(BaseModel):
#         biome: BiomeChoices
#         watchers: List[UserChoices]

#     def suspend_side_effect(wait_for_input=None):
#         if wait_for_input.__name__ == "AnalyseStudyInput":
#             return AnalyseStudyInput(
#                 biome=BiomeChoices["root.engineered"],
#                 watchers=[UserChoices["admin"]],
#             )

#     mock_suspend_flow_run.side_effect = suspend_side_effect

#     # Run flow
#     external_assembly_ingestion(
#         results_dir=str(results_dir),
#         samplesheet_path=str(samplesheet),
#     )

#     # Verify suspend was called (biome picker)
#     mock_suspend_flow_run.assert_called_once()

#     # Verify study biome was set
#     raw_reads_mgnify_study.refresh_from_db()
#     assert raw_reads_mgnify_study.biome.path == "root.engineered"


# @pytest.mark.django_db
# def test_ingestion_missing_results_directory_raises_error(tmp_path):
#     """Test that missing results directory raises FileNotFoundError."""
#     samplesheet = tmp_path / "samplesheet.csv"
#     samplesheet.write_text("sample,assembly_fasta,contaminant_reference,human_reference,phix_reference\nERZ1,/path,ref1,ref2,ref3\n")

#     with pytest.raises(FileNotFoundError, match="Results directory does not exist"):
#         external_assembly_ingestion(
#             results_dir="/nonexistent/path",
#             samplesheet_path=str(samplesheet),
#         )


# @pytest.mark.django_db
# def test_ingestion_missing_samplesheet_raises_error(tmp_path):
#     """Test that missing samplesheet raises error."""
#     results_dir = tmp_path / "results"
#     results_dir.mkdir()

#     with pytest.raises(FileNotFoundError):
#         external_assembly_ingestion(
#             results_dir=str(results_dir),
#             samplesheet_path="/nonexistent/samplesheet.csv",
#         )
