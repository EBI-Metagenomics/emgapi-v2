from unittest.mock import patch

import pytest

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    CopyError,
)
from workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results import (
    copy_out_of_production_analysis_results_to_destination_folder,
    copy_out_of_production_assembly_analysis_results,
    copy_single_out_of_production_analysis_results,
)
from workflows.models import AssemblyAnalysisPipeline


@pytest.fixture
def setup_analysis(
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


@pytest.mark.django_db
class TestCopyOutOfProductionAnalysisResultsToDestinationFolder:

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_single_out_of_production_analysis_results"
    )
    def test_copy_success_returns_result(
        self,
        mock_copy_out_of_production,
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
        mock_copy_out_of_production.return_value = copy_result

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analysis_ids=[analysis.id],
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == [copy_result]
        mock_copy_out_of_production.assert_called_once()
        assert (
            mock_copy_out_of_production.call_args.kwargs["analysis_id"] == analysis.id
        )
        assert (
            mock_copy_out_of_production.call_args.kwargs["results_workspace"]
            == results_workspace
        )
        assert (
            mock_copy_out_of_production.call_args.kwargs["destination_root"]
            == tmp_path / "ftp"
        )

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_single_out_of_production_analysis_results"
    )
    def test_copy_failed_returns_result(
        self,
        mock_copy_out_of_production,
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
                    pipeline_name=AssemblyAnalysisPipeline.ASA.value,
                    source=results_workspace
                    / AssemblyAnalysisPipeline.ASA.value
                    / "ERZ000000",
                    message="ASA results are missing",
                )
            ],
        )
        mock_copy_out_of_production.return_value = copy_result

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analysis_ids=[analysis.id],
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == [copy_result]
        assert results[0].success is False
        assert results[0].errors[0].message == "ASA results are missing"

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_single_out_of_production_analysis_results"
    )
    def test_copy_processes_all_analyses(
        self,
        mock_copy_out_of_production,
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
        mock_copy_out_of_production.side_effect = copy_results

        results = copy_out_of_production_analysis_results_to_destination_folder(
            analysis_ids=[analysis.id for analysis in analyses],
            results_workspace=results_workspace,
            destination_root=tmp_path / "ftp",
        )

        assert results == copy_results
        assert mock_copy_out_of_production.call_count == 2
        for call, analysis in zip(mock_copy_out_of_production.call_args_list, analyses):
            assert call.kwargs["analysis_id"] == analysis.id
            assert call.kwargs["results_workspace"] == results_workspace


@pytest.mark.django_db
class TestCopySingleOutOfProductionAnalysisResults:
    """Direct tests of the ASA/VIRify/MAP copy logic, mocking only ``copy_schema_directories``."""

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_schema_directories"
    )
    def test_asa_copy_failure_stops_before_optional_pipelines(
        self,
        mock_copy_schema_directories,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """A failed ASA copy should return immediately without attempting VIRify/MAP."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"

        asa_error = CopyError(
            pipeline_name=AssemblyAnalysisPipeline.ASA.value,
            source=results_workspace / AssemblyAnalysisPipeline.ASA.value / "acc",
            message="ASA results are missing",
        )
        mock_copy_schema_directories.return_value = (False, [asa_error])

        result = copy_single_out_of_production_analysis_results(
            analysis_id=analysis.id,
            destination_root=tmp_path / "ftp",
            results_workspace=results_workspace,
        )

        assert result.success is False
        assert result.errors == [asa_error]
        assert mock_copy_schema_directories.call_count == 1

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_schema_directories"
    )
    def test_asa_success_with_no_optional_outputs(
        self,
        mock_copy_schema_directories,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """No VIRify/MAP directories present means no extra copy attempts, and overall success."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"
        mock_copy_schema_directories.return_value = (True, [])

        result = copy_single_out_of_production_analysis_results(
            analysis_id=analysis.id,
            destination_root=tmp_path / "ftp",
            results_workspace=results_workspace,
        )

        assert result.success is True
        assert result.errors == []
        assert mock_copy_schema_directories.call_count == 1

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_schema_directories"
    )
    def test_asa_success_virify_present_but_fails(
        self,
        mock_copy_schema_directories,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """A failed optional VIRify copy is aggregated into the result, without an ASA error."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"
        assembly_accession = analysis.assembly_or_run.first_accession

        virify_source_base = (
            results_workspace
            / AssemblyAnalysisPipeline.VIRIFY.value
            / assembly_accession
        )
        virify_source_base.mkdir(parents=True)

        virify_error = CopyError(
            pipeline_name=AssemblyAnalysisPipeline.VIRIFY.value,
            source=virify_source_base,
            message="VIRify copy failed",
        )
        mock_copy_schema_directories.side_effect = [
            (True, []),  # ASA succeeds
            (False, [virify_error]),  # VIRify fails
        ]

        result = copy_single_out_of_production_analysis_results(
            analysis_id=analysis.id,
            destination_root=tmp_path / "ftp",
            results_workspace=results_workspace,
        )

        assert result.success is False
        assert result.errors == [virify_error]

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_schema_directories"
    )
    def test_asa_success_virify_and_map_present_and_succeed(
        self,
        mock_copy_schema_directories,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Both optional pipelines present and copied successfully."""
        analysis = setup_analysis
        results_workspace = tmp_path / "results"
        assembly_accession = analysis.assembly_or_run.first_accession

        (
            results_workspace
            / AssemblyAnalysisPipeline.VIRIFY.value
            / assembly_accession
        ).mkdir(parents=True)
        (
            results_workspace / AssemblyAnalysisPipeline.MAP.value / assembly_accession
        ).mkdir(parents=True)

        mock_copy_schema_directories.return_value = (True, [])

        result = copy_single_out_of_production_analysis_results(
            analysis_id=analysis.id,
            destination_root=tmp_path / "ftp",
            results_workspace=results_workspace,
        )

        assert result.success is True
        assert mock_copy_schema_directories.call_count == 3


@pytest.mark.django_db
class TestCopyOutOfProductionAssemblyAnalysisResults:
    """Tests for the outer task: QC filtering, destination routing, and DB updates."""

    @pytest.fixture
    def setup_two_analyses(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
    ):
        ok_analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
            pipeline_version="6.0",
        )
        qc_failed_analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[1],
            pipeline_version="6.0",
        )
        qc_failed_analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
        qc_failed_analysis.save()
        return raw_reads_mgnify_study, ok_analysis, qc_failed_analysis

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_out_of_production_analysis_results_to_destination_folder"
    )
    def test_qc_failed_analyses_are_excluded_from_copy(
        self,
        mock_copy_to_destination,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_two_analyses,
        prefect_harness,
        tmp_path,
    ):
        """QC-failed analyses should never be passed to the copy step."""
        study, ok_analysis, qc_failed_analysis = setup_two_analyses
        mock_copy_to_destination.return_value = []

        copy_out_of_production_assembly_analysis_results(
            study_id=study.id,
            results_dir=tmp_path,
            analysis_ids=[ok_analysis.id, qc_failed_analysis.id],
        )

        assert mock_copy_to_destination.call_count == 2  # external + NFS
        for call in mock_copy_to_destination.call_args_list:
            assert call.kwargs["analysis_ids"] == [ok_analysis.id]

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_out_of_production_analysis_results_to_destination_folder"
    )
    def test_private_study_routes_external_copy_to_private_results_dir(
        self,
        mock_copy_to_destination,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        analysis = setup_analysis
        analysis.study.is_private = True
        analysis.study.save()
        mock_copy_to_destination.return_value = []

        copy_out_of_production_assembly_analysis_results(
            study_id=analysis.study.id,
            results_dir=tmp_path,
            analysis_ids=[analysis.id],
        )

        external_call = mock_copy_to_destination.call_args_list[0]
        assert (
            external_call.kwargs["destination_root"]
            == EMG_CONFIG.slurm.private_results_dir
        )

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_out_of_production_analysis_results_to_destination_folder"
    )
    def test_public_study_routes_external_copy_to_ftp_results_dir(
        self,
        mock_copy_to_destination,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        analysis = setup_analysis
        analysis.study.is_private = False
        analysis.study.save()
        mock_copy_to_destination.return_value = []

        copy_out_of_production_assembly_analysis_results(
            study_id=analysis.study.id,
            results_dir=tmp_path,
            analysis_ids=[analysis.id],
        )

        external_call = mock_copy_to_destination.call_args_list[0]
        assert (
            external_call.kwargs["destination_root"] == EMG_CONFIG.slurm.ftp_results_dir
        )

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_out_of_production_analysis_results_to_destination_folder"
    )
    def test_both_external_and_nfs_destinations_are_copied_to(
        self,
        mock_copy_to_destination,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Both the external (FTP/private) and NFS destinations should be attempted."""
        analysis = setup_analysis
        mock_copy_to_destination.return_value = []

        copy_out_of_production_assembly_analysis_results(
            study_id=analysis.study.id,
            results_dir=tmp_path,
            analysis_ids=[analysis.id],
        )

        assert mock_copy_to_destination.call_count == 2
        external_call, nfs_call = mock_copy_to_destination.call_args_list
        assert (
            external_call.kwargs["destination_root"]
            != nfs_call.kwargs["destination_root"]
        )
        assert nfs_call.kwargs["destination_root"] == tmp_path / "results"

    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.copy_out_of_production_assembly_analysis_results.copy_out_of_production_analysis_results_to_destination_folder"
    )
    def test_study_external_results_dir_is_set_after_copy(
        self,
        mock_copy_to_destination,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_analysis,
        prefect_harness,
        tmp_path,
    ):
        analysis = setup_analysis
        study = analysis.study
        assert study.external_results_dir in (None, "")
        mock_copy_to_destination.return_value = []

        copy_out_of_production_assembly_analysis_results(
            study_id=study.id,
            results_dir=tmp_path,
            analysis_ids=[analysis.id],
        )

        study.refresh_from_db()
        expected_prefix = str(
            accession_prefix_separated_dir_path(study.first_accession, -3)
        )
        assert study.external_results_dir == expected_prefix
