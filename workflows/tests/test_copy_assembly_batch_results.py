from pathlib import Path
from unittest.mock import patch

import pytest

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis
from workflows.data_io_utils.file_rules.common_rules import DirectoryExistsRule
from workflows.data_io_utils.schemas.base import PipelineDirectorySchema
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    CopyError,
    copy_assembly_batch_results_to_destination_folder,
    copy_schema_directory,
)
from workflows.flows.analysis.assembly.flows.sync_assembly_batch_results import (
    copy_assembly_batch_results as sync_assembly_batch_results,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)


@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def test_copy_schema_directory_uses_external_folder_name(
    mock_run_deployment,
    tmp_path,
    prefect_harness,
):
    source_base = tmp_path / "source"
    destination_base = tmp_path / "destination"
    (source_base / "pipeline-folder").mkdir(parents=True)

    success, errors = copy_schema_directory(
        directory_schema=PipelineDirectorySchema(
            folder_name="pipeline-folder",
            external_folder_name="external-folder/gff",
            validation_rules=[DirectoryExistsRule],
        ),
        source_base=source_base,
        destination_base=destination_base,
        timeout=30,
    )

    assert success
    assert errors == []
    assert mock_run_deployment.call_count == 1
    assert mock_run_deployment.call_args.kwargs["parameters"]["target"] == str(
        destination_base / "external-folder/gff"
    )


@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def test_copy_schema_directory_stops_at_external_folder_name(
    mock_run_deployment,
    tmp_path,
    prefect_harness,
):
    source_base = tmp_path / "source"
    destination_base = tmp_path / "destination"
    (source_base / "parent" / "child").mkdir(parents=True)

    success, errors = copy_schema_directory(
        directory_schema=PipelineDirectorySchema(
            folder_name="parent",
            external_folder_name="external-parent",
            validation_rules=[DirectoryExistsRule],
            subdirectories=[
                PipelineDirectorySchema(
                    folder_name="child",
                    external_folder_name="external-child",
                    validation_rules=[DirectoryExistsRule],
                )
            ],
        ),
        source_base=source_base,
        destination_base=destination_base,
        timeout=30,
    )

    assert success
    assert errors == []
    assert mock_run_deployment.call_count == 1
    assert mock_run_deployment.call_args.kwargs["parameters"]["target"] == str(
        destination_base / "external-parent"
    )


@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def test_copy_schema_directory_skips_unmapped_container(
    mock_run_deployment,
    tmp_path,
    prefect_harness,
):
    source_base = tmp_path / "source"
    destination_base = tmp_path / "destination"
    (source_base / "parent" / "child").mkdir(parents=True)

    success, errors = copy_schema_directory(
        directory_schema=PipelineDirectorySchema(
            folder_name="parent",
            validation_rules=[DirectoryExistsRule],
            subdirectories=[
                PipelineDirectorySchema(
                    folder_name="child",
                    external_folder_name="external-child",
                    validation_rules=[DirectoryExistsRule],
                )
            ],
        ),
        source_base=source_base,
        destination_base=destination_base,
        timeout=30,
    )

    assert success
    assert errors == []
    assert mock_run_deployment.call_count == 1
    assert (
        mock_run_deployment.call_args.kwargs["parameters"]["source"]
        == str(source_base / "parent" / "child") + "/"
    )
    assert mock_run_deployment.call_args.kwargs["parameters"]["target"] == str(
        destination_base / "external-child"
    )


@pytest.mark.django_db
class TestCopyAssemblyBatchResults:

    @pytest.fixture
    def setup_batch_and_analysis(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
    ):
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
            pipeline_version="6.0",
        )
        batch_analysis_relation = AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )
        return batch, analysis, batch_analysis_relation

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.copy_single_analysis_results"
    )
    def test_copy_failed_returns_result(
        self,
        mock_copy_single,
        setup_batch_and_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Test that copy failures are returned without updating analysis status."""
        batch, analysis, _batch_analysis_relation = setup_batch_and_analysis

        copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "ftp" / "analysis",
            success=False,
            errors=[
                CopyError(
                    pipeline_name="asa",
                    source=tmp_path / "workspace" / "asa",
                    message="Copy failed!",
                )
            ],
        )
        mock_copy_single.return_value = copy_result

        results = copy_assembly_batch_results_to_destination_folder(
            batch.id,
            destination_root=tmp_path / "ftp",
        )

        analysis.refresh_from_db()
        assert results == [copy_result]
        assert not analysis.status[
            Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
        ]
        assert (
            mock_copy_single.call_args.kwargs["batch_analysis_job"].virify_status
            != AssemblyAnalysisPipelineStatus.COMPLETED
        )
        assert (
            mock_copy_single.call_args.kwargs["batch_analysis_job"].map_status
            != AssemblyAnalysisPipelineStatus.COMPLETED
        )

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.copy_single_analysis_results"
    )
    def test_copy_success_returns_result(
        self,
        mock_copy_single,
        setup_batch_and_analysis,
        prefect_harness,
        tmp_path,
    ):
        """Test that successful copies are returned without updating analysis status."""
        batch, analysis, batch_analysis_relation = setup_batch_and_analysis
        batch_analysis_relation.virify_status = AssemblyAnalysisPipelineStatus.COMPLETED
        batch_analysis_relation.map_status = AssemblyAnalysisPipelineStatus.COMPLETED
        batch_analysis_relation.save()

        analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
        analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] = True
        analysis.save()

        copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "ftp" / "analysis",
            success=True,
        )
        mock_copy_single.return_value = copy_result

        results = copy_assembly_batch_results_to_destination_folder(
            batch.id,
            destination_root=tmp_path / "ftp",
        )

        analysis.refresh_from_db()
        assert results == [copy_result]
        assert analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] is True
        assert analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] is True

        mock_copy_single.assert_called_once()
        assert mock_copy_single.call_args.kwargs["destination_root"] == tmp_path / "ftp"
        assert (
            mock_copy_single.call_args.kwargs["batch_analysis_job"].virify_status
            == AssemblyAnalysisPipelineStatus.COMPLETED
        )
        assert (
            mock_copy_single.call_args.kwargs["batch_analysis_job"].map_status
            == AssemblyAnalysisPipelineStatus.COMPLETED
        )

    @patch(
        "workflows.flows.analysis.assembly.flows.sync_assembly_batch_results.update_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.flows.sync_assembly_batch_results.update_analysis_statuses_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.flows.sync_assembly_batch_results.update_external_results_dirs_from_copy_results"
    )
    @patch(
        "workflows.flows.analysis.assembly.flows.sync_assembly_batch_results.copy_assembly_batch_results_to_destination_folder"
    )
    def test_sync_assembly_batch_results_returns_copy_results(
        self,
        mock_copy_batch_results,
        mock_update_external_results_dirs,
        mock_update_analysis_statuses,
        mock_update_results_dirs,
        setup_batch_and_analysis,
        prefect_harness,
        tmp_path,
    ):
        batch, analysis, _batch_analysis_relation = setup_batch_and_analysis
        external_copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "ftp" / "analysis",
            success=True,
        )
        nfs_copy_result = BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=tmp_path / "workspace" / "results" / "analysis",
            success=True,
        )
        mock_copy_batch_results.side_effect = [
            [external_copy_result],
            [nfs_copy_result],
        ]

        result = sync_assembly_batch_results(batch.id)

        expected_external_results_root = (
            EMG_CONFIG.slurm.private_results_dir
            if batch.study.is_private
            else EMG_CONFIG.slurm.ftp_results_dir
        )
        assert result.external_copy_results == [external_copy_result]
        assert result.nfs_copy_results == [nfs_copy_result]
        assert mock_copy_batch_results.call_count == 2
        assert mock_copy_batch_results.call_args_list[0].kwargs["destination_root"] == (
            expected_external_results_root
        )
        assert mock_copy_batch_results.call_args_list[1].kwargs["destination_root"] == (
            tmp_path / "results"
        )
        mock_update_external_results_dirs.assert_called_once_with(
            [external_copy_result],
            destination_root=Path(expected_external_results_root),
        )
        mock_update_analysis_statuses.assert_called_once_with([external_copy_result])
        mock_update_results_dirs.assert_called_once_with([nfs_copy_result])
