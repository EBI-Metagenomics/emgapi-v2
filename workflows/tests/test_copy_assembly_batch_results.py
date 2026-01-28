from unittest.mock import patch

import pytest

from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_assembly_batch_results,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)
from analyses.models import Analysis


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
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )
        return batch, analysis

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results._copy_single_analysis_results"
    )
    def test_copy_failed_updates_status(
        self,
        mock_copy_single,
        setup_batch_and_analysis,
        prefect_harness,
    ):
        """Test that if _copy_single_analysis_results fails, the analysis status is updated correctly."""
        batch, analysis = setup_batch_and_analysis

        mock_copy_single.side_effect = Exception("Copy failed!")

        copy_assembly_batch_results(batch.id)

        # Verify analysis status was updated
        analysis.refresh_from_db()
        assert (
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            is False
        )

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results._copy_single_analysis_results"
    )
    def test_copy_success_updates_status(
        self,
        mock_copy_single,
        setup_batch_and_analysis,
        prefect_harness,
    ):
        """Test that if _copy_single_analysis_results succeeds, the analysis status is updated correctly."""
        batch, analysis = setup_batch_and_analysis

        analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
        analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] = True
        analysis.save()

        mock_copy_single.return_value = None

        copy_assembly_batch_results(batch.id)

        analysis.refresh_from_db()
        assert (
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            is True
        )
        assert analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] is False
        assert analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] is False
