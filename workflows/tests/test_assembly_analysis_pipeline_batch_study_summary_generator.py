from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator import (
    generate_assembly_analysis_pipeline_batch_summary,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
    AssemblyAnalysisPipeline,
)


@pytest.mark.django_db
class TestGenerateAssemblyAnalysisPipelineBatchSummary:
    """Test the generate_assembly_analysis_pipeline_batch_summary task... to some degree."""

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator.study_summary_generator"
    )
    def test_clears_old_study_level_summary_files(
        self,
        mock_summary_generator,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """
        Test that old study-level summary files are cleared before generating new ones.

        Verifies that files matching the pattern {study_accession}_*_summary.tsv
        are deleted, while batch-level files (UUID prefix) are preserved.
        """
        # Create an isolated results directory for this test
        isolated_results_dir = tmp_path / "isolated_test_results"
        isolated_results_dir.mkdir(parents=True, exist_ok=True)

        # Create batch with analysis
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path / "workspace"),
            batch_type="assembly_analysis",
        )
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Override the dir
        raw_reads_mgnify_study.results_dir = str(isolated_results_dir)
        raw_reads_mgnify_study.save()

        # Create old study-level summary files that should be deleted
        study_accession = raw_reads_mgnify_study.accession
        old_study_files = [
            isolated_results_dir / f"{study_accession}_antismash_summary.tsv",
            isolated_results_dir / f"{study_accession}_go_summary.tsv",
            isolated_results_dir / f"{study_accession}_taxonomy_summary.tsv",
        ]
        for old_file in old_study_files:
            old_file.write_text("old data")

        # Create batch-level files that should be preserved
        batch_file = isolated_results_dir / f"{batch.id}_taxonomy_summary.tsv"
        batch_file.write_text("batch data")

        # Create required input files for the task
        asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
        asa_workspace.mkdir(parents=True, exist_ok=True)
        analysed_assemblies = asa_workspace / "analysed_assemblies.csv"
        analysed_assemblies.write_text(
            f"{mgnify_assemblies[0].first_accession},completed"
        )

        # Run the task
        generate_assembly_analysis_pipeline_batch_summary(batch.id)

        # Verify old study-level files were deleted
        for old_file in old_study_files:
            assert not old_file.exists()

        # Verify batch-level file was preserved
        assert batch_file.exists()
