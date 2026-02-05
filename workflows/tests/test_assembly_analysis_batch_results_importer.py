from unittest.mock import Mock, patch

import pytest

from analyses.models import Analysis
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    _copy_single_analysis_results,
)
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path
from workflows.data_io_utils.schemas import PipelineValidationError
from workflows.flows.analysis.assembly.tasks.process_import_results import (
    process_import_results,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    assembly_analysis_batch_results_importer,
    ImportResult,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    clear_pipeline_downloads,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


@pytest.mark.django_db
class TestAssemblyAnalysisBatchResultsImporter:
    """Test the assembly_analysis_batch_results_importer task."""

    def test_unsupported_pipeline_type_raises_error(
        self, raw_reads_mgnify_study, tmp_path, prefect_harness
    ):
        """Test that invalid pipeline type raises ValueError."""
        # Create a batch
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        # Create a mock invalid pipeline type
        invalid_pipeline = Mock()
        invalid_pipeline.value = "invalid"

        with pytest.raises(ValueError, match="Unsupported pipeline type"):
            assembly_analysis_batch_results_importer(
                batch.id,
                invalid_pipeline,
            )

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    def test_successful_import_returns_success_result(
        self,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that a successful import returns ImportResult with success=True."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Mock successful import
        mock_importer = Mock()
        mock_importer.import_results.return_value = 10
        mock_importer_class.return_value = mock_importer

        # Run importer
        results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
        )

        # Verify results
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].analysis_id == analysis.id
        assert results[0].downloads_count == 10
        assert results[0].error is None

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    def test_validation_failure_returns_failed_result(
        self,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that validation error returns ImportResult with success=False."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Mock validation failure
        mock_importer = Mock()
        mock_importer.import_results.side_effect = PipelineValidationError(
            "IPR pattern validation failed"
        )
        mock_importer_class.return_value = mock_importer

        # Run importer
        results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
        )

        # Verify results
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].analysis_id == analysis.id
        assert results[0].downloads_count is None
        assert "IPR pattern validation failed" in results[0].error

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    def test_unexpected_exception_returns_failed_result(
        self,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that unexpected exceptions are caught and returned as failed results."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Mock unexpected exception
        mock_importer = Mock()
        mock_importer.import_results.side_effect = RuntimeError(
            "Database connection lost"
        )
        mock_importer_class.return_value = mock_importer

        # Run importer
        results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.VIRIFY,
        )

        # Verify results
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].analysis_id == analysis.id
        assert "Unexpected error" in results[0].error
        assert "Database connection lost" in results[0].error

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    def test_mixed_results_some_pass_some_fail(
        self,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test partial failure scenario where some analyses pass and some fail validation."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        analyses = []
        for i in range(3):
            analysis = Analysis.objects.create(
                study=raw_reads_mgnify_study,
                sample=raw_reads_mgnify_sample[0],
                ena_study=raw_reads_mgnify_study.ena_study,
                assembly=mgnify_assemblies[0],
            )
            AssemblyAnalysisBatchAnalysis.objects.create(
                batch=batch,
                analysis=analysis,
                map_status=AssemblyAnalysisPipelineStatus.COMPLETED,
            )
            analyses.append(analysis)

        # Mock mixed results: first succeeds, second fails validation, third succeeds
        def mock_import_side_effect(*args, **kwargs):
            call_count = mock_importer_class.call_count
            if call_count == 2:  # Second call
                raise PipelineValidationError("Invalid data in analysis 2")
            return 5  # Success

        mock_importer = Mock()
        mock_importer.import_results.side_effect = mock_import_side_effect
        mock_importer_class.return_value = mock_importer

        # Run importer
        results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.MAP,
        )

        # Verify results
        assert len(results) == 3

        # First analysis: success
        assert results[0].success is True
        assert results[0].analysis_id == analyses[0].id

        # Second analysis: failed
        assert results[1].success is False
        assert results[1].analysis_id == analyses[1].id
        assert "Invalid data" in results[1].error

        # Third analysis: success
        assert results[2].success is True
        assert results[2].analysis_id == analyses[2].id


@pytest.mark.django_db
class TestProcessImportResults:
    """Test the process_import_results helper function."""

    def test_empty_results_updates_counts_only(
        self,
        raw_reads_mgnify_study,
        tmp_path,
        caplog,
        prefect_harness,
    ):
        """Test that empty results list only updates pipeline counts."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        process_import_results(batch.id, [], AssemblyAnalysisPipeline.ASA)

        assert "No import results to process" in caplog.text

    def test_all_success_no_status_changes(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that all successful imports don't trigger status updates."""
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
        )
        batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        results = [
            ImportResult(analysis_id=analysis.id, success=True, downloads_count=10)
        ]

        process_import_results(batch.id, results, AssemblyAnalysisPipeline.ASA)

        # Verify status unchanged
        batch_analysis.refresh_from_db()
        assert batch_analysis.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED

        analysis.refresh_from_db()
        assert not analysis.status.get(
            Analysis.AnalysisStates.ANALYSIS_QC_FAILED, False
        )

    def test_failed_results_update_statuses_in_bulk(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that failed imports update batch and analysis statuses correctly."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        # Create multiple analyses
        analyses = []
        batch_analyses = []
        for i in range(2):
            analysis = Analysis.objects.create(
                study=raw_reads_mgnify_study,
                sample=raw_reads_mgnify_sample[0],
                ena_study=raw_reads_mgnify_study.ena_study,
                assembly=mgnify_assemblies[0],
            )
            batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
                batch=batch,
                analysis=analysis,
                virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
            )
            analyses.append(analysis)
            batch_analyses.append(batch_analysis)

        # Both failed
        results = [
            ImportResult(
                analysis_id=analyses[0].id,
                success=False,
                error="Validation error 1",
            ),
            ImportResult(
                analysis_id=analyses[1].id,
                success=False,
                error="Validation error 2",
            ),
        ]

        process_import_results(
            batch.id,
            results,
            AssemblyAnalysisPipeline.VIRIFY,
        )

        # Verify batch analysis statuses updated
        for batch_analysis in batch_analyses:
            batch_analysis.refresh_from_db()
            assert batch_analysis.virify_status == AssemblyAnalysisPipelineStatus.FAILED

        # Verify Analysis statuses updated
        for i, analysis in enumerate(analyses):
            analysis.refresh_from_db()
            assert analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] is True
            assert (
                f"Validation error {i + 1}"
                in analysis.status[
                    f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
                ]
            )

    def test_mixed_results_only_failures_marked(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that only failed analyses are marked as QC_FAILED."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        # Create 3 analyses
        analyses = []
        batch_analyses = []
        for i in range(3):
            analysis = Analysis.objects.create(
                study=raw_reads_mgnify_study,
                sample=raw_reads_mgnify_sample[0],
                ena_study=raw_reads_mgnify_study.ena_study,
                assembly=mgnify_assemblies[0],
            )
            batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
                batch=batch,
                analysis=analysis,
                map_status=AssemblyAnalysisPipelineStatus.COMPLETED,
            )
            analyses.append(analysis)
            batch_analyses.append(batch_analysis)

        # Mixed: pass, fail, pass
        results = [
            ImportResult(analysis_id=analyses[0].id, success=True, downloads_count=5),
            ImportResult(
                analysis_id=analyses[1].id,
                success=False,
                error="Schema validation failed",
            ),
            ImportResult(analysis_id=analyses[2].id, success=True, downloads_count=7),
        ]

        process_import_results(batch.id, results, AssemblyAnalysisPipeline.MAP)

        # Verify only the failed one is marked
        batch_analyses[0].refresh_from_db()
        assert batch_analyses[0].map_status == AssemblyAnalysisPipelineStatus.COMPLETED

        batch_analyses[1].refresh_from_db()
        assert batch_analyses[1].map_status == AssemblyAnalysisPipelineStatus.FAILED

        batch_analyses[2].refresh_from_db()
        assert batch_analyses[2].map_status == AssemblyAnalysisPipelineStatus.COMPLETED

        # Verify Analysis QC_FAILED status
        analyses[0].refresh_from_db()
        assert not analyses[0].status.get(
            Analysis.AnalysisStates.ANALYSIS_QC_FAILED, False
        )

        analyses[1].refresh_from_db()
        assert analyses[1].status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] is True

        analyses[2].refresh_from_db()
        assert not analyses[2].status.get(
            Analysis.AnalysisStates.ANALYSIS_QC_FAILED, False
        )


@pytest.mark.django_db
class TestValidationOnlyMode:
    """Test validation-only mode and retry scenarios."""

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    def test_validation_only_does_not_import(
        self,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that validation_only=True validates but doesn't import."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Mock the schema validation (not the importer)
        with patch(
            "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultSchema"
        ) as mock_schema_class:
            mock_schema = mock_schema_class.return_value
            mock_schema.validate_results.return_value = None  # Validation succeeds

            results = assembly_analysis_batch_results_importer(
                batch.id,
                AssemblyAnalysisPipeline.ASA,
                validation_only=True,
            )

            # Verify validation was called
            mock_schema.validate_results.assert_called_once()

            # Verify importer was NOT called
            mock_importer_class.assert_not_called()

            # Verify result indicates validation-only
            assert len(results) == 1
            assert results[0].success is True
            assert results[0].downloads_count is None

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultSchema"
    )
    def test_validation_failure_prevents_import(
        self,
        mock_schema_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that validation failure in validation-only mode marks analysis as FAILED."""
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
        )
        batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Mock validation failure
        mock_schema = mock_schema_class.return_value
        mock_schema.validate_results.side_effect = PipelineValidationError(
            "Schema error"
        )

        # Validate (should fail)
        validation_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
            validation_only=True,
        )

        assert len(validation_results) == 1
        assert validation_results[0].success is False
        assert "Schema error" in validation_results[0].error

        # Process results to update statuses
        process_import_results(
            batch.id,
            validation_results,
            AssemblyAnalysisPipeline.ASA,
        )

        # Verify status was marked as FAILED
        batch_analysis.refresh_from_db()
        assert batch_analysis.asa_status == AssemblyAnalysisPipelineStatus.FAILED

        analysis.refresh_from_db()
        assert analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] is True

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultSchema"
    )
    def test_retry_after_validation_failure(
        self,
        mock_schema_class,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """
        Test retry scenario: validation fails, data fixed, retry succeeds.

        Verifies that after fixing data:
        1. Validation passes
        2. Import succeeds
        3. Status changes from FAILED → COMPLETED
        """
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
        )
        batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # FIRST RUN: Validation fails
        mock_schema = mock_schema_class.return_value
        mock_schema.validate_results.side_effect = PipelineValidationError("Bad data")

        validation_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
            validation_only=True,
        )

        process_import_results(
            batch.id, validation_results, AssemblyAnalysisPipeline.ASA
        )

        batch_analysis.refresh_from_db()
        assert batch_analysis.asa_status == AssemblyAnalysisPipelineStatus.FAILED

        # RETRY: MGnifier fixes data, re-runs validation
        # First, reset status to COMPLETED (simulating re-running set_post_assembly_analysis_states)
        batch_analysis.asa_status = AssemblyAnalysisPipelineStatus.COMPLETED
        batch_analysis.save()

        # Mock validation now succeeds
        mock_schema.validate_results.side_effect = None
        mock_schema.validate_results.return_value = None

        # Mock import succeeds
        mock_importer = Mock()
        mock_importer.import_results.return_value = 10
        mock_importer_class.return_value = mock_importer

        # Validate again (should pass)
        validation_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
            validation_only=True,
        )
        assert validation_results[0].success is True

        # Import (should succeed)
        import_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.ASA,
            validation_only=False,
        )

        assert import_results[0].success is True
        assert import_results[0].downloads_count == 10

        # Verify final status is COMPLETED
        batch_analysis.refresh_from_db()
        assert batch_analysis.asa_status == AssemblyAnalysisPipelineStatus.COMPLETED

    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.AssemblyResultImporter"
    )
    @patch(
        "workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer.VirifyResultSchema"
    )
    def test_partial_retry_only_failed_reprocessed(
        self,
        mock_schema_class,
        mock_importer_class,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """
        Test partial retry: 2 analyses, 1 fails validation, 1 succeeds.
        On retry, only the failed one should be revalidated/imported.
        """
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        # Create 2 analyses
        analyses = []
        batch_analyses = []
        for i in range(2):
            analysis = Analysis.objects.create(
                study=raw_reads_mgnify_study,
                sample=raw_reads_mgnify_sample[0],
                ena_study=raw_reads_mgnify_study.ena_study,
                assembly=mgnify_assemblies[0],
            )
            batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
                batch=batch,
                analysis=analysis,
                virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
            )
            analyses.append(analysis)
            batch_analyses.append(batch_analysis)

        # FIRST RUN: First passes, second fails validation
        mock_schema = mock_schema_class.return_value
        call_count = [0]

        def validation_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:  # The second call fails
                raise PipelineValidationError("Invalid data")
            return None

        mock_schema.validate_results.side_effect = validation_side_effect

        # Mock import for a successful one
        mock_importer = Mock()
        mock_importer.import_results.return_value = 5
        mock_importer_class.return_value = mock_importer

        # Validate
        validation_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.VIRIFY,
            validation_only=True,
        )

        process_import_results(
            batch.id, validation_results, AssemblyAnalysisPipeline.VIRIFY
        )

        # First should still be COMPLETED, the second should be FAILED
        batch_analyses[0].refresh_from_db()
        assert (
            batch_analyses[0].virify_status == AssemblyAnalysisPipelineStatus.COMPLETED
        )

        batch_analyses[1].refresh_from_db()
        assert batch_analyses[1].virify_status == AssemblyAnalysisPipelineStatus.FAILED

        # Import successful validations (only the first one)
        successful_validations = [r for r in validation_results if r.success]
        assert len(successful_validations) == 1

        import_results = assembly_analysis_batch_results_importer(
            batch.id,
            AssemblyAnalysisPipeline.VIRIFY,
            validation_only=False,
        )

        # Only the first one should be imported
        assert len(import_results) == 1
        assert import_results[0].analysis_id == analyses[0].id


@pytest.mark.django_db
class TestErrorMessageDistinction:
    """Test distinction between validation and import errors."""

    def test_validation_error_prefix(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that validation errors are prefixed correctly."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Validation failure
        results = [
            ImportResult(
                analysis_id=analysis.id, success=False, error="Missing required file"
            )
        ]

        process_import_results(
            batch.id, results, AssemblyAnalysisPipeline.VIRIFY, validation_only=True
        )

        analysis.refresh_from_db()
        reason = analysis.status.get(
            f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
        )
        assert reason == "VIRIFY Validation error: Missing required file"

    def test_import_error_prefix(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that import errors are prefixed correctly."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            map_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Import failure
        results = [
            ImportResult(
                analysis_id=analysis.id,
                success=False,
                error="Database constraint violation",
            )
        ]

        process_import_results(
            batch.id,
            results,
            AssemblyAnalysisPipeline.MAP,
        )

        analysis.refresh_from_db()
        reason = analysis.status.get(
            f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
        )
        assert reason == "MAP Import error: Database constraint violation"


@pytest.mark.django_db
class TestErrorLogField:
    """Test error_log JSONField and log_error method."""

    def test_log_error_creates_entry(
        self,
        raw_reads_mgnify_study,
        tmp_path,
    ):
        """Test that log_error creates a properly formatted entry."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        batch.log_error(
            pipeline_type=AssemblyAnalysisPipeline.ASA,
            error_type="validation",
            message="Test error message",
            analysis_id=123,
        )

        assert len(batch.error_log) == 1
        entry = batch.error_log[0]
        assert entry["pipeline"] == "asa"
        assert entry["error_type"] == "validation"
        assert entry["message"] == "Test error message"
        assert entry["analysis_id"] == 123
        assert "timestamp" in entry

    def test_log_error_truncates_long_messages(
        self,
        raw_reads_mgnify_study,
        tmp_path,
    ):
        """Test that long error messages are truncated."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        long_message = "x" * 600
        batch.log_error(
            pipeline_type=AssemblyAnalysisPipeline.VIRIFY,
            error_type="import",
            message=long_message,
        )

        entry = batch.error_log[0]
        assert len(entry["message"]) == 503  # 500 + "..."
        assert entry["message"].endswith("...")

    def test_log_error_limits_to_100_entries(
        self,
        raw_reads_mgnify_study,
        tmp_path,
    ):
        """Test that error_log is capped at 100 entries."""
        batch = AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            workspace_dir=str(tmp_path),
            batch_type="assembly_analysis",
        )

        # Add 105 errors
        for i in range(105):
            batch.log_error(
                pipeline_type=AssemblyAnalysisPipeline.MAP,
                error_type="test",
                message=f"Error {i}",
                save=False,
            )
        batch.save()

        batch.refresh_from_db()
        assert len(batch.error_log) == 100
        # Should keep the last 100
        assert batch.error_log[0]["message"] == "Error 5"
        assert batch.error_log[-1]["message"] == "Error 104"

    def test_error_log_integrated_in_process_import_results(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that process_import_results logs errors to error_log."""
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
        )
        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        results = [
            ImportResult(
                analysis_id=analysis.id, success=False, error="Validation failed"
            )
        ]

        process_import_results(
            batch.id,
            results,
            AssemblyAnalysisPipeline.ASA,
            validation_only=True,
        )

        batch.refresh_from_db()
        assert len(batch.error_log) == 1
        assert batch.error_log[0]["pipeline"] == "asa"
        assert batch.error_log[0]["error_type"] == "validation"
        assert batch.error_log[0]["message"] == "Validation failed"
        assert batch.error_log[0]["analysis_id"] == analysis.id


@pytest.mark.django_db
class TestIdempotentImports:
    """Test idempotent import functionality via clear_pipeline_downloads."""

    def test_clear_asa_downloads(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
    ):
        """Test that ASA downloads are cleared correctly."""
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )

        # Add some ASA downloads
        analysis.downloads = [
            {"alias": "qc1", "download_group": "quality_control"},
            {"alias": "tax1", "download_group": Analysis.TAXONOMIES},
            {
                "alias": "func1",
                "download_group": f"{Analysis.FUNCTIONAL_ANNOTATION}.interpro",
            },
            {
                "alias": "path1",
                "download_group": f"{Analysis.PATHWAYS_AND_SYSTEMS}.antismash",
            },
            {
                "alias": "virify1",
                "download_group": Analysis.VIRIFY,
            },  # Should NOT be cleared
        ]
        analysis.save()

        clear_pipeline_downloads(analysis, AssemblyAnalysisPipeline.ASA)

        analysis.refresh_from_db()
        # Only VIRify download should remain
        assert len(analysis.downloads) == 1
        assert analysis.downloads[0]["alias"] == "virify1"

    def test_clear_virify_downloads(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
    ):
        """Test that VIRify downloads are cleared correctly."""
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )

        analysis.downloads = [
            {"alias": "virify1", "download_group": "virify"},
            {
                "alias": "asa1",
                "download_group": "quality_control",
            },  # Should NOT be cleared
        ]
        analysis.save()

        clear_pipeline_downloads(analysis, AssemblyAnalysisPipeline.VIRIFY)

        analysis.refresh_from_db()
        assert len(analysis.downloads) == 1
        assert analysis.downloads[0]["alias"] == "asa1"

    def test_clear_map_downloads(
        self,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
    ):
        """Test that MAP downloads are cleared correctly."""
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )

        analysis.downloads = [
            {"alias": "map1", "download_group": Analysis.MAP},
            {"alias": "asa1", "download_group": "taxonomy"},  # Should NOT be cleared
        ]
        analysis.save()

        clear_pipeline_downloads(analysis, AssemblyAnalysisPipeline.MAP)

        analysis.refresh_from_db()
        assert len(analysis.downloads) == 1
        assert analysis.downloads[0]["alias"] == "asa1"

    def test_clear_downloads_no_matching_downloads(
        self, raw_reads_mgnify_study, raw_reads_mgnify_sample, mgnify_assemblies
    ):
        """Test that clearing with no matching downloads doesn't save."""

        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )

        analysis.downloads = [
            {"alias": "other1", "download_group": "something_else"},
        ]
        analysis.save()
        original_updated = analysis.updated_at

        clear_pipeline_downloads(analysis, AssemblyAnalysisPipeline.ASA)

        analysis.refresh_from_db()
        # No save should have occurred
        assert analysis.updated_at == original_updated
        assert len(analysis.downloads) == 1

    def test_clear_downloads_logs_removal_count(
        self, raw_reads_mgnify_study, raw_reads_mgnify_sample, mgnify_assemblies, caplog
    ):
        """Test that clearing logs the number of removed downloads."""
        analysis = Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=raw_reads_mgnify_sample[0],
            ena_study=raw_reads_mgnify_study.ena_study,
            assembly=mgnify_assemblies[0],
        )

        analysis.downloads = [
            {"alias": "qc1", "download_group": "quality_control"},
            {"alias": "qc2", "download_group": "quality_control"},
            {"alias": "tax1", "download_group": "taxonomy"},
        ]
        analysis.save()

        clear_pipeline_downloads(analysis, AssemblyAnalysisPipeline.ASA)

        assert (
            f"Cleared 3 existing ASA downloads from {analysis.accession} for idempotent retry"
            in caplog.text
        )

    @patch(
        "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
    )
    def test_download_paths_no_duplicate_identifier(
        self,
        mock_run_deployment,
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        mgnify_assemblies,
        tmp_path,
        prefect_harness,
    ):
        """Test that download paths don't contain duplicate assembly identifiers."""
        # Mock run_deployment to prevent actual deployment execution
        mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

        assembly_id = mgnify_assemblies[0].first_accession

        # Create batch and analysis
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
            pipeline_version=Analysis.PipelineVersions.v6,
            experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
        )
        batch_analysis = AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED,
        )

        # Create source files in the batch workspace
        asa_workspace = tmp_path / "workspace" / "asa"
        test_qc_dir = asa_workspace / assembly_id / "qc"
        test_qc_dir.mkdir(parents=True)
        (test_qc_dir / "multiqc_report.html").write_text("<html>test</html>")

        # Mock get_pipeline_workspace
        batch.get_pipeline_workspace = lambda pipeline: asa_workspace

        # Copy results using actual function
        target_root = tmp_path / "ftp"
        logger = Mock()
        _copy_single_analysis_results(
            analysis=analysis,
            batch_analysis_relation=batch_analysis,
            batch=batch,
            target_root=str(target_root),
            logger=logger,
        )

        # Verify run_deployment was called once (only ASA is COMPLETED)
        assert mock_run_deployment.call_count == 1

        # Verify external_results_dir was set correctly
        analysis.refresh_from_db()
        assert analysis.external_results_dir

        # Verify no duplicate assembly identifier in external_results_dir
        assert f"/{assembly_id}/{assembly_id}/" not in str(
            analysis.external_results_dir
        )

        # Verify V6/assembly is in the path
        assert "/V6/assembly" in str(analysis.external_results_dir)

        # Verify the expected path structure
        study_accession = raw_reads_mgnify_study.first_accession
        expected_base = (
            target_root
            / accession_prefix_separated_dir_path(study_accession, -3)
            / accession_prefix_separated_dir_path(assembly_id, -3)
            / "V6"
            / "assembly"
        )
        assert f"/{assembly_id}/{assembly_id}/" not in str(expected_base)
