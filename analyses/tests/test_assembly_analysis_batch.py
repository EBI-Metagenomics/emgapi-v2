import pytest
from pathlib import Path

import analyses.models
import workflows.models


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_filters_completed_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study filters out already-completed analyses by default.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Mark first analysis as completed
    analyses_list[0].mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
        set_status_as=True,
    )

    # Create batches - should skip completed analysis
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
            skip_completed=True,
        )
    )

    # Should create batch with only non-completed analyses
    assert len(batches) == 1
    assert batches[0].total_analyses == len(analyses_list) - 1
    # Completed analysis should not be in batch
    assert not batches[0].analyses.filter(id=analyses_list[0].id).exists()


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_excludes_blocked_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study excludes blocked/failed analyses and logs a warning.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Mark the first analysis as blocked
    analyses_list[0].mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED, set_status_as=True
    )

    # Create batches - should exclude blocked analysis with a warning
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    assert len(batches) == 1
    assert batches[0].total_analyses == len(analyses_list) - 1
    # Verify the blocked analysis was excluded
    assert not batches[0].analyses.filter(id=analyses_list[0].id).exists()


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_with_valid_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study works correctly with valid analyses.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Create batches
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    assert len(batches) == 1
    batch = batches[0]
    assert batch.total_analyses == len(analyses_list)
    assert batch.study == study
    assert batch.batch_type == "assembly_analysis"

    # Verify analyses are linked to batch
    assert batch.analyses.count() == len(analyses_list)

    # Verify all analyses are linked to batch
    for analysis in analyses_list:
        analysis.refresh_from_db()
        assert analysis in batch.analyses.all()


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_chunks_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study chunks analyses correctly.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Create batches with small chunk size
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            chunk_size=2,  # Force multiple batches
            workspace_dir=tmp_path,
        )
    )

    # Should create multiple batches
    expected_batches = (len(analyses_list) + 1) // 2  # Ceiling division
    assert len(batches) == expected_batches

    # Verify total analyses across all batches
    total_analyses = sum(batch.total_analyses for batch in batches)
    assert total_analyses == len(analyses_list)


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_respects_max_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study respects max_analyses safety cap.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Set max_analyses lower than actual count
    max_analyses = len(analyses_list) - 1

    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            max_analyses=max_analyses,
            workspace_dir=tmp_path,
        )
    )

    # Should only process max_analyses count
    total_analyses = sum(batch.total_analyses for batch in batches)
    assert total_analyses == max_analyses


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_with_custom_results_dir(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study uses custom workspace_dir correctly.
    """
    study = assembly_with_analyses[0].study

    custom_base_dir = tmp_path / "custom_results"

    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=custom_base_dir,
        )
    )

    assert len(batches) == 1
    batch = batches[0]
    assert str(custom_base_dir) in batch.workspace_dir
    assert Path(batch.workspace_dir).exists()


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_returns_empty_on_no_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study returns empty list when no pending analyses exist.
    """
    study = assembly_with_analyses[0].study

    # Mark all analyses as completed
    for analysis in assembly_with_analyses:
        analysis.mark_status(
            analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
            set_status_as=True,
        )

    # Should return empty list
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    assert len(batches) == 0


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_handles_rerun(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study handles re-running the same study correctly.

    When re-running this flow for the same study, or retrying a flow:
    1. Return existing batches
    2. Not create duplicate batch-analysis relationships
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    batches_first_run = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    assert len(batches_first_run) == 1
    first_batch = batches_first_run[0]
    assert first_batch.total_analyses == len(analyses_list)

    # Second run, simulate re-running the flow (should return existing batches)
    batches_second_run = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    # Should return the existing batch
    assert len(batches_second_run) == 1
    assert batches_second_run[0].id == first_batch.id

    # Should not create duplicate relationships
    assert first_batch.analyses.count() == len(analyses_list)

    # Verify that it doesn't duplicate AssemblyAnalysisBatchAnalysis records
    total_batch_analyses = (
        workflows.models.AssemblyAnalysisBatchAnalysis.all_objects.filter(
            batch__study=study
        ).count()
    )
    assert total_batch_analyses == len(analyses_list)


@pytest.mark.django_db(transaction=True)
def test_get_or_create_batches_for_study_handles_rerun_with_disabled_relationships(
    assembly_with_analyses, tmp_path
):
    """
    Test that get_or_create_batches_for_study handles disabled batch-analysis relationships correctly.

    When some batch-analysis relationships are disabled, it should not create duplicates for disabled relationships
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # First batch
    batches_first_run = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    assert len(batches_first_run) == 1
    first_batch = batches_first_run[0]

    # Disable one of the batch-analysis relationships
    batch_analysis = workflows.models.AssemblyAnalysisBatchAnalysis.all_objects.filter(
        batch=first_batch
    ).first()
    batch_analysis.disabled = True
    batch_analysis.save()

    # Second run: should still return existing batch and not create duplicates
    batches_second_run = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )

    # Should return the existing batch
    assert len(batches_second_run) == 1
    assert batches_second_run[0].id == first_batch.id

    # Should not have created any duplicate relationships (including disabled ones)
    total_batch_analyses = (
        workflows.models.AssemblyAnalysisBatchAnalysis.all_objects.filter(
            batch__study=study
        ).count()
    )
    assert total_batch_analyses == len(analyses_list)
