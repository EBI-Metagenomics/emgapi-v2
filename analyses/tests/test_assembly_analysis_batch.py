"""
Tests for AssemblyAnalysisBatch model and manager.
"""

import pytest
from pathlib import Path

import analyses.models


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_filters_completed_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that create_batches_for_study filters out already-completed analyses by default.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Mark first analysis as completed
    analyses_list[0].mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
        set_status_as=True,
    )

    # Create batches - should skip completed analysis
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        base_results_dir=tmp_path,
        skip_completed=True,
    )

    # Should create batch with only non-completed analyses
    assert len(batches) == 1
    assert batches[0].total_analyses == len(analyses_list) - 1
    # Completed analysis should not be in batch
    assert not batches[0].assembly_analysis_set.filter(id=analyses_list[0].id).exists()


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_warns_about_blocked_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that create_batches_for_study includes blocked/failed analyses but logs a warning.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Mark first analysis as blocked
    analyses_list[0].mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED, set_status_as=True
    )

    # Create batches - should include blocked analysis with warning
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        base_results_dir=tmp_path,
    )

    assert len(batches) == 1
    assert batches[0].total_analyses == len(analyses_list)
    # Verify the blocked analysis was included
    assert batches[0].assembly_analysis_set.filter(id=analyses_list[0].id).exists()


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_with_valid_analyses(assembly_with_analyses, tmp_path):
    """
    Test that create_batches_for_study works correctly with valid analyses.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Create batches
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        base_results_dir=tmp_path,
    )

    assert len(batches) == 1
    batch = batches[0]
    assert batch.total_analyses == len(analyses_list)
    assert batch.study == study
    assert batch.batch_type == "assembly_analysis"

    # Verify analyses are linked to batch
    assert batch.assembly_analysis_set.count() == len(analyses_list)

    # Verify all analyses have correct results_dir
    for analysis in analyses_list:
        analysis.refresh_from_db()
        assert analysis.assembly_analysis_batch == batch
        assert analysis.results_dir == batch.results_dir


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_chunks_analyses(assembly_with_analyses, tmp_path):
    """
    Test that create_batches_for_study chunks analyses correctly.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Create batches with small chunk size
    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        chunk_size=2,  # Force multiple batches
        base_results_dir=tmp_path,
    )

    # Should create multiple batches
    expected_batches = (len(analyses_list) + 1) // 2  # Ceiling division
    assert len(batches) == expected_batches

    # Verify total analyses across all batches
    total_analyses = sum(batch.total_analyses for batch in batches)
    assert total_analyses == len(analyses_list)


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_respects_max_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that create_batches_for_study respects max_analyses safety cap.
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    # Set max_analyses lower than actual count
    max_analyses = len(analyses_list) - 1

    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        max_analyses=max_analyses,
        base_results_dir=tmp_path,
    )

    # Should only process max_analyses count
    total_analyses = sum(batch.total_analyses for batch in batches)
    assert total_analyses == max_analyses


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_with_custom_results_dir(
    assembly_with_analyses, tmp_path
):
    """
    Test that create_batches_for_study uses custom base_results_dir correctly.
    """
    study = assembly_with_analyses[0].study

    custom_base_dir = tmp_path / "custom_results"

    batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
        study=study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        base_results_dir=custom_base_dir,
    )

    assert len(batches) == 1
    batch = batches[0]
    assert str(custom_base_dir) in batch.results_dir
    assert Path(batch.results_dir).exists()


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_study_raises_on_no_analyses(
    assembly_with_analyses, tmp_path
):
    """
    Test that create_batches_for_study raises ValueError when no pending analyses exist.
    """
    study = assembly_with_analyses[0].study

    # Mark all analyses as completed
    for analysis in assembly_with_analyses:
        analysis.mark_status(
            analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
            set_status_as=True,
        )

    # Should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            base_results_dir=tmp_path,
        )

    assert "No pending analyses found" in str(exc_info.value)
