import pytest

from analyses.models import Analysis
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
)


@pytest.mark.django_db(transaction=True)
def test_create_batches_for_a_study(assembly_with_analyses, tmp_path):
    """Test the batch creation logic works as expected
    In this artificial test, we chunk by 1, so we should have one batch per analysis
    """
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
        chunk_size=1,  # Force one analysis per batch
    )

    assert len(batches) == len(analyses_list)

    for batch in batches:
        assert batch.study == study
        assert batch.batch_type == "assembly_analysis"
        assert batch.total_analyses == 1
        assert batch.analyses.count() == 1
        assert batch.workspace_dir

    all_batched_analyses = set()
    for batch in batches:
        for analysis in batch.analyses.all():
            all_batched_analyses.add(analysis.id)

    assert len(all_batched_analyses) == len(analyses_list)
    for analysis in analyses_list:
        assert analysis.id in all_batched_analyses


@pytest.mark.django_db(transaction=True)
def test_new_assemblies_added_to_study_should_appear_in_batches(
    assembly_with_analyses, mgnify_assemblies, tmp_path
):
    """Test that when new assemblies are added to a study, they are added to the batches"""
    study = assembly_with_analyses[0].study
    initial_analyses = list(assembly_with_analyses)

    # Create initial batches
    initial_batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )

    assert len(initial_batches) == 1
    initial_batch = initial_batches[0]
    assert initial_batch.analyses.count() == len(initial_analyses)

    # Add new analyses to the study (using remaining assemblies)
    new_analyses = []
    for assembly in mgnify_assemblies[3:]:
        analysis, _ = Analysis.objects.get_or_create(
            study=study,
            sample=assembly.sample,
            assembly=assembly,
            ena_study=study.ena_study,
            pipeline_version=Analysis.PipelineVersions.v6,
        )
        analysis.inherit_experiment_type()
        new_analyses.append(analysis)

    # Re-run batch creation - should pick up new analyses
    new_batches = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )

    # Should have created an additional batch for new analyses
    assert len(new_batches) == 1

    # Verify total analyses across all batches include new ones
    all_batches = AssemblyAnalysisBatch.objects.filter(study=study)
    total_analyses_in_batches = sum(batch.analyses.count() for batch in all_batches)
    assert total_analyses_in_batches == len(initial_analyses) + len(new_analyses)


@pytest.mark.django_db(transaction=True)
def test_idempotence(assembly_with_analyses, tmp_path):
    """Test that when re-running the creation logic, it doesn't duplicate batches"""
    study = assembly_with_analyses[0].study
    analyses_list = list(assembly_with_analyses)

    batches_first_run = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )

    assert len(batches_first_run) == 1
    first_batch = batches_first_run[0]
    first_batch_id = first_batch.id
    assert first_batch.analyses.count() == len(analyses_list)

    # The number of batch-analysis relationships should be consistent
    initial_batch_analysis_count = AssemblyAnalysisBatchAnalysis.all_objects.filter(
        batch__study=study
    ).count()
    assert initial_batch_analysis_count == len(analyses_list)

    # Re-run
    batches_second_run = AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
        study=study,
        pipeline=Analysis.PipelineVersions.v6,
        workspace_dir=tmp_path,
    )

    # Should return the existing batch, not create a new one
    assert len(batches_second_run) == 1
    assert batches_second_run[0].id == first_batch_id

    # Verify no duplicate batch-analysis relationships were created
    final_batch_analysis_count = AssemblyAnalysisBatchAnalysis.all_objects.filter(
        batch__study=study
    ).count()
    assert final_batch_analysis_count == initial_batch_analysis_count
    assert final_batch_analysis_count == len(analyses_list)

    # Verify total analyses in the batch haven't changed
    first_batch.refresh_from_db()
    assert first_batch.analyses.count() == len(analyses_list)
