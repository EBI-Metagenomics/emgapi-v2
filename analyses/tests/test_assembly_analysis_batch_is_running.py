import pytest

import analyses.models
import workflows.models
from workflows.models import (
    AssemblyAnalysisBatchStatusCounts,
    AssemblyAnalysisPipeline,
    PipelineStatusCounts,
)


def test_pipeline_status_counts_is_running_true():
    """Test that is_running returns True when there are running analyses."""
    counts = PipelineStatusCounts(running=3)
    assert counts.is_running() is True


def test_pipeline_status_counts_is_running_false():
    """Test that is_running returns False when no analyses are running."""
    counts = PipelineStatusCounts(running=0, completed=5)
    assert counts.is_running() is False


def test_batch_status_counts_is_any_running_true():
    """Test that is_any_running returns True when at least one pipeline has running analyses."""
    status = AssemblyAnalysisBatchStatusCounts(
        asa=PipelineStatusCounts(running=1),
    )
    assert status.is_any_running() is True


def test_batch_status_counts_is_any_running_false():
    """Test that is_any_running returns False when all pipelines are completed."""
    status = AssemblyAnalysisBatchStatusCounts(
        asa=PipelineStatusCounts(completed=5),
        virify=PipelineStatusCounts(completed=5),
        map=PipelineStatusCounts(completed=5),
    )
    assert status.is_any_running() is False


def test_batch_status_counts_is_pipeline_running():
    """Test that is_pipeline_running correctly distinguishes between running and non-running pipelines."""
    status = AssemblyAnalysisBatchStatusCounts(
        asa=PipelineStatusCounts(completed=5),
        virify=PipelineStatusCounts(running=2),
    )
    assert status.is_pipeline_running(AssemblyAnalysisPipeline.ASA) is False
    assert status.is_pipeline_running(AssemblyAnalysisPipeline.VIRIFY) is True


@pytest.mark.django_db(transaction=True)
def test_is_running_no_counts(assembly_with_analyses, tmp_path):
    """
    When pipeline_status_counts is empty, is_running should return False.
    """
    study = assembly_with_analyses[0].study
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )
    batch = batches[0]
    assert batch.is_running() is False
    assert batch.is_running(AssemblyAnalysisPipeline.ASA) is False


@pytest.mark.django_db(transaction=True)
def test_is_running_with_running_asa(assembly_with_analyses, tmp_path):
    """
    When ASA has running analyses, is_running() and is_running(ASA) should return True.
    """
    study = assembly_with_analyses[0].study
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            workspace_dir=tmp_path,
        )
    )
    batch = batches[0]
    batch.pipeline_status_counts = AssemblyAnalysisBatchStatusCounts(
        asa=PipelineStatusCounts(running=2, pending=1),
    )
    batch.save()

    batch.refresh_from_db()
    assert batch.is_running() is True
    assert batch.is_running(AssemblyAnalysisPipeline.ASA) is True
    assert batch.is_running(AssemblyAnalysisPipeline.VIRIFY) is False
