import pytest
from django.urls import reverse

from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)


@pytest.mark.django_db
def test_show_assembly_status_summary_empty_study(admin_client, raw_reads_mgnify_study):
    """
    Test show_assembly_status_summary with a study that has no assemblies.

    This should render without error with zero counts.
    """
    url = reverse(
        "admin:analyses_study_show_assembly_status_summary",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.get(url)

    assert response.status_code == 200
    assert "study" in response.context
    assert "assemblies_status_table" in response.context
    assert "assemblies_progress" in response.context


@pytest.mark.django_db
def test_refresh_batch_counts_no_batches(admin_client, raw_reads_mgnify_study):
    """
    Test refresh_batch_counts with a study that has no batches.
    Should redirect and show success message for 0 batches.
    """
    url = reverse(
        "admin_refresh_study_assembly_analysis_counts",
        kwargs={"study_id": raw_reads_mgnify_study.pk},
    )
    response = admin_client.get(url)

    # Should redirect back to the assembly analysis summary
    assert response.status_code == 302


@pytest.mark.django_db
def test_refresh_batch_counts_initializes_uninitialized_counts(
    admin_client, raw_reads_mgnify_study, tmp_path
):
    """
    Test refresh_batch_counts properly initializes empty pipeline_status_counts to zero.

    When pipeline_status_counts is empty dict (uninitialized), the refresh action should
    initialize all count fields to zero and ensure all pipeline status attributes exist.
    """
    batch = AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        batch_type="test",
        workspace_dir=str(tmp_path),
        total_analyses=0,
    )

    url = reverse(
        "admin_refresh_study_assembly_analysis_counts",
        kwargs={"study_id": raw_reads_mgnify_study.pk},
    )
    response = admin_client.get(url)

    assert response.status_code == 302

    batch.refresh_from_db()
    assert batch.pipeline_status_counts is not None

    # Verify all pipeline status attributes are present
    assert hasattr(batch.pipeline_status_counts, "asa")
    assert hasattr(batch.pipeline_status_counts, "virify")
    assert hasattr(batch.pipeline_status_counts, "map")

    # Verify all counts are initialized to zero
    assert batch.pipeline_status_counts.asa.pending == 0
    assert batch.pipeline_status_counts.asa.running == 0
    assert batch.pipeline_status_counts.asa.completed == 0
    assert batch.pipeline_status_counts.asa.failed == 0


@pytest.mark.django_db
def test_show_assembly_analysis_status_summary_handles_empty_counts(
    admin_client, raw_reads_mgnify_study, tmp_path
):
    """
    Test that show_assembly_analysis_status_summary doesn't crash with empty counts.

    This tests the specific code path where batch.pipeline_status_counts might be empty dict
    when trying to access batch.pipeline_status_counts.asa.pending
    """
    # Create batch without initialized pipeline_status_counts
    AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        batch_type="test",
        workspace_dir=str(tmp_path),
        total_analyses=0,
    )
    url = reverse(
        "admin:analyses_study_show_assembly_analysis_status_summary",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.get(url)

    assert response.status_code == 200
    assert "study" in response.context
    assert "batches_table" in response.context
    assert "summary_items" in response.context


@pytest.mark.django_db
def test_show_assembly_analysis_status_summary_with_initialized_counts(
    admin_client, raw_reads_mgnify_study, assembly_with_analyses, tmp_path
):
    """
    Test that show_assembly_analysis_status_summary correctly displays initialized counts.
    """
    batch = AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        batch_type="test",
        workspace_dir=str(tmp_path),
        total_analyses=len(assembly_with_analyses),
    )

    # Add analyses with mixed statuses
    for i, analysis in enumerate(assembly_with_analyses):
        if i % 2 == 0:
            status = AssemblyAnalysisPipelineStatus.PENDING
        else:
            status = AssemblyAnalysisPipelineStatus.RUNNING

        AssemblyAnalysisBatchAnalysis.objects.create(
            batch=batch,
            analysis=analysis,
            asa_status=status,
            virify_status=AssemblyAnalysisPipelineStatus.PENDING,
            map_status=AssemblyAnalysisPipelineStatus.PENDING,
        )

    # Initialize counts
    batch.update_pipeline_status_counts()

    url = reverse(
        "admin:analyses_study_show_assembly_analysis_status_summary",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.get(url)

    assert response.status_code == 200

    # Check that summary items are correct
    summary_items = response.context["summary_items"]
    total_analyses_item = next(
        item for item in summary_items if item["label"] == "Total analyses"
    )
    assert total_analyses_item["value"] == len(assembly_with_analyses)


@pytest.mark.django_db
def test_refresh_batch_counts_multiple_batches(
    admin_client, raw_reads_mgnify_study, tmp_path
):
    """
    Test refresh_batch_counts with multiple batches - all should be refreshed.
    """
    # Create 3 batches with uninitialized counts
    for i in range(3):
        AssemblyAnalysisBatch.objects.create(
            study=raw_reads_mgnify_study,
            batch_type="test",
            workspace_dir=str(tmp_path / f"batch_{i}"),
            total_analyses=0,
        )

    url = reverse(
        "admin_refresh_study_assembly_analysis_counts",
        kwargs={"study_id": raw_reads_mgnify_study.pk},
    )
    response = admin_client.get(url)

    assert response.status_code == 302

    # All batches should have initialized counts
    for batch in raw_reads_mgnify_study.analysis_batches.all():
        batch.refresh_from_db()
        assert batch.pipeline_status_counts is not None
        assert batch.pipeline_status_counts.asa.pending == 0
        assert batch.pipeline_status_counts.virify.pending == 0
        assert batch.pipeline_status_counts.map.pending == 0
