from unittest.mock import Mock, patch

import pytest
from django.urls import reverse

from analyses.models import Run, Study
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
)


@patch("analyses.admin.study.run_deployment")
@pytest.mark.django_db
def test_resync_study_with_ena_admin_action(
    mock_run_deployment, monkeypatch, admin_client, raw_reads_mgnify_study
):
    mock_run_deployment.return_value = Mock(id="flow-run-id")
    monkeypatch.setenv("PREFECT_UI_URL", "https://prefect.example.com")

    response = admin_client.post(
        reverse("admin:analyses_study_changelist"),
        {
            "action": "resync_with_ena",
            "_selected_action": [raw_reads_mgnify_study.pk],
            "select_across": "0",
        },
        follow=True,
    )

    assert response.status_code == 200
    mock_run_deployment.assert_called_once_with(
        name="sync-studies-with-ena/sync_studies_with_ena",
        parameters={"accessions": [raw_reads_mgnify_study.ena_study.accession]},
        timeout=0,
    )
    assert (
        "flow run flow-run-id"  # text of the prefect UI link
        in response.content.decode()
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


@pytest.mark.django_db
def test_curate_run_experiment_types_updates_only_target_study(
    admin_client, raw_reads_mgnify_study, raw_read_run
):
    """
    The curation action should set the experiment type of the selected runs of the study,
    and leave runs of other studies alone.
    """
    other_study = Study.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study, title="Another study"
    )
    other_run = Run.objects.create(
        ena_accessions=["SRR9999999"],
        study=other_study,
        ena_study=other_study.ena_study,
        sample=raw_read_run[0].sample,
        experiment_type=Run.ExperimentTypes.AMPLICON,
    )

    url = reverse(
        "admin:analyses_study_curate_run_experiment_types",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.post(
        url,
        {
            "experiment_type": Run.ExperimentTypes.METATRANSCRIPTOMIC,
            "runs": [run.pk for run in raw_read_run],
        },
    )

    assert response.status_code == 302
    for run in raw_read_run:
        run.refresh_from_db()
        assert run.experiment_type == Run.ExperimentTypes.METATRANSCRIPTOMIC

    other_run.refresh_from_db()
    assert other_run.experiment_type == Run.ExperimentTypes.AMPLICON


@pytest.mark.django_db
def test_curate_run_experiment_types_lists_the_studys_runs(
    admin_client, raw_reads_mgnify_study, raw_read_run
):
    """
    The curation page should render the study's runs and the experiment types to choose from.
    """
    url = reverse(
        "admin:analyses_study_curate_run_experiment_types",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.get(url)

    assert response.status_code == 200
    assert list(response.context["runs"]) == raw_read_run
    assert response.context["experiment_type_choices"] == Run.ExperimentTypes.choices
    assert raw_read_run[0].first_accession in response.content.decode()


@pytest.mark.django_db
def test_curate_run_experiment_types_without_selected_runs_changes_nothing(
    admin_client, raw_reads_mgnify_study, raw_read_run
):
    """
    Posting no runs should warn and leave the experiment types alone.
    """
    experiment_types_before = [run.experiment_type for run in raw_read_run]

    url = reverse(
        "admin:analyses_study_curate_run_experiment_types",
        args=[raw_reads_mgnify_study.pk],
    )
    response = admin_client.post(
        url, {"experiment_type": Run.ExperimentTypes.METAGENOMIC}
    )

    assert response.status_code == 200
    for run, experiment_type_before in zip(raw_read_run, experiment_types_before):
        run.refresh_from_db()
        assert run.experiment_type == experiment_type_before


@pytest.mark.django_db
def test_curate_run_experiment_types_rejects_non_staff(client, raw_reads_mgnify_study):
    url = reverse(
        "admin:analyses_study_curate_run_experiment_types",
        args=[raw_reads_mgnify_study.pk],
    )
    response = client.get(url)

    assert response.status_code == 302
    assert "/login" in response.url


@pytest.mark.django_db
def test_set_experiment_type_action_applies_across_the_filtered_runs(
    admin_client, raw_reads_mgnify_study, raw_read_run
):
    """
    "Select all runs matching this filter" must curate every filtered run, including the ones whose
    ids the confirmation page does not carry.
    """
    other_study = Study.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study, title="Another study"
    )
    other_run = Run.objects.create(
        ena_accessions=["SRR9999999"],
        study=other_study,
        ena_study=other_study.ena_study,
        sample=raw_read_run[0].sample,
        experiment_type=Run.ExperimentTypes.AMPLICON,
    )

    changelist_filtered_to_the_study = reverse(
        "admin:analyses_run_changelist",
        query={"study_accession": raw_reads_mgnify_study.accession},
    )
    confirmation = admin_client.post(
        changelist_filtered_to_the_study,
        {
            "action": "set_experiment_type",
            "index": "0",
            "select_across": "1",
            "_selected_action": [run.pk for run in raw_read_run],
        },
    )
    assert confirmation.status_code == 200

    # the confirmation page posts back a run id, without which the changelist would not dispatch
    assert confirmation.context["selected_ids"]

    response = admin_client.post(
        changelist_filtered_to_the_study,
        {
            "action": "set_experiment_type",
            "apply": "1",
            "select_across": "1",
            "_selected_action": list(confirmation.context["selected_ids"]),
            "experiment_type": Run.ExperimentTypes.METATRANSCRIPTOMIC,
        },
    )

    assert response.status_code == 302
    for run in raw_read_run:
        run.refresh_from_db()
        assert run.experiment_type == Run.ExperimentTypes.METATRANSCRIPTOMIC

    other_run.refresh_from_db()
    assert other_run.experiment_type == Run.ExperimentTypes.AMPLICON
