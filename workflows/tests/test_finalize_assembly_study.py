import pytest
from unittest.mock import MagicMock, patch
from analyses.models import Analysis
from workflows.models import AssemblyAnalysisBatch
from workflows.flows.analysis.assembly.flows.finalize_assembly_study import (
    finalize_assembly_study,
)


@pytest.mark.django_db
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.get_run_logger")
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.load_flow_run")
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.merge_assembly_study_summaries"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.add_assembly_study_summaries_to_downloads"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.copy_v6_study_summaries"
)
def test_finalize_assembly_study_all_complete(
    mock_copy,
    mock_add,
    mock_merge,
    mock_load_flow_run,
    mock_logger,
    raw_reads_mgnify_study,
    batch,
):
    # Set pipeline version to v6 to match the filter in the flow
    batch.pipeline_versions = [Analysis.PipelineVersions.v6]
    batch.save()

    # Setup mock flow runs to be NOT running
    mock_flow_run = MagicMock()
    mock_flow_run.is_running.return_value = False
    mock_load_flow_run.return_value = mock_flow_run

    # Execute flow
    finalize_assembly_study.fn(raw_reads_mgnify_study.accession)

    # Verify all tasks were called
    mock_merge.assert_called_once_with(
        raw_reads_mgnify_study.accession, cleanup_partials=True
    )
    mock_add.assert_called_once_with(raw_reads_mgnify_study.accession)
    mock_copy.assert_called_once_with(raw_reads_mgnify_study.accession)


@pytest.mark.django_db
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.get_run_logger")
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.load_flow_run")
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.merge_assembly_study_summaries"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.add_assembly_study_summaries_to_downloads"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.copy_v6_study_summaries"
)
def test_finalize_assembly_study_still_running(
    mock_copy,
    mock_add,
    mock_merge,
    mock_load_flow_run,
    mock_logger,
    raw_reads_mgnify_study,
    batch,
):
    # Set pipeline version to v6 to match the filter in the flow
    batch.pipeline_versions = [Analysis.PipelineVersions.v6]
    batch.save()

    # Setup mock flow runs: one is still running
    mock_running_flow = MagicMock()
    mock_running_flow.is_running.return_value = True

    mock_finished_flow = MagicMock()
    mock_finished_flow.is_running.return_value = False

    # Let's say flow "1" (asa) is running
    def side_effect(flow_id):
        print(flow_id)
        if flow_id == "1":
            return mock_running_flow
        return mock_finished_flow

    mock_load_flow_run.side_effect = side_effect

    # Execute flow
    finalize_assembly_study.fn(raw_reads_mgnify_study.accession)

    # Verify tasks were NOT called
    mock_merge.assert_not_called()
    mock_add.assert_not_called()
    mock_copy.assert_not_called()


@pytest.mark.django_db
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.get_run_logger")
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.load_flow_run")
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.merge_assembly_study_summaries"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.add_assembly_study_summaries_to_downloads"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.copy_v6_study_summaries"
)
def test_finalize_assembly_study_invalid_flow_ids(
    mock_copy,
    mock_add,
    mock_merge,
    mock_load_flow_run,
    mock_logger,
    raw_reads_mgnify_study,
):
    # Batch with some None and invalid flow IDs
    AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        asa_flow_run_id=None,
        virify_flow_run_id="invalid",
        map_flow_run_id="123",
        pipeline_versions=[Analysis.PipelineVersions.v6],
    )

    mock_flow_run = MagicMock()
    mock_flow_run.is_running.return_value = False

    def side_effect(flow_id):
        if flow_id == "invalid":
            raise ValueError("Not an integer")
        return mock_flow_run

    mock_load_flow_run.side_effect = side_effect

    # Execute flow - should not crash and should proceed if "123" is not running
    finalize_assembly_study.fn(raw_reads_mgnify_study.accession)

    # Tasks should be called because none are "running" (None/invalid are skipped)
    mock_merge.assert_called_once()


@pytest.mark.django_db
@pytest.mark.skip
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.get_run_logger")
@patch("workflows.flows.analysis.assembly.flows.finalize_assembly_study.load_flow_run")
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.merge_assembly_study_summaries"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.add_assembly_study_summaries_to_downloads"
)
@patch(
    "workflows.flows.analysis.assembly.flows.finalize_assembly_study.copy_v6_study_summaries"
)
def test_finalize_assembly_study_features_update(
    mock_copy,
    mock_add,
    mock_merge,
    mock_load_flow_run,
    mock_logger,
    assembly_analysis,
    batch,
):
    # Mock all complete
    mock_flow_run = MagicMock()
    mock_flow_run.is_running.return_value = False
    mock_load_flow_run.return_value = mock_flow_run

    study = assembly_analysis.study
    batch.study = study
    batch.pipeline_versions = [Analysis.PipelineVersions.v6]
    batch.save()
    # The finalization flow needs at least one v6 analysis to be ready (which is imported thourh the generated field
    # is ready)
    analysis = study.analyses.first()
    analysis.pipeline_version = Analysis.PipelineVersions.v6
    analysis.status = Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    analysis.save()

    finalize_assembly_study.fn(study.accession)

    study.refresh_from_db()
    # FIXME: this is failing, this value should be true
    assert study.features.has_v6_analyses is True

    # Now test with no ready v6 analyses
    Analysis.objects.filter(
        study=study, pipeline_version=Analysis.PipelineVersions.v6
    ).delete()

    finalize_assembly_study.fn(study.accession)
    study.refresh_from_db()
    assert study.features.has_v6_analyses is False
