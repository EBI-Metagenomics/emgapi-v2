"""
Characterisation tests for ``import_completed_analyses`` (plan step 0d).

The skip rules are what makes the amplicon flow restartable; the batch shape
leans on them harder, so each branch gets its own test.
"""

from unittest.mock import patch

import pytest

from analyses.models import Analysis
from workflows.flows.analyse_study_tasks.amplicon.import_completed_amplicon_analyses import (
    import_completed_analyses,
)

IMPORT_ONE = (
    "workflows.flows.analyse_study_tasks.amplicon."
    "import_completed_amplicon_analyses.import_completed_analysis"
)


@pytest.fixture
def completed_analysis(amplicon_analyses_factory):
    analysis = amplicon_analyses_factory(1)[0]
    analysis.mark_status(Analysis.AnalysisStates.ANALYSIS_COMPLETED)
    return analysis


@pytest.mark.django_db(transaction=True)
@patch(IMPORT_ONE)
def test_completed_analysis_is_imported_with_results_dir_set_first(
    mock_import_one, prefect_harness, completed_analysis, tmp_path
):
    """
    ``results_dir`` is written to ``<outdir>/<run_accession>`` and saved before
    the per-analysis import runs — the importer reads it back off the model.
    """
    run = completed_analysis.run.first_accession
    expected_dir = tmp_path / run

    def assert_results_dir_already_saved(analysis):
        assert Analysis.objects.get(id=analysis.id).results_dir == str(expected_dir)

    mock_import_one.side_effect = assert_results_dir_already_saved

    import_completed_analyses(tmp_path, [completed_analysis])

    mock_import_one.assert_called_once()
    completed_analysis.refresh_from_db()
    assert completed_analysis.results_dir == str(expected_dir)


@pytest.mark.django_db(transaction=True)
@patch(IMPORT_ONE)
def test_analysis_that_is_not_completed_is_skipped(
    mock_import_one, prefect_harness, amplicon_analyses_factory, tmp_path
):
    analysis = amplicon_analyses_factory(1)[0]
    assert not analysis.status.get(Analysis.AnalysisStates.ANALYSIS_COMPLETED)

    import_completed_analyses(tmp_path, [analysis])

    mock_import_one.assert_not_called()
    analysis.refresh_from_db()
    assert analysis.results_dir is None


@pytest.mark.django_db(transaction=True)
@patch(IMPORT_ONE)
def test_analysis_that_failed_the_sanity_check_is_skipped(
    mock_import_one, prefect_harness, completed_analysis, tmp_path
):
    completed_analysis.mark_status(
        Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED
    )

    import_completed_analyses(tmp_path, [completed_analysis])

    mock_import_one.assert_not_called()


@pytest.mark.django_db(transaction=True)
@patch(IMPORT_ONE)
def test_analysis_that_already_has_taxonomies_is_skipped(
    mock_import_one, prefect_harness, completed_analysis, tmp_path
):
    with_annotations = Analysis.objects_and_annotations.get(id=completed_analysis.id)
    with_annotations.annotations[Analysis.TAXONOMIES] = {"ssu": [{"count": 1}]}
    with_annotations.save()

    import_completed_analyses(tmp_path, [completed_analysis])

    mock_import_one.assert_not_called()


@pytest.mark.django_db(transaction=True)
@patch(IMPORT_ONE)
def test_import_failure_marks_post_sanity_check_failed_and_continues(
    mock_import_one, prefect_harness, amplicon_analyses_factory, tmp_path
):
    """
    An exception from the per-analysis import is recorded on that analysis as
    ANALYSIS_POST_SANITY_CHECK_FAILED with the message, and does not abort the
    remaining analyses.
    """
    first, second = amplicon_analyses_factory(2)
    for analysis in (first, second):
        analysis.mark_status(Analysis.AnalysisStates.ANALYSIS_COMPLETED)

    def fail_for_first(analysis):
        if analysis.id == first.id:
            raise RuntimeError("boom")

    mock_import_one.side_effect = fail_for_first

    import_completed_analyses(tmp_path, [first, second])

    assert mock_import_one.call_count == 2
    first.refresh_from_db()
    states = Analysis.AnalysisStates
    assert first.status[states.ANALYSIS_POST_SANITY_CHECK_FAILED] is True
    assert (
        first.status[f"{states.ANALYSIS_POST_SANITY_CHECK_FAILED}_reason"]
        == "Failed during import: boom"
    )
    second.refresh_from_db()
    assert not second.status.get(states.ANALYSIS_POST_SANITY_CHECK_FAILED)
