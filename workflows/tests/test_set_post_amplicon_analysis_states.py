"""
Characterisation tests for ``set_post_analysis_states`` (plan step 0c).

The pipeline writes two top-level reports; this task is the only place that
turns them into per-run ``Analysis.status`` outcomes. The exact strings asserted
here become the mapping used by the batch flow, so they are pinned now.
"""

from unittest.mock import patch

import pytest
from django.conf import settings

from analyses.models import Analysis
from workflows.fixtures.amplicon.conftest import write_amplicon_run_reports
from workflows.flows.analyse_study_tasks.amplicon.set_post_analysies_states import (
    set_post_analysis_states,
)

EMG_CONFIG = settings.EMG_CONFIG

SANITY_CHECK = (
    "workflows.flows.analyse_study_tasks.amplicon."
    "set_post_analysies_states.sanity_check_amplicon_results"
)


def _run_accession(analysis: Analysis) -> str:
    return analysis.run.first_accession


@pytest.mark.django_db(transaction=True)
@patch(SANITY_CHECK)
def test_qc_passed_run_is_marked_completed_and_sanity_checked(
    mock_sanity_check, prefect_harness, amplicon_analyses_factory, tmp_path
):
    """
    A run listed in the completed-runs CSV becomes ANALYSIS_COMPLETED, keeps
    that CSV's info string as the reason, and has its results sanity-checked
    against ``<outdir>/<run_accession>``.
    """
    analysis = amplicon_analyses_factory(1)[0]
    run = _run_accession(analysis)
    # Pre-set the statuses that a COMPLETED outcome must clear.
    analysis.mark_status(Analysis.AnalysisStates.ANALYSIS_FAILED)
    analysis.mark_status(Analysis.AnalysisStates.ANALYSIS_BLOCKED)

    write_amplicon_run_reports(tmp_path, completed={run: "all_results"}, failed={})

    set_post_analysis_states(tmp_path, [analysis])

    analysis.refresh_from_db()
    states = Analysis.AnalysisStates
    assert analysis.status[states.ANALYSIS_COMPLETED] is True
    assert analysis.status[f"{states.ANALYSIS_COMPLETED}_reason"] == "all_results"
    # COMPLETED clears a previous failure/block
    assert analysis.status[states.ANALYSIS_FAILED] is False
    assert analysis.status[states.ANALYSIS_BLOCKED] is False

    mock_sanity_check.assert_called_once()
    outdir_arg, analysis_arg = mock_sanity_check.call_args.args
    assert outdir_arg == tmp_path / run
    assert analysis_arg.id == analysis.id


@pytest.mark.django_db(transaction=True)
@patch(SANITY_CHECK)
def test_qc_failed_run_is_marked_qc_failed_with_the_csv_reason(
    mock_sanity_check, prefect_harness, amplicon_analyses_factory, tmp_path
):
    """
    A run listed in the failed-runs CSV becomes ANALYSIS_QC_FAILED with the
    pipeline's own reason preserved, and is not sanity-checked.
    """
    analysis = amplicon_analyses_factory(1)[0]
    run = _run_accession(analysis)
    write_amplicon_run_reports(tmp_path, completed={}, failed={run: "seqfu_fail"})

    set_post_analysis_states(tmp_path, [analysis])

    analysis.refresh_from_db()
    states = Analysis.AnalysisStates
    assert analysis.status[states.ANALYSIS_QC_FAILED] is True
    assert analysis.status[f"{states.ANALYSIS_QC_FAILED}_reason"] == "seqfu_fail"
    assert not analysis.status.get(states.ANALYSIS_COMPLETED)
    mock_sanity_check.assert_not_called()


@pytest.mark.django_db(transaction=True)
@patch(SANITY_CHECK)
def test_run_absent_from_both_reports_is_marked_failed(
    mock_sanity_check, prefect_harness, amplicon_analyses_factory, tmp_path
):
    """A run in neither report is ANALYSIS_FAILED, reason "Missing run in execution"."""
    completed, missing = amplicon_analyses_factory(2)
    write_amplicon_run_reports(
        tmp_path, completed={_run_accession(completed): "no_asvs"}, failed={}
    )

    set_post_analysis_states(tmp_path, [completed, missing])

    missing.refresh_from_db()
    states = Analysis.AnalysisStates
    assert missing.status[states.ANALYSIS_FAILED] is True
    assert (
        missing.status[f"{states.ANALYSIS_FAILED}_reason"] == "Missing run in execution"
    )
    assert not missing.status.get(states.ANALYSIS_COMPLETED)


@pytest.mark.django_db(transaction=True)
@patch(SANITY_CHECK)
def test_missing_report_csvs_fail_every_run(
    mock_sanity_check, prefect_harness, amplicon_analyses_factory, tmp_path
):
    """
    No reports at all — which is what a failed cluster job leaves behind —
    means every run is FAILED with "Missing run in execution".
    """
    analyses = amplicon_analyses_factory(3)
    assert not list(tmp_path.iterdir())

    set_post_analysis_states(tmp_path, analyses)

    states = Analysis.AnalysisStates
    for analysis in analyses:
        analysis.refresh_from_db()
        assert analysis.status[states.ANALYSIS_FAILED] is True
        assert (
            analysis.status[f"{states.ANALYSIS_FAILED}_reason"]
            == "Missing run in execution"
        )
    mock_sanity_check.assert_not_called()
