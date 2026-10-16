"""
Characterisation tests for ``analysis_amplicon_study`` as it behaves today.

These pin behaviour that the planned batch refactor must preserve:

* chunking (0b) — the set of analyses handed to each pipeline invocation;
* the study-level tail (0e) — the merge / download / copy / cleanup / event steps
  that run after the chunk loop, and which the refactor moves into a separate
  ``finalize_amplicon_study`` flow;
* the input gate (0f) — amplicon suspends unconditionally, always asks for a
  biome, re-queries ENA when the policies change, and refuses a private study
  with no webin owner.

They deliberately assert on call arguments rather than on logs, so that the same
assertions can be re-pointed at the post-refactor flows unchanged.
"""

from pathlib import Path
from unittest.mock import DEFAULT, patch

import pytest
from django.conf import settings

import ena.models
from analyses.models import Analysis
from workflows.ena_utils.ena_policies import (
    ENALibrarySourcePolicy,
    ENALibraryStrategyPolicy,
)
from workflows.flows.analysis import AnalysisType
from workflows.flows.analysis_amplicon_study import analysis_amplicon_study

EMG_CONFIG = settings.EMG_CONFIG

STUDY_FLOW = "workflows.flows.analysis_amplicon_study"

# Everything the study flow calls that would hit ENA, Slurm, the filesystem or
# the Prefect event bus. Patched as one block so each test only names the mocks
# it actually asserts on.
_PATCHED = [
    "get_study_readruns_from_ena",
    "create_analyses",
    "run_amplicon_pipeline_via_samplesheet",
    "merge_study_summaries",
    "merge_dwc_ready_summaries",
    "add_study_summaries_to_downloads",
    "add_dwcr_summaries_to_downloads",
    "copy_v6_study_summaries",
    "delete_study_nextflow_workdir",
    "emit_event",
]


@pytest.fixture
def study_flow_mocks():
    """Patch the study flow's external calls; yields ``{name: mock}``."""
    with patch.multiple(STUDY_FLOW, **{name: DEFAULT for name in _PATCHED}) as mocks:
        mocks["get_study_readruns_from_ena"].return_value = []
        yield mocks


@pytest.fixture
def resume_with_input(mock_suspend_flow_run, biome_choices, user_choices, admin_user):
    """
    Pretend a human resumed the flow from the input form.

    Returns a callable so each test can choose the policies / webin owner it needs.
    """

    def _resume(
        library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
        library_source_policy=ENALibrarySourcePolicy.OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME,
        webin_owner=None,
        watchers=None,
    ):
        class _Input:
            def __init__(self):
                self.biome = biome_choices["root.engineered"]
                self.watchers = watchers
                self.library_strategy_policy = library_strategy_policy
                self.library_source_policy = library_source_policy
                self.webin_owner = webin_owner

        mock_suspend_flow_run.return_value = _Input()
        return mock_suspend_flow_run

    return _resume


def _study_workdir(ena_accession: str) -> Path:
    return (
        Path(EMG_CONFIG.slurm.default_nextflow_workdir)
        / ena_accession
        / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{EMG_CONFIG.amplicon_pipeline.pipeline_version}"
    )


def _study_outdir(ena_accession: str) -> Path:
    return (
        Path(EMG_CONFIG.slurm.default_workdir)
        / ena_accession
        / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{EMG_CONFIG.amplicon_pipeline.pipeline_version}"
    )


#############
# 0b — chunking
#############


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_chunking_partitions_analyses_to_attempt(
    prefect_harness,
    monkeypatch,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    With 5 analyses and a chunk size of 2, the pipeline flow is invoked 3 times
    with disjoint analysis-id lists whose union is exactly the set of analyses to
    attempt, in ascending analysis-id order.

    This is the contract the batch rows must reproduce.
    """
    monkeypatch.setattr(EMG_CONFIG.amplicon_pipeline, "samplesheet_chunk_size", 2)
    analyses = amplicon_analyses_factory(5)
    expected_ids = [analysis.id for analysis in analyses]
    resume_with_input()

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    run_pipeline = study_flow_mocks["run_amplicon_pipeline_via_samplesheet"]
    chunks = [list(call.args[1]) for call in run_pipeline.call_args_list]

    assert chunks == [expected_ids[0:2], expected_ids[2:4], expected_ids[4:5]]
    # disjoint, and the union is exactly get_analyses_to_attempt's result
    flattened = [analysis_id for chunk in chunks for analysis_id in chunk]
    assert len(set(flattened)) == len(flattened)
    assert set(flattened) == set(expected_ids)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_chunking_passes_study_scoped_work_and_out_dirs(
    prefect_harness,
    monkeypatch,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    Every chunk is handed the same study-scoped workdir and outdir; the
    per-chunk directory is derived inside the pipeline flow from the samplesheet
    hash. The refactor changes this to batch-id-derived paths, so pin it here.
    """
    monkeypatch.setattr(EMG_CONFIG.amplicon_pipeline, "samplesheet_chunk_size", 2)
    amplicon_analyses_factory(3)
    resume_with_input()
    ena_accession = raw_reads_mgnify_study.ena_study.accession

    analysis_amplicon_study(ena_accession)

    run_pipeline = study_flow_mocks["run_amplicon_pipeline_via_samplesheet"]
    assert run_pipeline.call_count == 2
    for call in run_pipeline.call_args_list:
        study, _chunk, workdir, outdir = call.args
        assert study.accession == raw_reads_mgnify_study.accession
        assert workdir == _study_workdir(ena_accession)
        assert outdir == _study_outdir(ena_accession)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_chunking_excludes_imported_and_qc_failed_analyses(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    Already-imported, QC-failed and blocked analyses are not attempted again —
    this is what makes the current flow restartable, and the batch flow's
    idempotent member selection has to preserve it.
    """
    analyses = amplicon_analyses_factory(4)
    analyses[0].mark_status(Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    analyses[1].mark_status(Analysis.AnalysisStates.ANALYSIS_QC_FAILED)
    analyses[2].mark_status(Analysis.AnalysisStates.ANALYSIS_BLOCKED)
    resume_with_input()

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    run_pipeline = study_flow_mocks["run_amplicon_pipeline_via_samplesheet"]
    assert [list(call.args[1]) for call in run_pipeline.call_args_list] == [
        [analyses[3].id]
    ]


#############
# 0e — the study-level tail, i.e. the part that moves to finalize_amplicon_study
#############


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_study_tail_calls_each_study_level_step_once(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    Pins the merge / download / copy / cleanup calls and their arguments.

    Re-point these assertions at ``finalize_amplicon_study`` in Phase 4 — that
    re-pointing is the proof the move was faithful.
    """
    analyses = amplicon_analyses_factory(2)
    resume_with_input()
    accession = raw_reads_mgnify_study.accession
    ena_accession = raw_reads_mgnify_study.ena_study.accession
    cleanup_partials = not EMG_CONFIG.amplicon_pipeline.keep_study_summary_partials

    analysis_amplicon_study(ena_accession)

    study_flow_mocks["merge_study_summaries"].assert_called_once_with(
        accession,
        cleanup_partials=cleanup_partials,
        analysis_type=AnalysisType.AMPLICON,
    )
    study_flow_mocks["merge_dwc_ready_summaries"].assert_called_once_with(
        accession, cleanup_partials=cleanup_partials
    )
    study_flow_mocks["add_study_summaries_to_downloads"].assert_called_once_with(
        accession, analysis_type=AnalysisType.AMPLICON
    )
    study_flow_mocks["add_dwcr_summaries_to_downloads"].assert_called_once_with(
        accession
    )
    study_flow_mocks["copy_v6_study_summaries"].assert_called_once_with(
        accession, analysis_type=AnalysisType.AMPLICON
    )
    study_flow_mocks["delete_study_nextflow_workdir"].assert_called_once()
    workdir_arg, analyses_arg = study_flow_mocks[
        "delete_study_nextflow_workdir"
    ].call_args.args
    assert workdir_arg == _study_workdir(ena_accession)
    assert list(analyses_arg) == [analysis.id for analysis in analyses]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
@pytest.mark.parametrize("imported", [True, False])
def test_amplicon_study_tail_sets_has_v6_analyses_from_ready_analyses(
    imported,
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """``features.has_v6_analyses`` is True iff a v6-family analysis is_ready."""
    analyses = amplicon_analyses_factory(1)
    if imported:
        analyses[0].mark_status(Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    resume_with_input()

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    raw_reads_mgnify_study.refresh_from_db()
    assert raw_reads_mgnify_study.features.has_v6_analyses is imported


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_study_tail_emits_finished_event_with_status_counts(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
    admin_user,
    user_choices,
):
    """
    Pins the exact ``flow.analysis.finished`` payload.

    The counts are computed in the process that just ran everything, which is
    why this event has to move into the finalize flow once batch dispatch
    becomes fire-and-forget.
    """
    analyses = amplicon_analyses_factory(5)
    analyses[0].mark_status(Analysis.AnalysisStates.ANALYSIS_COMPLETED)
    analyses[1].mark_status(Analysis.AnalysisStates.ANALYSIS_FAILED)
    analyses[2].mark_status(Analysis.AnalysisStates.ANALYSIS_QC_FAILED)
    analyses[3].mark_status(Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED)
    analyses[4].mark_status(Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    resume_with_input(watchers=[user_choices[admin_user.username]])

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    emit_event = study_flow_mocks["emit_event"]
    emit_event.assert_called_once()
    assert emit_event.call_args.kwargs["event"] == "flow.analysis.finished"
    assert emit_event.call_args.kwargs["payload"] == {
        "successful": 1,
        "failed": 3,
        "imported": 1,
        "total": 5,
        "study_watchers": [admin_user.username],
    }


#############
# 0f — the input gate
#############


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_input_gate_suspends_unconditionally_and_always_asks_for_biome(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    Amplicon suspends for input on every run, even for a public study whose
    biome is already set — unlike ASA, which suspends only when it needs
    something. The form always carries a (required) biome field.
    """
    amplicon_analyses_factory(1)
    assert raw_reads_mgnify_study.biome is not None
    suspend = resume_with_input()

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    suspend.assert_called_once()
    run_input = suspend.call_args.kwargs["wait_for_input"]
    assert "biome" in run_input.model_fields
    assert run_input.model_fields["biome"].is_required()


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_input_gate_refetches_read_runs_when_policies_change(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """
    A non-default library strategy/source policy triggers a second ENA
    read-run query, this time with ``raise_on_empty=True`` and both policies.
    """
    amplicon_analyses_factory(1)
    resume_with_input(
        library_strategy_policy=ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES
    )

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    fetch = study_flow_mocks["get_study_readruns_from_ena"]
    assert fetch.call_count == 2
    first, second = fetch.call_args_list
    assert first.kwargs["raise_on_empty"] is False
    assert "library_strategy_policy" not in first.kwargs
    assert second.kwargs["raise_on_empty"] is True
    assert (
        second.kwargs["library_strategy_policy"]
        == ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES
    )
    assert (
        second.kwargs["library_source_policy"]
        == ENALibrarySourcePolicy.OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_input_gate_does_not_refetch_read_runs_for_default_policies(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    amplicon_analyses_factory,
    raw_reads_mgnify_study,
):
    """Default policies mean one ENA read-run query, not two."""
    amplicon_analyses_factory(1)
    resume_with_input()

    analysis_amplicon_study(raw_reads_mgnify_study.ena_study.accession)

    assert study_flow_mocks["get_study_readruns_from_ena"].call_count == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("mock_suspend_flow_run", [STUDY_FLOW], indirect=True)
def test_amplicon_input_gate_rejects_private_study_without_webin_owner(
    prefect_harness,
    study_flow_mocks,
    resume_with_input,
    top_level_biomes,
):
    """A private study resumed with no webin owner raises ValueError."""
    private_ena_study = ena.models.Study.objects.create(
        accession="PRJNA111111", title="Private, no owner", is_private=True
    )
    resume_with_input(webin_owner=None)

    with pytest.raises(ValueError, match="private, but no webin owner"):
        analysis_amplicon_study(private_ena_study.accession)

    study_flow_mocks["create_analyses"].assert_not_called()
    study_flow_mocks["run_amplicon_pipeline_via_samplesheet"].assert_not_called()
