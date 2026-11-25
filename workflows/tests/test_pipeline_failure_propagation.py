"""Regression tests for process failures and parent flow logs."""

import importlib
from unittest.mock import Mock, patch

import pytest


@pytest.mark.django_db(transaction=True)
def test_asa_dispatch_reports_scheduled_state(
    prefect_harness, raw_reads_mgnify_study, tmp_path
):
    """Keep dispatch completion distinct from pipeline completion."""
    module = importlib.import_module(
        "workflows.flows.analysis.assembly.flows.analysis_assembly_study"
    )
    study = raw_reads_mgnify_study
    batches = [
        Mock(id="batch-1", total_analyses=1),
        Mock(id="batch-2", total_analyses=1),
    ]
    with (
        patch.object(
            module.ena.models.Study.objects,
            "get_ena_study",
            return_value=study.ena_study,
        ),
        patch.object(
            module.analyses.models.Study.objects,
            "get_or_create_for_ena_study",
            return_value=study,
        ),
        patch.object(module, "get_study_assemblies_from_ena", return_value=["ERZ123"]),
        patch.object(module, "create_analyses_for_assemblies"),
        patch.object(
            module.workflows.models.AssemblyAnalysisBatch.objects,
            "get_or_create_batches_for_study",
            return_value=batches,
        ),
        patch.object(module, "run_deployment") as deploy,
    ):
        state = module.analysis_assembly_study(
            study.ena_study.accession, str(tmp_path), return_state=True
        )
    assert state.is_completed()
    assert state.name == "Completed (pipeline runs are scheduled)"
    assert "Scheduled 2" in state.message
    assert "not yet completed" in state.message
    assert deploy.call_count == 2
    assert all(call.kwargs["timeout"] == 0 for call in deploy.call_args_list)
