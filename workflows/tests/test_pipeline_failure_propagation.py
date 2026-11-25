"""Regression tests for process failures and parent flow logs."""

import importlib
from unittest.mock import Mock, patch

import pytest
from prefect.tasks import Task

from workflows.prefect_utils.slurm_flow import (
    SubprocessFailedException,
)


@pytest.mark.parametrize(
    "runner",
    [
        "amplicon.run_amplicon_pipeline_via_samplesheet",
        "raw_reads.run_rawreads_pipeline_via_samplesheet",
    ],
)
@pytest.mark.parametrize(
    "error",
    [
        SubprocessFailedException(123, 7),
    ],
)
def test_samplesheet_failure_is_raised(runner, error, tmp_path):
    """Preserve analysis statuses and propagate process failures."""
    module = importlib.import_module(f"workflows.flows.analyse_study_tasks.{runner}")
    name = runner.split(".")[-1]
    run_pipeline = getattr(module, name)
    assert isinstance(run_pipeline, Task)
    analysis = Mock()
    queryset = Mock()
    queryset.__iter__ = Mock(side_effect=lambda: iter([analysis]))
    with (
        patch.object(
            module.analyses.models.Analysis.objects, "select_related"
        ) as select,
        patch.object(module.analyses.models.Run.objects, "filter"),
        patch.object(module, "queryset_hash", return_value="hash"),
        patch.object(
            module,
            (
                "make_samplesheet_amplicon"
                if name.startswith("run_amplicon")
                else "make_samplesheet_rawreads"
            ),
            return_value=tmp_path / "samplesheet.csv",
        ),
        patch.object(module, "mark_analysis_as_started"),
        patch.object(module, "mark_analysis_as_failed") as mark_failed,
        patch.object(module, "run_cluster_job", side_effect=error),
        patch.object(module, "import_completed_analyses") as import_results,
    ):
        select.return_value.filter.return_value = queryset
        with pytest.raises(type(error)):
            run_pipeline.fn(Mock(), [1], tmp_path / "work", tmp_path / "out")
    mark_failed.assert_called_once_with(analysis)
    import_results.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        SubprocessFailedException(123, 7),
        RuntimeError("submission failed"),
    ],
)
def test_uploader_raises_process_failures(error, tmp_path):
    """Mark upload failures and preserve their original exceptions."""
    module = importlib.import_module("workflows.flows.upload_assembly")
    assembly = Mock(is_private=False)
    with (
        patch.object(module, "run_cluster_job") as run_job,
        patch.object(module, "get_run_logger"),
        patch.object(module, "mark_assembly_status") as mark_status,
    ):
        run_job.with_options.return_value.side_effect = error
        with pytest.raises(type(error)):
            module.submit_assembly_slurm.fn(
                tmp_path / "assembly.manifest", assembly, False
            )
    mark_status.assert_called_once_with(
        assembly, status=assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED
    )


def test_uploader_without_accession_fails(tmp_path):
    """Fail uploads without the required ENA receipt accession."""
    module = importlib.import_module("workflows.flows.upload_assembly")
    assembly = Mock(is_private=False)
    with (
        patch.object(module, "run_cluster_job"),
        patch.object(module, "get_run_logger"),
        patch.object(module, "get_assigned_assembly_accession", return_value=None),
        patch.object(module, "mark_assembly_status") as mark_status,
    ):
        with pytest.raises(RuntimeError, match="No assembly accession"):
            module.submit_assembly_slurm.fn(
                tmp_path / "assembly.manifest", assembly, False
            )
    mark_status.assert_called_once_with(
        assembly, status=assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED
    )


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
