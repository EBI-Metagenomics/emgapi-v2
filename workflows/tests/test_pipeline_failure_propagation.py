"""Regression tests for process failures and parent flow logs."""

import importlib
import time
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest
from prefect import flow, get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.tasks import Task

from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.slurm_flow import (
    SubprocessFailedException,
    run_cluster_job,
)
from workflows.prefect_utils.slurm_policies import NeverResubmitPolicy
from workflows.prefect_utils.slurm_status import SlurmStatus
from workflows.prefect_utils.testing_utils import get_logs_for_flow_run


@pytest.mark.django_db(transaction=True)
def test_subprocess_failure_reaches_parent_and_logs(prefect_harness, tmp_path):
    """Fail the parent and report process output without a job subflow."""

    @flow
    def parent():
        return run_cluster_job(
            name="failing pipeline",
            command="printf 'pipeline stdout marker\\n'; printf 'stderr marker\\n' >&2; exit 7",
            expected_time=timedelta(seconds=5),
            memory="100M",
            environment={},
            working_dir=tmp_path,
            resubmit_policy=NeverResubmitPolicy,
        )

    state = parent(return_state=True)
    assert state.is_failed()
    with pytest.raises(SubprocessFailedException, match="exit code 7"):
        state.result()
    # A failed historical process must not become a successful task on reuse.
    repeated = parent(return_state=True)
    assert repeated.is_failed()
    with pytest.raises(SubprocessFailedException, match="Reused job is not successful"):
        repeated.result()

    job = OrchestratedClusterJob.objects.get()
    assert job.last_known_state == SlurmStatus.failed
    assert "pipeline stdout marker" in job.cluster_log
    assert "stderr marker" in job.cluster_log
    assert str(job.flow_run_id) == str(state.state_details.flow_run_id)
    # Prefect saves task states and logs asynchronously after the flow returns.
    deadline = time.monotonic() + 10
    with get_client(sync_client=True) as client:
        while True:
            tasks = client.read_task_runs(
                flow_run_filter=FlowRunFilter(
                    id=FlowRunFilterId(any_=[job.flow_run_id])
                )
            )
            failed_job_tasks = [
                task
                for task in tasks
                if task.name == "Run job: failing pipeline"
                and task.state is not None
                and task.state.is_failed()
            ]
            logs = get_logs_for_flow_run(state.state_details.flow_run_id)
            output_recorded = (
                "pipeline stdout marker" in logs and "stderr marker" in logs
            )
            if (failed_job_tasks and output_recorded) or time.monotonic() >= deadline:
                break
            time.sleep(0.1)
    assert failed_job_tasks, [(task.name, task.state) for task in tasks]
    assert "pipeline stdout marker" in logs
    assert "stderr marker" in logs
    assert all(task.flow_run_id == job.flow_run_id for task in tasks)
    assert not any(task.flow_run_id is None for task in tasks)


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


@pytest.mark.parametrize("failed", [False, True])
def test_nextflow_log_is_reported_after_process_exit(tmp_path, failed):
    """Report Nextflow diagnostics after success and preserve process exceptions."""
    module = importlib.import_module("workflows.prefect_utils.slurm_flow")
    log_path = tmp_path / ".nextflow.log"
    log_path.write_text("INFO starting\nERROR test diagnostic\n")
    with (
        patch.object(module, "get_run_logger"),
        patch.object(
            module.OrchestratedClusterJob.objects, "get_previous_job", return_value=None
        ),
        patch.object(module.OrchestratedClusterJob.objects, "create"),
        patch.object(module, "create_markdown_artifact"),
        patch.object(module, "store_nextflow_trace"),
        patch.object(module.subprocess, "Popen") as popen,
        patch("workflows.nextflow_utils.logs.get_run_logger") as get_logger,
    ):
        process = popen.return_value
        process.pid = 123
        process.returncode = 7 if failed else 0
        process.communicate.return_value = ("", "")
        kwargs = dict(
            name="Nextflow diagnostic test",
            command="nextflow run pipeline",
            expected_time=timedelta(seconds=5),
            memory="100M",
            environment={},
            working_dir=tmp_path,
        )
        if failed:
            with pytest.raises(SubprocessFailedException, match="exit code 7"):
                run_cluster_job.fn(**kwargs)
        else:
            run_cluster_job.fn(**kwargs)
    get_logger.return_value.log.assert_called_once_with(
        40, "Nextflow log: %s", log_path
    )
