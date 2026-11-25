import logging
import re
import time
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from django.db import connection
from prefect import flow
from prefect.runtime import flow_run

from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.slurm_flow import (
    SubprocessFailedException,
    compute_hash_of_input_file,
)
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job as run_cluster_job_task,
)
from workflows.prefect_utils.slurm_policies import (
    DontResubmitIfOnlyInputFilesChangePolicy,
    ResubmitAlwaysPolicy,
    ResubmitIfFailedPolicy,
    _SlurmResubmitPolicy,
)
from workflows.prefect_utils.slurm_status import SlurmStatus
from workflows.prefect_utils.testing_utils import (
    combine_caplog_records,
    run_flow_and_capture_logs,
)


@flow
def run_cluster_job(**kwargs):
    """Run job tasks within a flow for integration tests."""
    return run_cluster_job_task(**kwargs)


@pytest.fixture
def mock_slurm_flow_run():
    with patch(
        "workflows.prefect_utils.slurm_flow.flow_run",
        new=type("FlowRun", (), {"id": uuid.uuid4()})(),
    ):
        yield


@flow(log_prints=True, retries=2)
def intermittently_buggy_flow_that_includes_a_job_task(workdir: Path):
    print("starting flow")
    orchestrated_job = run_cluster_job(
        name="test job in buggy flow",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=workdir,
    )
    print(f"JOB ID = {orchestrated_job.cluster_job_id}")
    if flow_run.run_count == 1:
        raise Exception("Failing first time")

    else:
        print(f"Not failing because on {flow_run.run_count = }")

    return orchestrated_job.cluster_job_id


@pytest.mark.django_db
def test_run_cluster_job_state_persistence(
    prefect_harness,
    tmp_path,
):
    job_initial = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        working_dir=tmp_path / "work",  # should be made automatically
    )

    assert (tmp_path / "work").exists()

    # exactly the same inputs should NOT start another cluster job
    # we assume identical calls should not usually start identical another job

    job_repeat_call = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=tmp_path / "work",
    )
    assert (
        job_initial.cluster_job_id == job_repeat_call.cluster_job_id
    )  # same job ID as before
    assert (
        job_initial.id == job_repeat_call.id
    )  # also the exact same Orchestrated Cluster Job
    # in theory the updated_at should have increased, but not here because of the test mock

    # a change to the params should start a new job
    job_altered_call = run_cluster_job(
        name="test job",
        command="echo 'test but different'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=tmp_path / "work2",
    )
    assert (
        job_altered_call.cluster_job_id != job_initial.cluster_job_id
    )  # A new process has a different PID.

    # we can use a different policy to ensure a job is resubmitted even if it previously ran
    job_explicitly_resubmitted_call = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,  # < ensures the initial job is not used
        working_dir=tmp_path / "work",
    )
    assert (
        job_initial.cluster_job_id != job_explicitly_resubmitted_call.cluster_job_id
    )  # different job id

    # automatic retries of a buggy flow that fails after a cluster job should not resubmit cluster job
    logged_buggy_flow = run_flow_and_capture_logs(
        intermittently_buggy_flow_that_includes_a_job_task,
        workdir=tmp_path / "work-buggy",
    )
    assert "Failing first time" in logged_buggy_flow.logs
    assert "Not failing because on flow_run.run_count = 2" in logged_buggy_flow.logs

    job_ids_mentioned = re.findall(r"JOB ID\s*=\s*(\d+)", logged_buggy_flow.logs)
    unique_job_ids = set(job_ids_mentioned)
    assert len(unique_job_ids) == 1
    # (flow ran twice, job started once)

    with open(f"{tmp_path}/my_inputs.csv", "w") as file:
        file.write("my,initial,params")

    # cluster jobs can accept a list of input files to hash
    job_initial_with_hash = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=tmp_path,
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )

    # if input file unchanged, should be same job
    job_repeat_with_hash = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=tmp_path,
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )
    assert job_initial_with_hash.cluster_job_id == job_repeat_with_hash.cluster_job_id

    # if input file changes, should be new job
    with open(f"{tmp_path}/my_inputs.csv", "w") as file:
        file.write("my,altered,params")

    job_repeat_with_hash = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitIfFailedPolicy,
        working_dir=tmp_path,
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )
    assert job_initial_with_hash.cluster_job_id != job_repeat_with_hash.cluster_job_id

    # if input file changes, but policy ignores that, should NOT be new job
    with open(f"{tmp_path}/my_inputs.csv", "w") as file:
        file.write("even,more,altered,params")

    job_repeat_with_changed_hash = run_cluster_job(
        name="test job",
        command="echo 'test'",
        expected_time=timedelta(minutes=1),
        memory="100M",
        environment={},
        resubmit_policy=DontResubmitIfOnlyInputFilesChangePolicy,
        working_dir=tmp_path,
        input_files_to_hash=[tmp_path / "my_inputs.csv"],
    )
    assert (
        job_repeat_with_changed_hash.cluster_job_id
        == job_repeat_with_hash.cluster_job_id
    )


def test_input_file_hash(tmp_path, caplog, prefect_harness):
    caplog.set_level(logging.WARNING)
    f1 = tmp_path / "file1.txt"
    f1.touch()

    f2 = tmp_path / "file2.txt"
    f2.touch()

    f3 = tmp_path / "file3.txt"
    hash = compute_hash_of_input_file([f1, f2, f3])
    caplog_text = combine_caplog_records(caplog.records)
    assert f"Did not find a file to hash at {f3}." in caplog_text
    assert hash.startswith("786a02f7")


@pytest.mark.django_db
def test_slurm_resubmit_policies():
    jsd1 = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name="My first job",
        time_limit="1-0:0:0",
        script="man dalorian",
        working_directory="/nfs",
        memory_per_node="1",
    )

    job1 = OrchestratedClusterJob.objects.create(
        cluster_job_id=1,
        job_submit_description=jsd1,
        flow_run_id=uuid.uuid4(),
        input_files_hashes=[],
    )

    assert job1.job_submit_description.script == "man dalorian"

    # an identical job description...
    jsd2 = OrchestratedClusterJob.SlurmJobSubmitDescription(**jsd1.model_dump())

    matching_jobs = OrchestratedClusterJob.objects.filter_similar_to_by_policy(
        policy=ResubmitAlwaysPolicy, job=jsd2
    )

    assert matching_jobs.count() == 1

    fully_matching_job: OrchestratedClusterJob = (
        OrchestratedClusterJob.objects.get_previous_job(
            policy=ResubmitAlwaysPolicy, job=jsd2, input_file_hashes=[]
        )
    )

    assert fully_matching_job == job1

    assert fully_matching_job.should_resubmit_according_to_policy(ResubmitAlwaysPolicy)

    # should not match if we only want to match previously failed jobs
    matching_jobs = OrchestratedClusterJob.objects.filter_similar_to_by_policy(
        policy=ResubmitIfFailedPolicy, job=jsd2
    )
    assert matching_jobs.count() == 1
    # still matches a job

    fully_matching_job: OrchestratedClusterJob = (
        OrchestratedClusterJob.objects.get_previous_job(
            policy=ResubmitIfFailedPolicy, job=jsd2, input_file_hashes=[]
        )
    )

    assert not fully_matching_job.should_resubmit_according_to_policy(
        ResubmitIfFailedPolicy
    )

    # but if previous job failed, we would expect an instruction to resubmit it
    job1.last_known_state = SlurmStatus.failed
    job1.save()
    job1.refresh_from_db()

    assert job1.should_resubmit_according_to_policy(ResubmitIfFailedPolicy)

    # a novel job should match no previous
    jsd3 = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name="My second job",
        time_limit="2-0:0:0",
        script="run kessel -p 12",
        working_directory="/nfs",
        memory_per_node="1",
    )

    matching_jobs = OrchestratedClusterJob.objects.filter_similar_to_by_policy(
        policy=ResubmitIfFailedPolicy, job=jsd3
    )
    assert matching_jobs.count() == 0


@patch("workflows.prefect_utils.slurm_flow.store_nextflow_trace")
@patch("workflows.prefect_utils.slurm_flow.get_run_logger", return_value=MagicMock())
@patch("workflows.prefect_utils.slurm_flow.create_markdown_artifact")
@pytest.mark.django_db(transaction=True)
def test_run_cluster_job_recovers_stale_connection(
    mock_create_markdown_artifact,
    mock_get_run_logger,
    mock_store_nextflow_trace,
    mock_slurm_flow_run,
    tmp_path,
):
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION idle_session_timeout = '200ms'")

    result = run_cluster_job_task.fn(
        name="subprocess stale connection test",
        command="sleep 0.5",
        expected_time=timedelta(seconds=2),
        memory="100M",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        working_dir=tmp_path / "work",
    )

    assert result.last_known_state == SlurmStatus.completed
    connection.close()


@patch("workflows.prefect_utils.slurm_flow.store_nextflow_trace")
@patch(
    "workflows.prefect_utils.slurm_flow.run_cluster_job",
    side_effect=lambda *args, **kwargs: time.sleep(0.5),
)
@patch(
    "workflows.models.OrchestratedClusterJob.objects.get_previous_job",
)
@patch("workflows.prefect_utils.slurm_flow.get_run_logger", return_value=MagicMock())
@patch("workflows.prefect_utils.slurm_flow.create_markdown_artifact")
@pytest.mark.django_db(transaction=True)
def test_run_cluster_job_recovers_stale_connection_after_preparation_job(
    mock_create_markdown_artifact,
    mock_get_run_logger,
    mock_get_previous_job,
    mock_run_cluster_job,
    mock_store_nextflow_trace,
    mock_slurm_flow_run,
    tmp_path,
):
    prior_job = MagicMock()
    prior_job.should_resubmit_according_to_policy.return_value = True
    mock_get_previous_job.return_value = prior_job
    resubmit_policy = _SlurmResubmitPolicy(
        policy_name="Resubmit subprocess with preparation command",
        if_status_matches=[SlurmStatus.failed],
        then_resubmit=True,
        resubmit_needs_preparation_command="sleep 0.5",
    )

    with connection.cursor() as cursor:
        cursor.execute("SET SESSION idle_session_timeout = '200ms'")

    result = run_cluster_job_task.fn(
        name="subprocess stale connection with preparation test",
        command="echo 'test'",
        expected_time=timedelta(seconds=2),
        memory="100M",
        environment={},
        resubmit_policy=resubmit_policy,
        working_dir=tmp_path / "work",
    )

    assert result.last_known_state == SlurmStatus.completed
    mock_run_cluster_job.assert_called_once()
    connection.close()


@patch("workflows.prefect_utils.slurm_flow.store_nextflow_trace")
@patch("workflows.prefect_utils.slurm_flow.get_run_logger", return_value=MagicMock())
@patch("workflows.prefect_utils.slurm_flow.create_markdown_artifact")
@pytest.mark.django_db(transaction=True)
def test_run_cluster_job_timeout_recovers_stale_connection(
    mock_create_markdown_artifact,
    mock_get_run_logger,
    mock_store_nextflow_trace,
    mock_slurm_flow_run,
    tmp_path,
):
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION idle_session_timeout = '200ms'")

    with pytest.raises(SubprocessFailedException, match="Timed out"):
        run_cluster_job_task.fn(
            name="subprocess stale connection timeout test",
            command="sleep 0.5",
            expected_time=timedelta(seconds=0.3),
            memory="100M",
            environment={},
            resubmit_policy=ResubmitAlwaysPolicy,
            working_dir=tmp_path / "work",
        )

    connection.close()


@pytest.mark.django_db(transaction=True)
def test_job_uses_environment_and_relative_workdir(
    prefect_harness, tmp_path, monkeypatch
):
    """Preserve environment exports and resolve the job directory."""
    monkeypatch.setattr(
        "workflows.prefect_utils.slurm_flow.EMG_CONFIG.slurm.default_workdir",
        str(tmp_path),
    )
    result = run_cluster_job(
        name="environment test",
        command='printf "%s" "$PIPELINE_LABEL"',
        expected_time=timedelta(seconds=5),
        memory="100M",
        environment={"PIPELINE_LABEL": "expected-label"},
        working_dir=Path("relative"),
    )
    assert result.job_submit_description.working_directory == str(tmp_path / "relative")
    assert result.cluster_log.strip() == "expected-label"


@pytest.mark.django_db(transaction=True)
def test_default_policy_retries_failed_process(prefect_harness, tmp_path):
    """Retry failed commands and reuse their subsequent successful result."""
    kwargs = dict(
        name="retry failed process",
        command="if [ ! -f attempted ]; then touch attempted; exit 7; fi",
        expected_time=timedelta(seconds=5),
        memory="100M",
        environment={},
        working_dir=tmp_path,
    )
    with pytest.raises(SubprocessFailedException, match="exit code 7"):
        run_cluster_job(**kwargs)
    completed = run_cluster_job(**kwargs)
    reused = run_cluster_job(**kwargs)
    assert completed.last_known_state == SlurmStatus.completed
    assert reused.id == completed.id
    assert OrchestratedClusterJob.objects.count() == 2
