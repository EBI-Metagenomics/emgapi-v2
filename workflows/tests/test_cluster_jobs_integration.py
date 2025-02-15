import logging
import re
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from prefect import flow
from prefect.logging import disable_run_logger
from prefect.runtime import flow_run

from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.slurm_flow import (
    compute_hash_of_input_file,
    run_cluster_job,
)
from workflows.prefect_utils.slurm_policies import (
    DontResubmitIfOnlyInputFilesChangePolicy,
    ResubmitAlwaysPolicy,
    ResubmitIfFailedPolicy,
)
from workflows.prefect_utils.slurm_status import SlurmStatus
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@flow(log_prints=True, retries=2)
def intermittently_buggy_flow_that_includes_a_cluster_job_subflow(workdir: Path):
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
    mock_cluster_can_accept_jobs_yes,
    mock_check_cluster_job_all_completed,
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
        int(job_altered_call.cluster_job_id) == int(job_initial.cluster_job_id) + 1
    )  # slurm job ids just increment up. assumes tests are not being run in parallel :)

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
        intermittently_buggy_flow_that_includes_a_cluster_job_subflow,
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


def test_input_file_hash(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    f1 = tmp_path / "file1.txt"
    f1.touch()

    f2 = tmp_path / "file2.txt"
    f2.touch()

    f3 = tmp_path / "file3.txt"
    with disable_run_logger():
        hash = compute_hash_of_input_file.fn([f1, f2, f3])
    assert f"Did not find a file to hash at {f3}." in caplog.text
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
