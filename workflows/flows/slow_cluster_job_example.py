import shutil
from datetime import timedelta
from pathlib import Path

from prefect import flow

from activate_django_first import EMG_CONFIG

from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.slurm_policies import _SlurmResubmitPolicy, ANYTHING


@flow(
    name="Slow running cluster job example",
    log_prints=True,
)
def slow_cluster_job_example(
    run_for_seconds: int, slurm_resubmit_policy_minutes_ago: int = 60
):
    assert run_for_seconds > 10

    ResubmitIfOlderThanXPolicy = _SlurmResubmitPolicy(
        policy_name=f"Do not resubmit if identical job was submitted more recently than {slurm_resubmit_policy_minutes_ago} minutes ago",
        given_previous_job_submitted_after=timedelta(
            minutes=-slurm_resubmit_policy_minutes_ago
        ),
        if_status_matches=ANYTHING,
        then_resubmit=False,
    )

    command = f"HOW_LONG={run_for_seconds}; for ((i=0; i<HOW_LONG; i+=5)); do date; sleep 5; done"

    workdir = Path(EMG_CONFIG.slurm.default_workdir) / "slow-example"

    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(exist_ok=True)

    run_cluster_job(
        name=f"Tell the time every 5 seconds for {run_for_seconds} seconds",
        command=command,
        expected_time=timedelta(seconds=run_for_seconds + 10),
        memory="100M",
        resubmit_policy=ResubmitIfOlderThanXPolicy,
        working_dir=workdir,
        environment={},
    )
