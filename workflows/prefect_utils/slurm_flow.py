import hashlib
import os
import signal
import subprocess
from datetime import timedelta
from pathlib import Path
from textwrap import dedent as _
from typing import List, Optional, Union

from django.conf import settings
from django.urls import reverse
from prefect import get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NONE
from prefect.runtime import flow_run

from emgapiv2.log_utils import mask_sensitive_data as safe
from workflows.models import OrchestratedClusterJob
from workflows.nextflow_utils.tower import get_nextflow_tower_url
from workflows.nextflow_utils.trace import maybe_get_nextflow_trace_df
from workflows.prefect_utils.flows_utils import (
    close_stale_connections,
)
from workflows.prefect_utils.flows_utils import (
    django_db_task as task,
)
from workflows.prefect_utils.prefect_worker_utils import (
    make_environment,
)
from workflows.prefect_utils.slurm_policies import (
    ResubmitAlwaysPolicy,
    ResubmitIfFailedPolicy,
    _SlurmResubmitPolicy,
)
from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_successfully,
)

EMG_CONFIG = settings.EMG_CONFIG


def slurm_timedelta(delta: timedelta) -> str:
    """
    Rewrite a python timedelta as a slurm duration.
    :param delta: Python timedelta, e.g. `timedelta(minutes=5)
    :return: Slurm duration in days-hours:mins:secs, e.g. `00-00:05:00`
    """
    t_minutes, seconds = divmod(delta.seconds, 60)
    hours, minutes = divmod(t_minutes, 60)
    days = delta.days
    return f"{days:02}-{hours:02}:{minutes:02}:{seconds:02}"


def _ensure_absolute_workdir(workdir):
    path = Path(workdir)
    if not path.is_absolute():
        base_path = Path(EMG_CONFIG.slurm.default_workdir)
        return base_path / path
    return path


class SubprocessFailedException(Exception):
    def __init__(self, pid, exitcode, message=None):
        self.pid = pid
        self.exitcode = exitcode
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self):
        msg = f"Subprocess {self.pid} failed with exit code {self.exitcode}"
        if self.message:
            msg += f"\nDetails: {self.message}"
        return msg


@task
def compute_hash_of_input_file(
    input_files_to_hash: Optional[List[Union[Path, str]]] = None,
) -> str:
    logger = get_run_logger()
    input_files_hash = hashlib.new("blake2b")

    for input_file in input_files_to_hash or []:
        if not Path(input_file).is_file():
            logger.warning(f"Did not find a file to hash at {input_file}. Ignoring it.")
            continue
        with open(input_file, "rb") as f:
            for chunk in iter(
                lambda: f.read(131072), b""
            ):  # 131072 is rsize on EBI /nfs/production, so slightly optimised for that
                input_files_hash.update(chunk)
    return input_files_hash.hexdigest()


@task(log_prints=True)
def store_nextflow_trace(orchestrated_cluster_job: OrchestratedClusterJob):
    job_description: OrchestratedClusterJob.SlurmJobSubmitDescription = (
        orchestrated_cluster_job.job_submit_description
    )
    maybe_trace = maybe_get_nextflow_trace_df(
        workdir=Path(job_description.working_directory), command=job_description.script
    )
    if maybe_trace is not None:
        orchestrated_cluster_job.nextflow_trace = maybe_trace.to_dict(orient="index")
        orchestrated_cluster_job.save()


@task(
    task_run_name="Run job: {name}",
    cache_policy=NONE,
    log_prints=True,
    timeout_seconds=EMG_CONFIG.slurm.cluster_job_flow_timeout_seconds,
)
def run_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    environment: Union[dict, str],
    working_dir: Optional[Path] = None,
    resubmit_policy: Optional[_SlurmResubmitPolicy] = None,
    input_files_to_hash: Optional[List[Union[Path, str]]] = None,
    **kwargs,
) -> OrchestratedClusterJob:
    """Run a command within the SlurmWorker allocation.

    Reuse successful jobs according to the resubmission policy.
    Process failures and timeouts raise exceptions.

    :param name: The job label in Prefect and the database.
    :param command: The shell command to execute.
    :param expected_time: The maximum process duration.
    :param memory: Requested memory, stored with the job metadata.
    :param environment: Environment variables or an sbatch-style export string.
    :param working_dir: The process directory, relative to the configured default if needed.
    :param resubmit_policy: The policy for previous jobs. Defaults to retrying failed jobs.
    :param input_files_to_hash: Input files for job reuse checks.
    :param kwargs: Additional job metadata.
    :return: The completed or reused job record.
    :raises SubprocessFailedException: If the process fails, times out, or reuses an unsuccessful job.
    """
    logger = get_run_logger()

    ### Prepare working directory for job
    job_workdir = _ensure_absolute_workdir(
        working_dir or Path(EMG_CONFIG.slurm.default_workdir)
    )
    job_workdir.mkdir(parents=True, exist_ok=True)
    resubmit_policy = resubmit_policy or ResubmitIfFailedPolicy
    logger.info(f"Will use {job_workdir=}")

    ### Prepare job submission description
    script = _(f"""\
        set -euo pipefail
        {command}
        """)

    logger.info(f"Will run the script ```{safe(script)}```")
    job_submit_description = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=str(job_workdir),
        environment=environment,
        **kwargs,
    )

    # check if a job already exists for this
    hashed_inputs = [
        OrchestratedClusterJob.JobInputFile(
            path=input_file,
            hash=compute_hash_of_input_file([input_file]),
        )
        for input_file in input_files_to_hash or []
    ]
    logger.info(f"Have hashed {len(hashed_inputs)} input files")

    last_submitted_similar_job: Optional[OrchestratedClusterJob] = (
        OrchestratedClusterJob.objects.get_previous_job(
            job=job_submit_description,
            policy=resubmit_policy,
            input_file_hashes=hashed_inputs,
        )
    )

    if last_submitted_similar_job:
        logger.info(f"A similar job exists in history: {last_submitted_similar_job}")
        if last_submitted_similar_job.should_resubmit_according_to_policy(
            resubmit_policy
        ):
            logger.info(
                f"Policy {resubmit_policy.policy_name} states we should resubmit the job."
            )
        else:
            logger.info(
                f"Policy {resubmit_policy.policy_name} states we should not resubmit the job. Using {last_submitted_similar_job}."
            )
            if not slurm_status_is_finished_successfully(
                last_submitted_similar_job.last_known_state
            ):
                raise SubprocessFailedException(
                    last_submitted_similar_job.cluster_job_id,
                    None,
                    f"Reused job is not successful: {last_submitted_similar_job.last_known_state}",
                )
            return last_submitted_similar_job

    if (
        last_submitted_similar_job
        and resubmit_policy.resubmit_needs_preparation_command
    ):
        logger.info(
            f"Policy {resubmit_policy.policy_name} requires a pre-resubmit command: {resubmit_policy.resubmit_needs_preparation_command}."
        )
        run_cluster_job(
            name=f"Preparation for: {name}",
            command=resubmit_policy.resubmit_needs_preparation_command,
            expected_time=timedelta(hours=1),
            memory=f"{EMG_CONFIG.slurm.preparation_command_job_memory_gb}G",
            working_dir=job_workdir,
            resubmit_policy=ResubmitAlwaysPolicy,
            environment={},
        )

    # Refresh Django connections before the first DB write after any preparation job.
    close_stale_connections()
    env = make_environment(environment)

    # need to submit new job
    process = subprocess.Popen(
        ["/bin/bash", "-e", "-u", "-o", "pipefail", "-c", command],
        cwd=job_workdir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        start_new_session=True,
    )

    nf_link = get_nextflow_tower_url()
    nf_link_markdown = f"[Watch Nextflow Workflow]({nf_link})" if nf_link else ""

    ocj = OrchestratedClusterJob.objects.create(
        cluster_job_id=process.pid,
        flow_run_id=flow_run.id,
        job_submit_description=job_submit_description,
        input_files_hashes=hashed_inputs,
    )

    create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(f"""\
            # Subprocess with pid {process.pid}
            [Orchestrated Cluster Job {ocj.id}]({EMG_CONFIG.service_urls.app_root}/{reverse("admin:workflows_orchestratedclusterjob_change", kwargs={"object_id": ocj.id})})
            Submitted a script as a subprocess:
            ~~~
            <<SCRIPT>>
            ~~~
            It will be terminated if not done in {slurm_timedelta(expected_time)}.
            Working dir is {job_workdir}.
            {nf_link_markdown}
            """).replace("<<SCRIPT>>", safe(script)),
    )

    try:
        stdout, stderr = process.communicate(timeout=expected_time.total_seconds())
    except (TimeoutError, subprocess.TimeoutExpired) as e:
        logger.error(f"Failed to run subprocess: {e}")
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = process.communicate()

        # Refresh Django connections before saving after the subprocess wait.
        close_stale_connections()
        ocj.last_known_state = SlurmStatus.failed
        ocj.save()

        raise SubprocessFailedException(
            process.pid, process.returncode, "Timed out"
        ) from e

    # Refresh Django connections before saving after the subprocess wait.
    close_stale_connections()
    if process.returncode == 0:
        logger.info(f"Job {ocj} finished successfully.")

        ocj.last_known_state = SlurmStatus.completed
        ocj.save()

        store_nextflow_trace(ocj)
    else:
        ocj.last_known_state = SlurmStatus.failed
        ocj.save()

        error_details = "\n".join(
            stderr.splitlines()[-EMG_CONFIG.slurm.job_log_failure_tail_lines :]
        )

        raise SubprocessFailedException(process.pid, process.returncode, error_details)

    return ocj
