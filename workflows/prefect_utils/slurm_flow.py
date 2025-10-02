import hashlib
import logging
import os
import subprocess
import time
from datetime import timedelta
from pathlib import Path
from textwrap import dedent as _
from typing import List, Optional, Union

from django.conf import settings
from django.urls import reverse
from django.utils.timezone import now
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run
from prefect_slurm.worker import SlurmWorker

from emgapiv2.log_utils import mask_sensitive_data as safe
from workflows.models import OrchestratedClusterJob
from workflows.nextflow_utils.tower import get_nextflow_tower_url
from workflows.nextflow_utils.trace import maybe_get_nextflow_trace_df
from workflows.prefect_utils.prefect_worker_utils import (
    get_prefect_worker_type,
    make_environment,
)
from workflows.prefect_utils.slurm_limits import delay_until_cluster_has_space
from workflows.prefect_utils.slurm_policies import (
    ResubmitAlwaysPolicy,
    _SlurmResubmitPolicy,
)
from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_successfully,
    slurm_status_is_finished_unsuccessfully,
)

if "PYTEST_VERSION" in os.environ:
    logging.debug("Unit testing, so patching pyslurm.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm
else:
    try:
        import pyslurm  # type: ignore
    except:  # noqa: E722
        logging.warning("No PySlurm available. Patching.")
        import workflows.prefect_utils.pyslurm_patch as pyslurm

EMG_CONFIG = settings.EMG_CONFIG

CLUSTER_WORKPOOL = "slurm"
SLURM_JOB_ID = "Slurm Job ID"
SLURM_UNSAFE_CHARS = ["|", "\n"]


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


def check_cluster_job(
    orchestrated_cluster_job: OrchestratedClusterJob,
) -> str:
    """
    Retrieve the state (e.g. RUNNING) of a cluster job on slurm.
    Updates the state of any associated OrchestratedClusterJob objects.
    :param orchestrated_cluster_job: Orchestrated Cluster Job referencing the Slurm job
    :return: state of the job, as one of the string values of SlurmStatus.
    """
    logger = get_run_logger()
    logger.info(f"Checking job {orchestrated_cluster_job}")

    job_id = orchestrated_cluster_job.cluster_job_id

    try:
        job = pyslurm.db.Job(job_id).load(job_id)
    except pyslurm.core.error.RPCError:
        logger.warning(f"Error talking to slurm for job {job_id}")
        orchestrated_cluster_job.last_known_state = SlurmStatus.unknown.value
        orchestrated_cluster_job.state_checked_at = now()
        orchestrated_cluster_job.save()
        return SlurmStatus.unknown.value

    logger.info(f"SLURM status of {job_id = } is {job.state}")

    orchestrated_cluster_job.last_known_state = job.state
    orchestrated_cluster_job.state_checked_at = now()

    job_log_path = Path(job.working_directory) / Path(f"slurm-{job_id}.out")
    if job_log_path.exists():
        with open(job_log_path, "r", encoding="utf-8", errors="ignore") as job_log:
            full_log = job_log.readlines()
            log = "\n".join(full_log[-EMG_CONFIG.slurm.job_log_tail_lines :])
            logger.info(
                _(
                    f"""\
                    Slurm Job Stdout Log (last {EMG_CONFIG.slurm.job_log_tail_lines} lines of {len(full_log)}):
                    ----------
                    <<LOG>>
                    ----------
                    """
                ).replace("<<LOG>>", safe(log))
            )

            orchestrated_cluster_job.cluster_log = log
    else:
        logger.info(f"No Slurm Job Stdout available at {job_log_path}")
    orchestrated_cluster_job.save()
    return job.state


def _ensure_absolute_workdir(workdir):
    path = Path(workdir)
    if not path.is_absolute():
        base_path = Path(EMG_CONFIG.slurm.default_workdir)
        return base_path / path
    return path


@task(
    task_run_name="Submit job to cluster: {name}",
    log_prints=True,
)
def submit_cluster_job(
    name: str,
    job_submit_description: OrchestratedClusterJob.SlurmJobSubmitDescription,
    **kwargs,
) -> str:
    """
    Launches a job on the HPC cluster.
    This is not-cached: it will submit a slurm job whenever this task is called.

    :param name: A name, purely to help identifying this task run in prefect.
    :param job_submit_description:  The job params that will be passed to slurm submission.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Job ID of the slurm job. Usually an int-as-a-string, but not guaranteed.
    """
    print(f"Submitting job {name}")
    desc = pyslurm.JobSubmitDescription(**job_submit_description.model_dump(), **kwargs)
    job_id = desc.submit()
    print(f"Submitted as slurm job {job_id}")
    return job_id


@task(
    task_run_name="Job submission: {name}",
)
def start_or_attach_cluster_job(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    slurm_resubmit_policy: _SlurmResubmitPolicy,
    workdir: Path,
    make_workdir_first: bool = True,
    input_files: Optional[List[Path]] = None,
    **kwargs,
) -> OrchestratedClusterJob:
    """
    Run a command on the HPC Cluster via a Slurm job.

    This task MAY launch a new slurm job, otherwise it may return the Job ID of a previously launched job
    that is considered identical.

    This allows flows to "reattach" to slurm jobs that they previously started,
    even if the flow has crashed and been restarted.
    (E.g. if the prefect worker VM is restarted during a long-running nextflow pipeline.)

    It also allows flows to require a slurm job to have run, but to accept that slurm job may have been run by
    a previous or different flow.
    (E.g. if a metagenome assembly is needed by two different analysis pipelines.)

    Note that this task does not use Prefect Caching - it uses OrchestratedClusterJob objects in the django DB,
    along with logic defined by SlurmResubmitPolicies, to decide whether to submit a new job or not.

    :param name: Name for the job (both on Slurm and Prefect), e.g. "Run analysis pipeline for x"
    :param command: Shell-level command to run, e.g. "nextflow run my-pipeline.nf --sample x"
    :param expected_time: A timedelta after which the job will be killed if not done.
        This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param slurm_resubmit_policy: A SlurmResubmitPolicy to determine whether older identical jobs
        should be used in place of a new one.
    :param workdir: Work dir for the job (pathlib.Path, or str). Otherwise, a default will be used based on the name.
    :param make_workdir_first: Make the work dir first, on the SUBMITTER machine.
        Usually this is desirable, except in cases where you're launching a job to a slurm node which has diff fs mounts.
    :param input_files: List of input file paths used for this job.
        The content of these are hashed, as part of the decision about whether a new job should be launched or not.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: OrchestratedClusterJob submitted or attached.
    """
    logger = get_run_logger()

    ### Prepare working directory for job
    job_workdir = workdir
    _ensure_absolute_workdir(job_workdir)
    if make_workdir_first and not job_workdir.exists():
        job_workdir.mkdir(parents=True)
    logger.info(f"Will use {job_workdir=}")

    ### Prepare job submission description
    script = _(
        f"""\
        #!/bin/bash
        set -euo pipefail
        {command}
        """
    )
    logger.info(f"Will run the script ```{safe(script)}```")
    kwargs["environment"] = make_environment(kwargs.get("environment", None))
    job_submit_description = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=str(job_workdir),
        **kwargs,
    )

    # check if a job already exists for this
    input_files = [
        OrchestratedClusterJob.JobInputFile(
            path=input_file,
            hash=compute_hash_of_input_file([input_file]),
        )
        for input_file in input_files or []
    ]
    logger.info(f"Have hashed {len(input_files)} input files")

    last_submitted_similar_job: Optional[OrchestratedClusterJob] = (
        OrchestratedClusterJob.objects.get_previous_job(
            job=job_submit_description,
            policy=slurm_resubmit_policy,
            input_file_hashes=input_files,
        )
    )

    if last_submitted_similar_job:
        logger.info(f"A similar job exists in history: {last_submitted_similar_job}")
        if last_submitted_similar_job.should_resubmit_according_to_policy(
            slurm_resubmit_policy
        ):
            logger.info(
                f"Policy {slurm_resubmit_policy.policy_name} states we should resubmit the job."
            )
        else:
            logger.info(
                f"Policy {slurm_resubmit_policy.policy_name} states we should not resubmit the job. Using {last_submitted_similar_job}."
            )
            return last_submitted_similar_job

    if (
        last_submitted_similar_job
        and slurm_resubmit_policy.resubmit_needs_preparation_command
    ):
        logger.info(
            f"Policy {slurm_resubmit_policy.policy_name} requires a pre-resubmit command: {slurm_resubmit_policy.resubmit_needs_preparation_command}."
        )
        run_cluster_job(
            name=f"Preparation for: {name}",
            command=slurm_resubmit_policy.resubmit_needs_preparation_command,
            expected_time=timedelta(hours=1),
            memory=f"{EMG_CONFIG.slurm.preparation_command_job_memory_gb}G",
            working_dir=job_workdir,
            resubmit_policy=ResubmitAlwaysPolicy,
            environment={},
        )

    # need to submit new job
    job_id = submit_cluster_job(
        name=job_submit_description.name,
        job_submit_description=job_submit_description,
    )

    nf_link = get_nextflow_tower_url()
    nf_link_markdown = f"[Watch Nextflow Workflow]({nf_link})" if nf_link else ""

    ocj = OrchestratedClusterJob.objects.create(
        cluster_job_id=job_id,
        flow_run_id=flow_run.id,
        job_submit_description=job_submit_description,
        input_files_hashes=input_files,
    )

    create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(
            f"""\
            # Slurm job {job_id}
            [Orchestrated Cluster Job {ocj.id}]({EMG_CONFIG.service_urls.app_root}/{reverse("admin:workflows_orchestratedclusterjob_change", kwargs={"object_id": ocj.id})})
            Submitted a script to Slurm cluster:
            ~~~
            <<SCRIPT>>
            ~~~
            It will be terminated by Slurm if not done in {slurm_timedelta(expected_time)}.
            Slurm working dir is {job_workdir}.
            {nf_link_markdown}
            """
        ).replace("<<SCRIPT>>", safe(script)),
    )

    return ocj


@task(
    task_run_name="Job submission (as process): {name}",
)
def run_subprocess(
    name: str,
    command: str,
    expected_time: timedelta,
    memory: Union[int, str],
    slurm_resubmit_policy: _SlurmResubmitPolicy,
    workdir: Path,
    make_workdir_first: bool = True,
    input_files: Optional[List[Path]] = None,
    **kwargs,
) -> OrchestratedClusterJob:
    """
    Run a command as a subprocess.

    This task MAY launch a subprocess, otherwise it may return the Job ID of a previously launched job
    that is considered identical.

    This allows flows to "reattach" to slurm jobs that they previously started,
    even if the flow has crashed and been restarted.
    (E.g. if the prefect worker VM is restarted during a long-running nextflow pipeline.)

    It also allows flows to require a slurm job to have run, but to accept that slurm job may have been run by
    a previous or different flow.
    (E.g. if a metagenome assembly is needed by two different analysis pipelines.)

    Note that this task does not use Prefect Caching - it uses OrchestratedClusterJob objects in the django DB,
    along with logic defined by SlurmResubmitPolicies, to decide whether to submit a new job or not.

    :param name: Name for the job (both on Slurm and Prefect), e.g. "Run analysis pipeline for x"
    :param command: Shell-level command to run, e.g. "nextflow run my-pipeline.nf --sample x"
    :param expected_time: A timedelta after which the job will be killed if not done.
        This affects Prefect's polling interval too. E.g.  `timedelta(minutes=5)`
    :param memory: Maximum memory the job may use. In MB, or with a prefix. E.g. `100` or `10G`.
    :param slurm_resubmit_policy: A SlurmResubmitPolicy to determine whether older identical jobs
        should be used in place of a new one.
    :param workdir: Work dir for the job (pathlib.Path, or str). Otherwise, a default will be used based on the name.
    :param make_workdir_first: Make the work dir first, on the SUBMITTER machine.
        Usually this is desirable, except in cases where you're launching a job to a slurm node which has diff fs mounts.
    :param input_files: List of input file paths used for this job.
        The content of these are hashed, as part of the decision about whether a new job should be launched or not.
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription
    :return: OrchestratedClusterJob submitted or attached.
    """
    logger = get_run_logger()

    ### Prepare working directory for job
    job_workdir = workdir
    _ensure_absolute_workdir(job_workdir)
    if make_workdir_first and not job_workdir.exists():
        job_workdir.mkdir(parents=True)
    logger.info(f"Will use {job_workdir=}")

    ### Prepare job submission description
    script = _(
        f"""\
        set -euo pipefail
        {command}
        """
    )

    logger.info(f"Will run the script ```{safe(script)}```")
    job_submit_description = OrchestratedClusterJob.SlurmJobSubmitDescription(
        name=name,
        time_limit=slurm_timedelta(expected_time),
        memory_per_node=memory,
        script=script,
        working_directory=str(job_workdir),
        **kwargs,
    )

    # check if a job already exists for this
    input_files = [
        OrchestratedClusterJob.JobInputFile(
            path=input_file,
            hash=compute_hash_of_input_file([input_file]),
        )
        for input_file in input_files or []
    ]
    logger.info(f"Have hashed {len(input_files)} input files")

    last_submitted_similar_job: Optional[OrchestratedClusterJob] = (
        OrchestratedClusterJob.objects.get_previous_job(
            job=job_submit_description,
            policy=slurm_resubmit_policy,
            input_file_hashes=input_files,
        )
    )

    if last_submitted_similar_job:
        logger.info(f"A similar job exists in history: {last_submitted_similar_job}")
        if last_submitted_similar_job.should_resubmit_according_to_policy(
            slurm_resubmit_policy
        ):
            logger.info(
                f"Policy {slurm_resubmit_policy.policy_name} states we should resubmit the job."
            )
        else:
            logger.info(
                f"Policy {slurm_resubmit_policy.policy_name} states we should not resubmit the job. Using {last_submitted_similar_job}."
            )
            return last_submitted_similar_job

    if (
        last_submitted_similar_job
        and slurm_resubmit_policy.resubmit_needs_preparation_command
    ):
        logger.info(
            f"Policy {slurm_resubmit_policy.policy_name} requires a pre-resubmit command: {slurm_resubmit_policy.resubmit_needs_preparation_command}."
        )
        run_cluster_job(
            name=f"Preparation for: {name}",
            command=slurm_resubmit_policy.resubmit_needs_preparation_command,
            expected_time=timedelta(hours=1),
            memory=f"{EMG_CONFIG.slurm.preparation_command_job_memory_gb}G",
            working_dir=job_workdir,
            resubmit_policy=ResubmitAlwaysPolicy,
            environment={},
        )

    env = make_environment(kwargs.get("environment", None))

    # need to submit new job
    process = subprocess.Popen(
        ["/bin/bash", "-e", "-u", "-o", "pipefail", "-c", command],
        cwd=job_workdir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )

    nf_link = get_nextflow_tower_url()
    nf_link_markdown = f"[Watch Nextflow Workflow]({nf_link})" if nf_link else ""

    ocj = OrchestratedClusterJob.objects.create(
        cluster_job_id=process.pid,
        flow_run_id=flow_run.id,
        job_submit_description=job_submit_description,
        input_files_hashes=input_files,
    )

    create_markdown_artifact(
        key="slurm-job-submission",
        markdown=_(
            f"""\
            # Subprocess with pid {process.pid}
            [Orchestrated Cluster Job {ocj.id}]({EMG_CONFIG.service_urls.app_root}/{reverse("admin:workflows_orchestratedclusterjob_change", kwargs={"object_id": ocj.id})})
            Submitted a script as a subprocess:
            ~~~
            <<SCRIPT>>
            ~~~
            It will be terminated if not done in {slurm_timedelta(expected_time)}.
            Working dir is {job_workdir}.
            {nf_link_markdown}
            """
        ).replace("<<SCRIPT>>", safe(script)),
    )

    try:
        _stdout, stderr = process.communicate(timeout=expected_time.total_seconds())
    except TimeoutError as e:
        logger.error(f"Failed to run subprocess: {e}")
        process.kill()
        process.wait()

        ocj.last_known_state = SlurmStatus.failed
        ocj.save()

        raise SubprocessFailedException(process.pid, process.returncode, "Timed out")

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


def cancel_cluster_job(name: str):
    """
    Finds a job running slurm (by name) and cancels it provided there is exactly one match.
    :param name: The job name as submitted.
    :return:
    """
    logger = get_run_logger()
    logger.info(f"Cancelling job {name}")
    jobs = pyslurm.db.Jobs.load(
        db_filter=pyslurm.db.JobFilter(names=[name], users=[EMG_CONFIG.slurm.user])
    )
    jobs_to_cancel = [
        job.job_id
        for job in jobs.values()
        if job.state in [SlurmStatus.running.value, SlurmStatus.pending.value]
    ]

    if len(jobs_to_cancel) == 1:
        try:
            job_id = int(jobs_to_cancel[0])
        except ValueError:
            raise ValueError(
                "Cannot cancel job array jobs - job id must be integer like"
            )

        logger.info(f"Found one running job to cancel: {job_id}")
        pyslurm.Job(job_id).load(job_id).cancel()

    else:
        raise Exception(
            f"Found {len(jobs_to_cancel)} matching jobs to cancel for name {name}. Not cancelling."
        )


class ClusterJobFailedException(Exception):
    def __init__(self, job_id, state, message=None):
        self.job_id = job_id
        self.state = state
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self):
        msg = f"Cluster job {self.job_id} failed with state {self.state}"
        if self.message:
            msg += f"\nDetails: {self.message}"
        return msg


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


@flow(
    flow_run_name="Cluster job: {name}",
    persist_result=True,
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
    """
    Run and wait for a job on the HPC cluster.

    :param name: Name for the job on slurm, e.g. "job 1"
    :param command: Shell-level command to run, e.g. "touch 1.txt"
    :param expected_time: A timedelta after which the job will be killed if unfinished, e.g. timedelta(days=1)
    :param memory: Max memory the job may use. In MB, or with a suffix. E.g. `100` or `10G`.
    :param environment: Dictionary of environment variables to pass to job, or string in format of sbatch --export
        (see https://slurm.schedmd.com/sbatch.html). E.g. `TOWER_ACCESSION_TOKEN`
    :param working_dir: Path to a work dir for the job. If relative, it is relative to `default_workdir` in config.
    :param resubmit_policy: A SlurmResubmitPolicy to determine whether older identical jobs
        should be used in place of a new one.
    :param input_files_to_hash: Optional list of filepaths,
        whose contents will be hashed to determine if this job is identical to another.
        Note that the hash is done on the node where this flow runs, not the node where the job (may) run.
        This means hashes can't be computed for files only accessible to certain partitions (like datamover nodes).
    :param kwargs: Extra arguments to be passed to PySlurm's JobSubmitDescription.
    :return: Slurm job ID once finished.
    """
    logger = get_run_logger()

    _name = name
    for unsafe in SLURM_UNSAFE_CHARS:
        _name = _name.replace(unsafe, "-")

    worker_type = get_prefect_worker_type()

    if worker_type == SlurmWorker.type:
        return run_subprocess(
            name=_name,
            command=command,
            expected_time=expected_time,
            memory=memory,
            input_files=input_files_to_hash,
            slurm_resubmit_policy=resubmit_policy,
            workdir=working_dir or Path(settings.EMG_CONFIG.slurm.default_workdir),
            make_workdir_first=True,
            environment=environment,
            **kwargs,
        )

    # Potentially wait some time if our cluster queue is very full
    delay_until_cluster_has_space()

    # Submit or attach to a job on the cluster.
    # Depending on the job history and Resubmit Policy, this job may be a new one, an already running one,
    # or a previously completed one.
    orchestrated_cluster_job = start_or_attach_cluster_job(
        name=_name,
        command=command,
        expected_time=expected_time,
        memory=memory,
        input_files=input_files_to_hash,
        slurm_resubmit_policy=resubmit_policy,
        workdir=working_dir or Path(settings.EMG_CONFIG.slurm.default_workdir),
        make_workdir_first=True,
        environment=environment,
        **kwargs,
    )

    # Wait for job completion
    # Resumability: if this flow was re-run / restarted for some reason, or the exact same cluster job was sent later,
    #  we should have gotten  back an existing slurm job_id of a previous run of it. And therefore the first status
    #  check will just tell us the job finished immediately / it'll wait for the EXISTING job to finish.
    is_job_in_terminal_state = False
    while not is_job_in_terminal_state:
        job_state = check_cluster_job(orchestrated_cluster_job)
        if slurm_status_is_finished_successfully(job_state):
            logger.info(f"Job {orchestrated_cluster_job} finished successfully.")
            store_nextflow_trace(orchestrated_cluster_job)
            is_job_in_terminal_state = True

        if slurm_status_is_finished_unsuccessfully(job_state):
            error_details = None
            job_id = orchestrated_cluster_job.cluster_job_id
            try:
                job = pyslurm.db.Job(job_id).load(job_id)
                job_log_path = Path(job.working_directory) / Path(f"slurm-{job_id}.out")
                if job_log_path.exists():
                    with open(job_log_path, "r") as job_log:
                        full_log = job_log.readlines()
                        error_details = "\n".join(
                            full_log[-EMG_CONFIG.slurm.job_log_failure_tail_lines :]
                        )
            except Exception as e:
                logger.warning(f"Failed to get job error details: {e}")

            raise ClusterJobFailedException(job_id, job_state, error_details)

        else:
            logger.debug(
                f"Job {orchestrated_cluster_job} is still running. "
                f"Sleeping for {EMG_CONFIG.slurm.default_seconds_between_job_checks} seconds."
            )
            time.sleep(EMG_CONFIG.slurm.default_seconds_between_job_checks)

    return orchestrated_cluster_job
