import logging
import os
from collections import Counter

from django.conf import settings
from prefect import get_run_logger, task
from prefect.context import TaskRunContext

from workflows.prefect_utils.slurm_status import SlurmStatus

if "PYTEST_VERSION" in os.environ:
    logging.debug("Unit testing, so patching pyslurm.")
    import workflows.prefect_utils.pyslurm_patch as pyslurm
else:
    try:
        import pyslurm
    except:  # noqa: E722
        logging.warning("No PySlurm available. Patching.")
        import workflows.prefect_utils.pyslurm_patch as pyslurm

EMG_CONFIG = settings.EMG_CONFIG


def get_cluster_state_counts() -> dict[SlurmStatus, int]:
    logger = get_run_logger()
    try:
        our_jobs = pyslurm.db.JobFilter(users=[EMG_CONFIG.slurm.user])
        jobs = pyslurm.db.Jobs.load(our_jobs)
    except pyslurm.core.error.RPCError:
        logger.warning("Error talking to slurm")
        return {}
    logger.info(f"SLURM job total count: {len(jobs)}")
    return Counter([job.state for job in jobs.values()])


def cluster_can_accept_jobs() -> int:
    """
    Does the cluster have "space" for more pending jobs? And how many?
    :return: Zero if there is no space. Otherwise, positive int of how many jobs can be taken.
    """
    current_job_state_counts = get_cluster_state_counts()
    job_load = current_job_state_counts.get(
        SlurmStatus.running, 0
    ) + current_job_state_counts.get(SlurmStatus.pending, 0)
    space = EMG_CONFIG.slurm.incomplete_job_limit - job_load
    return max(space, 0)


class ClusterPendingJobsLimitReachedException(Exception): ...


def _cluster_delay_key(context: TaskRunContext, parameters: dict) -> str:
    """
    Creates a cache key that prevents the cluster delay task from running again for the same flow.
    I.e., will not be impactful on a re-run of the parent flow.
    """
    return f"cluster-delay-marker-{context.task_run.flow_run_id}"


@task(
    retries=EMG_CONFIG.slurm.default_submission_attempts_limit,
    retry_delay_seconds=EMG_CONFIG.slurm.default_seconds_between_submission_attempts,
    cache_key_fn=_cluster_delay_key,
)
def delay_until_cluster_has_space() -> int:
    """
    Run once (by caching the result of this task) at the start of a cluster job,
    to potentially wait until the slurm cluster queue is sufficiently small for us to submit
    a new job.
    TODO: add "pressure" based on creation time of this flow, to enable prioritisation
    TODO:   or use concurrency limit on this
    :param delay_key: a string key to prevent another delay occurring once one already has.
    :return: Free space (as number of jobs) below our limit. Will fail if above limit.
    """
    if not (space_on_cluster := cluster_can_accept_jobs()):
        raise ClusterPendingJobsLimitReachedException
    return space_on_cluster
