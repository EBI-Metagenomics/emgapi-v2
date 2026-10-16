"""Ownership of the protein dataset: the dataset update and the merge write it one at a time, and a
flow run that lost ownership never writes again.

The Prefect slot makes a second writer wait. It is not enough on its own: Prefect frees the slot of
a run it marks Crashed, whose SLURM job can still be alive. So the MGYP database also records the
owner's job, and the next writer starts only when SLURM shows that job has ended.
"""

import logging
import os
import re
import subprocess
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, NamedTuple

from django.db import connections, transaction
from django.utils import timezone
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import GlobalConcurrencyLimitResponse
from prefect.concurrency.sync import concurrency
from prefect.runtime import flow_run

from proteins.models import DatasetOwner
from proteins.routers import MGYP

logger = logging.getLogger(__name__)

# A job that ended in one of these on a working node has no process left: SLURM ended them all.
# NODE_FAIL is not one of them, since a node SLURM cannot reach may still be writing.
ENDED = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY"}


class OwnershipError(Exception):
    """This flow run cannot safely own the protein dataset."""


class Owner(NamedTuple):
    execution_id: uuid.UUID

    def check(self) -> None:
        """Exits the process at once unless this execution still owns the protein dataset, so that no cleanup code writes anything."""
        try:
            # A new connection: the flow's own may be in a transaction, or broken.
            database = connections[MGYP]
            with database.get_new_connection(database.get_connection_params()) as conn:
                (current,) = conn.execute(
                    "SELECT execution_id FROM dataset_owner"
                ).fetchone()
        except Exception:
            logger.exception("could not read the owner of the protein dataset")
            os._exit(1)
        if current != self.execution_id:
            logger.critical(
                "the protein dataset is owned by execution %s, not %s",
                current,
                self.execution_id,
            )
            os._exit(1)

    def confirm(self) -> None:
        """Locks the owner row for the rest of the mgyp alias's transaction, which then commits as the owner."""
        current = (
            DatasetOwner.objects.select_for_update()
            .values_list("execution_id", flat=True)
            .get()
        )
        if current != self.execution_id:
            raise OwnershipError(
                f"The protein dataset is owned by execution {current}, not {self.execution_id}"
            )


@contextmanager
def owner(flow: str, limit: str) -> Iterator[Owner]:
    """Makes this flow run the owner of the protein dataset for the duration.

    :param flow: The flow, recorded in dataset_owner: update or merge.
    :param limit: The name of the Prefect global concurrency limit that the update and the merge share.
    :raises OwnershipError: When the limit is missing, inactive, or not 1, or the previous owner's job is not known to have ended.
    """
    slot = find_limit(limit)
    if slot is None:
        raise OwnershipError(f"The concurrency limit {limit} does not exist")
    # Prefect does not enforce an inactive limit.
    if not slot.active or slot.limit != 1:
        raise OwnershipError(
            f"The concurrency limit {limit} must be active with a limit of 1,"
            f" not {'active' if slot.active else 'inactive'} with {slot.limit}"
        )
    # strict: a limit deleted since, or a lost lease, is an error rather than a warning.
    with concurrency(limit, occupy=1, strict=True):
        taken = take(flow)
        try:
            yield taken
        finally:
            release(taken)


def find_limit(name: str) -> GlobalConcurrencyLimitResponse | None:
    """The Prefect global concurrency limit with this name, if it exists.

    :param name: The limit's name.
    """
    # Read by name, a missing limit is a 404, which emgapi-v2's client settings retry for an hour.
    with get_client(sync_client=True) as client:
        offset = 0
        while page := client.read_global_concurrency_limits(limit=100, offset=offset):
            for limit in page:
                if limit.name == name:
                    return limit
            offset += len(page)
    return None


def take(flow: str) -> Owner:
    """Records this job as the owner, once SLURM shows that the previous owner's job has ended.

    :param flow: The flow, recorded in dataset_owner.
    """
    if "SLURM_JOB_ID" not in os.environ:
        raise OwnershipError("Only a SLURM job can own the protein dataset")
    job_id = int(os.environ["SLURM_JOB_ID"])
    submit_at = own_submit_time(job_id)
    execution_id = uuid.uuid4()
    with transaction.atomic(using=MGYP):
        # The row lock makes flows that start at once take turns, and the second finds the first.
        previous = DatasetOwner.objects.select_for_update().get()
        if previous.slurm_job_id is not None:
            check_ended(previous.slurm_job_id, previous.slurm_submit_at)
        DatasetOwner.objects.update(
            flow=flow,
            slurm_job_id=job_id,
            slurm_submit_at=submit_at,
            execution_id=execution_id,
            flow_run_id=flow_run.id,
            acquired_at=timezone.now(),
        )
    logger.info("execution %s of job %s owns the protein dataset", execution_id, job_id)
    return Owner(execution_id)


def check_ended(job_id: int, submit_at: datetime) -> None:
    """Raises OwnershipError unless SLURM shows that every run of the job has ended.

    :param job_id: The SLURM job id.
    :param submit_at: When the job was submitted, which tells it from others with the same id.
    """
    states = states_of(job_id, submit_at)
    # A requeued job has a record for each time it ran, and the first may have run on a failed node.
    if not states or not ENDED.issuperset(states):
        raise OwnershipError(
            f"The protein dataset is owned by job {job_id}, submitted at {submit_at},"
            f" whose states are {', '.join(states) or 'not found'}."
            " No update or merge can start until that job is known to have stopped."
        )


def states_of(job_id: int, submit_at: datetime) -> list[str]:
    """The states SLURM has recorded for one job, or why they are unknown.

    :param job_id: The SLURM job id.
    :param submit_at: When the job was submitted, which tells it from others with the same id.
    """
    try:
        return job_states(job_id).get(submit_at, [])
    except (OSError, subprocess.SubprocessError, ValueError) as e:
        return [f"unknown, as sacct failed: {e}"]


def job_states(job_id: int) -> dict[datetime, list[str]]:
    """The states SLURM has recorded for this job id, by submit time, since ids can be reused.

    -D lists every record of the id, where sacct would otherwise show only the most recent.

    :param job_id: The SLURM job id.
    """
    out = subprocess.run(
        ["sacct", "-j", str(job_id), "-X", "-D", "-n", "-P", "-o", "State,Submit"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    states = defaultdict(list)
    for line in out.splitlines():
        state, submit = line.split("|")
        # "CANCELLED by 1234"
        states[local_time(submit)].append(state.split()[0])
    return states


def own_submit_time(job_id: int) -> datetime:
    """When this job was submitted.

    :param job_id: This job's SLURM job id.
    """
    # From the controller rather than accounting, which can lag behind a job that has just started.
    out = subprocess.run(
        ["scontrol", "show", "job", "-o", str(job_id)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return local_time(re.search(r"\bSubmitTime=(\S+)", out)[1])


def local_time(slurm_time: str) -> datetime:
    """SLURM prints times without a zone, in the zone of the host.

    :param slurm_time: A time as SLURM prints it.
    """
    return datetime.fromisoformat(slurm_time).astimezone()


def release(owner: Owner) -> None:
    """Gives up ownership of the protein dataset, unless another execution has taken it over.

    :param owner: This run's ownership.
    """
    try:
        DatasetOwner.objects.filter(execution_id=owner.execution_id).update(
            flow=None,
            slurm_job_id=None,
            slurm_submit_at=None,
            execution_id=None,
            flow_run_id=None,
            acquired_at=None,
        )
    except Exception:
        logger.exception(
            "could not release the protein dataset; the next writer takes it over once this job has ended"
        )
