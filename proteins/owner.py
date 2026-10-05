"""Ownership of Tier 2: one load writes it at a time, and a load that lost ownership never writes again."""

import logging
import os
import re
import subprocess
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, NamedTuple

import psycopg
from prefect.client.orchestration import get_client
from prefect.client.schemas.responses import GlobalConcurrencyLimitResponse
from prefect.concurrency.sync import concurrency
from prefect.runtime import flow_run

logger = logging.getLogger(__name__)

# A job that ended in one of these on a working node has no process left: SLURM ended them all.
# NODE_FAIL is not one of them, since a node SLURM cannot reach may still be writing.
ENDED = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY"}


class OwnershipError(Exception): ...


class Owner(NamedTuple):
    dsn: str
    execution_id: uuid.UUID

    def check(self) -> None:
        """Exits the process at once unless this execution still owns Tier 2, so that no cleanup code writes anything."""
        try:
            with psycopg.connect(self.dsn) as conn:
                current = current_execution(conn)
        except Exception:
            logger.exception("could not read the owner of Tier 2")
            os._exit(1)
        if current != self.execution_id:
            logger.critical(
                "Tier 2 is owned by execution %s, not %s", current, self.execution_id
            )
            os._exit(1)

    def confirm(self, conn: psycopg.Connection) -> None:
        """Locks the owner row for the rest of `conn`'s transaction, which then commits as the owner."""
        current = current_execution(conn, lock=True)
        if current != self.execution_id:
            raise OwnershipError(
                f"Tier 2 is owned by execution {current}, not {self.execution_id}"
            )


def current_execution(conn: psycopg.Connection, lock: bool = False) -> uuid.UUID | None:
    return conn.execute(
        "SELECT execution_id FROM tier2_owner" + (" FOR UPDATE" if lock else "")
    ).fetchone()[0]


@contextmanager
def owner(dsn: str, flow: str, limit: str) -> Iterator[Owner]:
    """Makes this flow run the owner of Tier 2 for the duration."""
    slot = find_limit(limit)
    if slot is None:
        raise OwnershipError(f"the concurrency limit {limit} does not exist")
    # Prefect does not enforce an inactive limit.
    if not slot.active or slot.limit != 1:
        raise OwnershipError(
            f"the concurrency limit {limit} must be active with a limit of 1,"
            f" not {'active' if slot.active else 'inactive'} with {slot.limit}"
        )
    with concurrency(limit, occupy=1, strict=True):
        taken = take(dsn, flow)
        try:
            yield taken
        finally:
            release(taken)


def find_limit(name: str) -> GlobalConcurrencyLimitResponse | None:
    # Read by name, a missing limit is a 404, which the client retries for an hour (pyproject.toml).
    with get_client(sync_client=True) as client:
        offset = 0
        while page := client.read_global_concurrency_limits(limit=100, offset=offset):
            for limit in page:
                if limit.name == name:
                    return limit
            offset += len(page)
    return None


def take(dsn: str, flow: str) -> Owner:
    """Records this job as the owner, once SLURM shows that the previous owner's job has ended."""
    job_id = int(os.environ["SLURM_JOB_ID"])
    submit_at = own_submit_time(job_id)
    execution_id = uuid.uuid4()
    with psycopg.connect(dsn) as conn:
        previous_job, previous_submit_at = conn.execute(
            "SELECT slurm_job_id, slurm_submit_at FROM tier2_owner FOR UPDATE"
        ).fetchone()
        if previous_job is not None:
            check_ended(previous_job, previous_submit_at)
        conn.execute(
            """
            UPDATE tier2_owner SET flow = %s, slurm_job_id = %s, slurm_submit_at = %s,
                execution_id = %s, flow_run_id = %s, acquired_at = now()
            """,
            [flow, job_id, submit_at, execution_id, flow_run.id],
        )
    logger.info("execution %s of job %s owns Tier 2", execution_id, job_id)
    return Owner(dsn, execution_id)


def check_ended(job_id: int, submit_at: datetime) -> None:
    try:
        states = job_states(job_id).get(submit_at, [])
    except (OSError, subprocess.SubprocessError, ValueError) as e:
        states = [f"unknown, as sacct failed: {e}"]
    # A requeued job has a record for each time it ran, and the first may have run on a failed node.
    if not states or not ENDED.issuperset(states):
        raise OwnershipError(
            f"Tier 2 is owned by job {job_id}, submitted at {submit_at},"
            f" whose states are {', '.join(states) or 'not found'}."
            " No load can start until that job is known to have stopped."
        )


def job_states(job_id: int) -> dict[datetime, list[str]]:
    """The states SLURM has recorded for this job id, by submit time, since ids can be reused.

    -D lists every record of the id, where sacct would otherwise show only the most recent.
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
    # From the controller rather than accounting, which can lag behind a job that has just started.
    out = subprocess.run(
        ["scontrol", "show", "job", "-o", str(job_id)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return local_time(re.search(r"\bSubmitTime=(\S+)", out)[1])


def local_time(slurm_time: str) -> datetime:
    """SLURM prints times without a zone, in the zone of the host."""
    return datetime.fromisoformat(slurm_time).astimezone()


def release(owner: Owner) -> None:
    try:
        with psycopg.connect(owner.dsn) as conn:
            conn.execute(
                """
                UPDATE tier2_owner SET flow = NULL, slurm_job_id = NULL, slurm_submit_at = NULL,
                    execution_id = NULL, flow_run_id = NULL, acquired_at = NULL
                WHERE execution_id = %s
                """,
                [owner.execution_id],
            )
    except psycopg.Error:
        logger.exception(
            "could not release Tier 2; the next load takes it over once its job has ended"
        )
