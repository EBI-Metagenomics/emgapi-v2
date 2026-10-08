import contextvars
import subprocess
import threading
from uuid import UUID

import pytest
from prefect import flow
from prefect.runtime import flow_run

from proteins.owner import Owner, OwnershipError, owner, release, take
from proteins.tests.conftest import (
    SUBMITTED,
    Killed,
    query,
    role_connection,
    role_dsn,
    submitted,
)

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)


def dsn():
    return role_dsn("proteindb_load")


def recorded():
    return query(
        "SELECT flow, slurm_job_id, slurm_submit_at, execution_id, flow_run_id"
        " FROM proteindb.tier2_owner"
    )[0]


NOBODY = (None,) * 5


@pytest.mark.parametrize(
    "slots, active, error",
    [(None, True, "does not exist"), (1, False, "inactive"), (2, True, "with 2")],
    ids=["missing", "inactive", "two slots"],
)
def test_a_limit_that_is_not_one_active_slot_stops_the_flow_first(
    slurm, limit, slots, active, error
):
    name = limit(slots, active) if slots else "proteindb-tier2-missing"
    with pytest.raises(OwnershipError, match=error):
        with owner(dsn(), "load", name):
            pytest.fail("the flow ran")
    assert recorded() == NOBODY


def test_a_flow_run_owns_tier2_until_it_ends(slurm, limit):
    name = limit()

    @flow
    def load_flow(fail):
        with owner(dsn(), "load", name) as taken:
            assert recorded() == (
                "load",
                2,
                submitted(2),
                taken.execution_id,
                UUID(flow_run.id),
            )
            if fail:
                raise RuntimeError("crash")

    load_flow(False)
    assert recorded() == NOBODY
    with pytest.raises(RuntimeError):
        load_flow(True)
    assert recorded() == NOBODY


def test_a_second_flow_waits_for_the_slot(slurm, limit):
    name = limit()
    first_owns, first_may_end = threading.Event(), threading.Event()
    owners = []

    def first():
        with owner(dsn(), "load", name) as taken:
            owners.append(taken)
            first_owns.set()
            first_may_end.wait(60)

    def second():
        with owner(dsn(), "load", name) as taken:
            owners.append(taken)

    # The test harness's Prefect settings are context variables.
    threads = [
        threading.Thread(target=contextvars.copy_context().run, args=[target])
        for target in (first, second)
    ]
    threads[0].start()
    assert first_owns.wait(60)
    threads[1].start()
    threads[1].join(3)
    assert threads[1].is_alive() and len(owners) == 1

    first_may_end.set()
    for thread in threads:
        thread.join(60)
    assert len(owners) == 2
    assert recorded() == NOBODY


@pytest.mark.parametrize(
    "sacct_output",
    [
        f"RUNNING|{SUBMITTED[1]}\n",
        f"COMPLETING|{SUBMITTED[1]}\n",
        f"NODE_FAIL|{SUBMITTED[1]}\n",
        f"COMPLETED|{SUBMITTED[2]}\n",
        f"NODE_FAIL|{SUBMITTED[1]}\nFAILED|{SUBMITTED[1]}\n",
        "",
        subprocess.CalledProcessError(1, "sacct"),
        FileNotFoundError("sacct"),
    ],
    ids=[
        "running",
        "completing",
        "node failed",
        "reused id",
        "requeued",
        "unknown",
        "sacct fails",
        "no sacct",
    ],
)
def test_an_owner_not_known_to_have_ended_stops_the_next_flow(slurm, sacct_output):
    sacct, as_job = slurm
    as_job(1)
    first = take(dsn(), "load")
    sacct[1] = sacct_output
    as_job(2)
    with pytest.raises(OwnershipError, match="owned by job 1,"):
        take(dsn(), "load")
    assert recorded()[1:4] == (1, submitted(1), first.execution_id)


@pytest.mark.parametrize(
    "state", ["COMPLETED", "FAILED", "CANCELLED by 1000", "TIMEOUT", "OUT_OF_MEMORY"]
)
def test_an_owner_whose_job_ended_is_taken_over(slurm, state):
    sacct, as_job = slurm
    as_job(1)
    take(dsn(), "load")  # and killed: nothing releases it
    sacct[1] = f"{state}|{SUBMITTED[1]}\nRUNNING|{SUBMITTED[2]}\n"
    as_job(2)
    second = take(dsn(), "load")
    assert recorded()[:4] == ("load", 2, submitted(2), second.execution_id)


def test_flows_that_start_at_once_take_turns_on_the_row(slurm):
    sacct, _ = slurm
    sacct[1] = f"RUNNING|{SUBMITTED[1]}\n"
    errors = []

    def second():
        try:
            take(dsn(), "load")
        except OwnershipError as e:
            errors.append(e)

    with role_connection("proteindb_load") as first:
        first.execute("SELECT 1 FROM tier2_owner FOR UPDATE")
        thread = threading.Thread(target=second)
        thread.start()
        thread.join(2)
        assert thread.is_alive()
        first.execute(
            "UPDATE tier2_owner SET slurm_job_id = 1, slurm_submit_at = %s,"
            " execution_id = gen_random_uuid()",
            [submitted(1)],
        )
    thread.join(60)
    assert "owned by job 1," in str(errors[0])


@pytest.mark.parametrize("owned_by", ["another", "nobody", "unreadable"])
def test_check_exits_unless_this_execution_owns_tier2(slurm, exits, owned_by):
    mine = take(dsn(), "load")
    mine.check()
    if owned_by == "another":
        query("UPDATE proteindb.tier2_owner SET execution_id = gen_random_uuid()")
    elif owned_by == "nobody":
        query("UPDATE proteindb.tier2_owner SET execution_id = NULL")
    else:
        mine = Owner(role_dsn("proteindb_load") + " port=1", mine.execution_id)
    with pytest.raises(Killed):
        mine.check()


def test_a_release_leaves_a_later_owner_in_place(slurm):
    mine = take(dsn(), "load")
    query("UPDATE proteindb.tier2_owner SET execution_id = gen_random_uuid()")
    later = recorded()
    release(mine)
    assert recorded() == later
