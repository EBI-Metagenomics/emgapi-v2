import contextvars
import subprocess
import threading
from uuid import UUID

import pytest
from django.db import connections
from prefect import flow
from prefect.runtime import flow_run

from proteins.dataset.owner import Owner, OwnershipError, owner, release, take
from proteins.routers import MGYP
from proteins.tests.conftest import SUBMITTED, Killed, create_limit, query, submitted

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("mgyp_as_accession", "prefect_harness"),
]


def recorded():
    return query(
        "SELECT flow, slurm_job_id, slurm_submit_at, execution_id, flow_run_id"
        " FROM proteindb.dataset_owner"
    )[0]


NOBODY = (None,) * 5


def test_a_flow_run_owns_the_dataset_until_it_ends(slurm):
    name = create_limit()

    @flow
    def update_flow(fail):
        with owner("update", name) as taken:
            assert recorded() == (
                "update",
                2,
                submitted(2),
                taken.execution_id,
                UUID(flow_run.id),
            )
            if fail:
                raise RuntimeError("crash")

    update_flow(False)
    assert recorded() == NOBODY
    with pytest.raises(RuntimeError):
        update_flow(True)
    assert recorded() == NOBODY


def test_a_second_flow_waits_for_the_slot(slurm):
    name = create_limit()
    first_owns, first_may_end = threading.Event(), threading.Event()
    owners = []

    def first():
        with owner("update", name) as taken:
            owners.append(taken)
            first_owns.set()
            first_may_end.wait(60)
        connections.close_all()

    def second():
        with owner("merge", name) as taken:
            owners.append(taken)
        connections.close_all()

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
        "node-failed",
        "reused-id",
        "requeued",
        "unknown",
        "sacct-fails",
        "no-sacct",
    ],
)
def test_an_owner_not_known_to_have_ended_stops_the_next_flow(slurm, sacct_output):
    sacct, as_job = slurm
    as_job(1)
    first = take("update")
    sacct[1] = sacct_output
    as_job(2)
    with pytest.raises(OwnershipError, match="owned by job 1,"):
        take("merge")
    assert recorded()[:4] == ("update", 1, submitted(1), first.execution_id)


@pytest.mark.parametrize(
    "state", ["COMPLETED", "FAILED", "CANCELLED by 1000", "TIMEOUT", "OUT_OF_MEMORY"]
)
def test_an_owner_whose_job_ended_is_taken_over(slurm, state):
    sacct, as_job = slurm
    as_job(1)
    take("update")  # and killed: nothing releases it
    sacct[1] = f"{state}|{SUBMITTED[1]}\nRUNNING|{SUBMITTED[2]}\n"
    as_job(2)
    second = take("merge")
    assert recorded()[:4] == ("merge", 2, submitted(2), second.execution_id)


def test_flows_that_start_at_once_take_turns_on_the_row(slurm, connect_as):
    sacct, _ = slurm
    sacct[1] = f"RUNNING|{SUBMITTED[1]}\n"
    errors = []

    def second():
        try:
            take("update")
        except OwnershipError as e:
            errors.append(e)
        finally:
            connections.close_all()

    first = connect_as("proteindb_accession")
    first.execute("SELECT 1 FROM dataset_owner FOR UPDATE")
    thread = threading.Thread(target=second)
    thread.start()
    thread.join(2)
    assert thread.is_alive()
    first.execute(
        "UPDATE dataset_owner SET slurm_job_id = 1, slurm_submit_at = %s,"
        " execution_id = gen_random_uuid()",
        [submitted(1)],
    )
    first.commit()
    thread.join(60)
    assert "owned by job 1," in str(errors[0])


def test_only_a_slurm_job_can_own_the_dataset(slurm, monkeypatch):
    monkeypatch.delenv("SLURM_JOB_ID")
    with pytest.raises(OwnershipError, match="SLURM job"):
        take("update")
    assert recorded() == NOBODY


@pytest.mark.parametrize("owned_by", ["another", "nobody", "unreadable"])
def test_check_exits_unless_this_execution_owns_the_dataset(
    slurm, exits, monkeypatch, owned_by
):
    mine = take("update")
    mine.check()
    if owned_by == "another":
        query("UPDATE proteindb.dataset_owner SET execution_id = gen_random_uuid()")
    elif owned_by == "nobody":
        query("UPDATE proteindb.dataset_owner SET execution_id = NULL")
    else:
        monkeypatch.setitem(connections[MGYP].settings_dict, "PORT", 1)
    with pytest.raises(Killed):
        mine.check()


def test_a_release_leaves_a_later_owner_in_place(slurm):
    mine = take("update")
    query("UPDATE proteindb.dataset_owner SET execution_id = gen_random_uuid()")
    later = recorded()
    release(mine)
    assert recorded() == later
    release(Owner(later[3]))
    assert recorded() == NOBODY
