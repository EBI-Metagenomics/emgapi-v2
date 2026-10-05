import os
import subprocess
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from django.db import connections, transaction
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from psycopg.conninfo import make_conninfo

from proteins.accession.inputs import read_input

FIXTURES = Path(__file__).parent.parent / "fixtures"
TIER1_SQL = Path(__file__).parent.parent / "sql" / "tier1.sql"
ROLES = ("proteindb_accession", "proteindb_load", "proteindb_read")

# Whole short contigs, with every gene on them, cut from the published V6 analyses
# MGYA01028322, MGYA01030666 and MGYA01032591, chosen to cover each gene caller on
# both strands and every partial= value.
PUBLISHED_V6 = ("ERZ29562087", "ERZ25069264", "ERZ29895170")


def read_published(assembly):
    return read_input(
        *(FIXTURES / f"{assembly}.{suffix}.gz" for suffix in ("faa", "gff", "fasta"))
    )


@pytest.fixture(scope="session")
def tier1(django_db_setup, django_db_blocker):
    """The Tier 1 schema, applied once to the proteindb test database, and its runtime roles."""
    with (
        django_db_blocker.unblock(),
        transaction.atomic(using="proteindb"),
        connections["proteindb"].cursor() as cursor,
    ):
        # Roles belong to the cluster, which every xdist worker's database shares,
        # and concurrent changes to one role fail.
        cursor.execute("LOCK TABLE pg_catalog.pg_authid IN SHARE ROW EXCLUSIVE MODE")
        for role in ROLES:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [role])
            if not cursor.fetchone():
                cursor.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{role}'")
        cursor.execute(TIER1_SQL.read_text())
        cursor.execute("RESET search_path")


def role_dsn(role) -> str:
    """A libpq connection string for the proteindb test database, as a role."""
    settings = connections["proteindb"].settings_dict
    return make_conninfo(
        host=settings["HOST"],
        port=settings["PORT"] or None,
        dbname=settings["NAME"],
        user=role,
        password=role,
    )


def role_connection(role, **kwargs):
    return psycopg.connect(role_dsn(role), **kwargs)


@pytest.fixture
def connect_as(tier1):
    """Opens connections as a role. Closing them at teardown discards what they did not commit."""
    opened = []

    def connect(role):
        opened.append(role_connection(role))
        return opened[-1]

    yield connect
    for conn in opened:
        conn.close()


class Killed(BaseException):
    """Ends a process as a kill would: no handler of its own runs."""


@pytest.fixture
def exits(monkeypatch):
    """Makes os._exit raise Killed, so that a test sees the process exit."""

    def exit(code):
        raise Killed(code)

    monkeypatch.setattr(os, "_exit", exit)


def query(sql, params=None):
    """Runs sql on the proteindb test database as its owner."""
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else None


@pytest.fixture
def emptied_tier1(tier1):
    """For transactional tests, whose role connections commit outside the test's transaction."""
    yield
    query(
        "TRUNCATE proteindb.protein_key, proteindb.assembly, proteindb.gene_caller,"
        " proteindb.staging_protein, proteindb.staging_contig, proteindb.staging_occurrence,"
        " proteindb.study, proteindb.biome, proteindb.load_log RESTART IDENTITY"
    )
    query(
        "UPDATE proteindb.tier2_owner SET flow = NULL, slurm_job_id = NULL, slurm_submit_at = NULL,"
        " execution_id = NULL, flow_run_id = NULL, acquired_at = NULL"
    )


@pytest.fixture
def connect_accession(emptied_tier1):
    """Opens autocommit connections as proteindb_accession, as mgyp-accession does."""
    opened = []

    def connect():
        opened.append(role_connection("proteindb_accession", autocommit=True))
        return opened[-1]

    yield connect
    for conn in opened:
        conn.close()


SUBMITTED = {1: "2026-10-01T02:00:00", 2: "2026-10-02T02:00:00"}


def submitted(job):
    return datetime.fromisoformat(SUBMITTED[job]).astimezone()


@pytest.fixture
def slurm(monkeypatch, emptied_tier1):
    """A mocked SLURM. `as_job(n)` makes this process job n, and `sacct[n]` is every record of job n.

    Like sacct, it prints only the most recent record without -D.
    """
    sacct = {}

    def run(argv, **kwargs):
        if argv[0] == "scontrol":
            job = int(argv[-1])
            out = (
                f"JobId={job} JobName=load SubmitTime={SUBMITTED[job]} EligibleTime=x\n"
            )
        else:
            out = sacct[int(argv[2])]
            if isinstance(out, Exception):
                raise out
            if "-D" not in argv:
                out = "".join(out.splitlines(keepends=True)[-1:])
        return subprocess.CompletedProcess(argv, 0, out, "")

    monkeypatch.setattr(subprocess, "run", run)

    def as_job(job):
        monkeypatch.setenv("SLURM_JOB_ID", str(job))

    as_job(2)
    return sacct, as_job


@pytest.fixture
def limit(prefect_harness):
    """Creates a concurrency limit with a name of its own, as the Prefect server outlives the test."""

    def create(slots=1, active=True):
        name = f"proteindb-tier2-{uuid4()}"
        with get_client(sync_client=True) as client:
            client.create_global_concurrency_limit(
                GlobalConcurrencyLimitCreate(name=name, limit=slots, active=active)
            )
        return name

    return create
