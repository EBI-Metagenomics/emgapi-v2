import os
import subprocess
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from django.db import connections, transaction
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from psycopg.conninfo import make_conninfo

import ena.models as ena_models
from analyses.models import Analysis, Assembly, Sample, Study
from proteins.accession.inputs import read_input
from proteins.routers import MGYP

FIXTURES = Path(__file__).parent.parent / "fixtures"
SQL = Path(__file__).parent.parent / "sql"
SCHEMA_SQL, ROLES_SQL = SQL / "mgyp_schema.sql", SQL / "mgyp_roles.sql"
ROLES = ("proteindb_accession", "proteindb_load", "proteindb_read")

# Whole short contigs, with every gene on them, cut from the published V6 analyses
# MGYA01028322, MGYA01030666 and MGYA01032591, chosen to cover each gene caller on
# both strands and every partial= value.
PUBLISHED_V6 = ("ERZ29562087", "ERZ25069264", "ERZ29895170")


def read_published(assembly):
    return read_input(
        *(FIXTURES / f"{assembly}.{suffix}.gz" for suffix in ("faa", "gff", "fasta"))
    )


# Roles belong to the cluster, which every xdist worker's database shares, and concurrent
# changes to one role fail. This lock conflicts with itself but not with ALTER ROLE's, so a
# test can hold it while a role that cannot take it changes the roles.
LOCK_ROLES = "LOCK TABLE pg_catalog.pg_authid IN SHARE UPDATE EXCLUSIVE MODE"


@pytest.fixture(scope="session")
def mgyp_db(django_db_setup, django_db_blocker):
    """The MGYP database schema, applied once to the mgyp test database, and its runtime roles."""
    with (
        django_db_blocker.unblock(),
        transaction.atomic(using=MGYP),
        connections[MGYP].cursor() as cursor,
    ):
        cursor.execute(LOCK_ROLES)
        for role in ROLES:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [role])
            if not cursor.fetchone():
                cursor.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{role}'")
        cursor.execute(SCHEMA_SQL.read_text())
        cursor.execute(ROLES_SQL.read_text())
        cursor.execute("RESET search_path")


def role_dsn(role, dbname=None) -> str:
    """A libpq connection string for the mgyp test database, or dbname, as a role."""
    settings = connections[MGYP].settings_dict
    return make_conninfo(
        host=settings["HOST"],
        port=settings["PORT"] or None,
        dbname=dbname or settings["NAME"],
        user=role,
        password=role,
    )


@pytest.fixture
def connect_as(mgyp_db):
    """Opens connections as a role. Closing them at teardown discards what they did not commit."""
    opened = []

    def connect(role):
        opened.append(psycopg.connect(role_dsn(role)))
        return opened[-1]

    yield connect
    for conn in opened:
        conn.close()


def query(sql, params=None):
    """Runs sql on the mgyp test database as its owner."""
    with connections[MGYP].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else None


@pytest.fixture
def emptied_mgyp_db(mgyp_db):
    """For transactional tests, whose role connections commit outside the test's transaction."""
    yield
    query(
        "TRUNCATE proteindb.protein, proteindb.assembly_proteins, proteindb.gene_caller,"
        " proteindb.staged_protein, proteindb.staged_contig, proteindb.staged_occurrence,"
        " proteindb.dataset_update RESTART IDENTITY"
    )
    query(
        "UPDATE proteindb.dataset_owner SET flow = NULL, slurm_job_id = NULL,"
        " slurm_submit_at = NULL, execution_id = NULL, flow_run_id = NULL, acquired_at = NULL"
    )


@pytest.fixture
def connect_accession(emptied_mgyp_db):
    """The accessioning core's connect: autocommit connections as proteindb_accession."""

    @contextmanager
    def connect():
        with psycopg.connect(role_dsn("proteindb_accession"), autocommit=True) as conn:
            yield conn

    return connect


def write_assembly(directory: Path, sequences: list[str], caller="Pyrodigal_v3.6.3"):
    """An assembly's input: one gene per sequence, all on one contig."""
    directory.mkdir(parents=True)
    with open(directory / "in.faa", "w") as faa, open(directory / "in.gff", "w") as gff:
        for n, sequence in enumerate(sequences, 1):
            faa.write(
                f">c1_{n} # {n} # {n + 8} # 1 # ID=1_{n};partial=00\n{sequence}\n"
            )
            gff.write(f"c1\t{caller}\tCDS\t{n}\t{n + 8}\t.\t+\t0\tID=c1_{n}\n")
    (directory / "contigs.fasta").write_text(">c1\n" + "ACGT" * 250 + "\n")
    return directory


def read_assembly(directory: Path):
    return read_input(
        directory / "in.faa", directory / "in.gff", directory / "contigs.fasta"
    )


@pytest.fixture
def mgyp_as_accession(emptied_mgyp_db):
    """Django's mgyp alias connects as proteindb_accession, as in production, and finds the schema at login.

    Restored before emptied_mgyp_db's teardown, which needs the owner.
    """
    settings = connections[MGYP].settings_dict
    saved, role = dict(settings), "proteindb_accession"
    names = "USER", "PASSWORD"
    settings.update(dict.fromkeys(names, role))
    connections[MGYP].close()
    yield
    connections[MGYP].close()
    settings.update(saved)


IMPORTED = Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED


def make_study(accession):
    ena_study = ena_models.Study.objects.create(accession=accession, title=accession)
    return Study.objects.create(ena_study=ena_study, title=accession)


def make_analysis(study, erz, version="V6", results_dir=None, state=IMPORTED):
    assembly = Assembly.objects.filter(ena_accessions__contains=[erz]).first()
    if assembly is None:
        ena_sample = ena_models.Sample.objects.create(
            study=study.ena_study, accession=f"SAM{erz}"
        )
        sample = Sample.objects.create(ena_sample=ena_sample, ena_study=study.ena_study)
        assembly = Assembly.objects.create(
            reads_study=study,
            ena_study=study.ena_study,
            sample=sample,
            ena_accessions=[erz],
        )
    analysis = Analysis.objects.create(
        study=study,
        ena_study=study.ena_study,
        sample=assembly.sample,
        assembly=assembly,
        experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
        pipeline_version=version,
        results_dir=results_dir and str(results_dir),
    )
    analysis.mark_status(state)
    return analysis


class Killed(BaseException):
    """Ends a process as a kill would: no handler of its own runs."""


@pytest.fixture
def exits(monkeypatch):
    """Makes os._exit raise Killed, so that a test sees the process exit."""

    def exit(code):
        raise Killed(code)

    monkeypatch.setattr(os, "_exit", exit)


SUBMITTED = {1: "2026-10-01T02:00:00", 2: "2026-10-02T02:00:00"}


def submitted(job):
    return datetime.fromisoformat(SUBMITTED[job]).astimezone()


@pytest.fixture
def slurm(monkeypatch):
    """A mocked SLURM, where this process is job 2. `as_job(n)` makes it job n, and `sacct[n]` is
    every record of job n.

    Like sacct, it prints only the most recent record without -D.
    """
    sacct = {}

    def run(argv, **kwargs):
        if argv[0] == "scontrol":
            job = int(argv[-1])
            out = f"JobId={job} JobName=x SubmitTime={SUBMITTED[job]} EligibleTime=x\n"
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


def create_limit(slots=1, active=True):
    """A Prefect global concurrency limit with a name of its own, as the Prefect server outlives the test."""
    name = f"protein-dataset-{uuid4()}"
    with get_client(sync_client=True) as client:
        client.create_global_concurrency_limit(
            GlobalConcurrencyLimitCreate(name=name, limit=slots, active=active)
        )
    return name
