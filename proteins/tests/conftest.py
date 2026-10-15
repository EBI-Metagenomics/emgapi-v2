from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest
from django.db import connections, transaction
from psycopg.conninfo import make_conninfo

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
