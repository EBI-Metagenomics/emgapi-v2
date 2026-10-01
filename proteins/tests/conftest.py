from pathlib import Path

import psycopg
import pytest
from django.db import connections, transaction

TIER1_SQL = Path(__file__).parent.parent / "sql" / "tier1.sql"
ROLES = ("proteindb_accession", "proteindb_load", "proteindb_read")


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


def role_connection(role, **kwargs):
    """A new connection to the proteindb test database as a role."""
    settings = connections["proteindb"].settings_dict
    return psycopg.connect(
        host=settings["HOST"],
        port=settings["PORT"] or None,
        dbname=settings["NAME"],
        user=role,
        password=role,
        **kwargs,
    )


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


@pytest.fixture
def connect_accession(tier1):
    """Opens autocommit connections as proteindb_accession, as mgyp-accession does.

    What they commit is outside the test's transaction, so this is for transactional
    tests only, and the tables are emptied afterwards.
    """
    opened = []

    def connect():
        opened.append(role_connection("proteindb_accession", autocommit=True))
        return opened[-1]

    yield connect
    for conn in opened:
        conn.close()
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(
            "TRUNCATE proteindb.protein_key, proteindb.assembly, proteindb.gene_caller,"
            " proteindb.staging_protein, proteindb.staging_contig, proteindb.staging_occurrence"
            " RESTART IDENTITY"
        )
