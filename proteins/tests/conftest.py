from pathlib import Path

import psycopg
import pytest
from django.db import connections, transaction
from psycopg.conninfo import make_conninfo

from proteins.routers import MGYP

SQL = Path(__file__).parent.parent / "sql"
SCHEMA_SQL, ROLES_SQL = SQL / "mgyp_schema.sql", SQL / "mgyp_roles.sql"
ROLES = ("proteindb_accession", "proteindb_load", "proteindb_read")

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
