"""The privileges proteindb_owner needs, as a role that is not a superuser, to apply each SQL file."""

from uuid import uuid4

import psycopg
import pytest
from django.db import connections
from psycopg.errors import InsufficientPrivilege, RaiseException

from proteins.tests.conftest import LOCK_ROLES, ROLES, ROLES_SQL, TIER1_SQL, role_dsn

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"])


def superuser():
    database = connections["proteindb"]
    conn = database.get_new_connection(database.get_connection_params())
    conn.autocommit = True
    return conn


@pytest.fixture
def fresh(tier1):
    """A new database, and an owner role with no privileges yet."""
    suffix = uuid4().hex[:8]
    database, owner = f"proteindb_fresh_{suffix}", f"proteindb_owner_{suffix}"
    with superuser() as conn:
        conn.execute(f"CREATE ROLE {owner} LOGIN PASSWORD '{owner}'")
        conn.execute(f"CREATE DATABASE {database}")
    yield database, owner
    with superuser() as conn:
        conn.execute(f"DROP DATABASE {database} WITH (FORCE)")
        conn.execute(f"DROP ROLE {owner}")


def apply(database, role, sql):
    warnings = []
    with superuser() as lock, lock.transaction():
        lock.execute(LOCK_ROLES)
        with psycopg.connect(role_dsn(role, database)) as conn:
            # A GRANT without the right to grant it only warns.
            conn.add_notice_handler(
                lambda notice: notice.severity_nonlocalized == "WARNING"
                and warnings.append(notice.message_primary)
            )
            conn.execute(sql.read_text())
    assert warnings == []


def test_tier1_sql_needs_only_create_on_the_database(fresh):
    database, owner = fresh
    with superuser() as conn:
        conn.execute(f"GRANT CREATE ON DATABASE {database} TO {owner}")
    apply(database, owner, TIER1_SQL)

    with psycopg.connect(role_dsn(owner, database)) as conn:
        assert conn.execute(
            "SELECT DISTINCT pg_get_userbyid(relowner) FROM pg_class"
            " WHERE relnamespace = 'proteindb'::regnamespace"
            " UNION SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'proteindb'"
        ).fetchall() == [(owner,)]
        assert conn.execute(
            "SELECT has_table_privilege('proteindb_accession', 'proteindb.protein_key_3f', 'SELECT')"
        ).fetchone() == (True,)


@pytest.mark.parametrize(
    "createrole, admin",
    [(True, True), (False, True), (True, False)],
    ids=["both", "no CREATEROLE", "no ADMIN"],
)
def test_roles_sql_needs_createrole_and_admin_on_the_runtime_roles(
    fresh, createrole, admin
):
    database, owner = fresh
    with superuser() as conn:
        conn.execute(f"ALTER DATABASE {database} OWNER TO {owner}")
        if createrole:
            conn.execute(f"ALTER ROLE {owner} CREATEROLE")
        if admin:
            conn.execute(
                f"GRANT {', '.join(ROLES)} TO {owner} WITH ADMIN OPTION, INHERIT FALSE, SET FALSE"
            )

    if not (createrole and admin):
        with pytest.raises(
            InsufficientPrivilege, match="permission denied to alter role"
        ):
            apply(database, owner, ROLES_SQL)
        return
    apply(database, owner, ROLES_SQL)
    for role in ROLES:
        with psycopg.connect(role_dsn(role, database)) as conn:
            assert conn.execute("SHOW search_path").fetchone() == ("proteindb",)


def test_roles_sql_fails_if_it_cannot_grant_temporary(fresh):
    database, owner = fresh
    with superuser() as conn:
        conn.execute(f"REVOKE TEMPORARY ON DATABASE {database} FROM PUBLIC")
        conn.execute(f"ALTER ROLE {owner} CREATEROLE")
        conn.execute(
            f"GRANT {', '.join(ROLES)} TO {owner} WITH ADMIN OPTION, INHERIT FALSE, SET FALSE"
        )
    with pytest.raises(
        RaiseException, match="proteindb_accession cannot create temporary tables"
    ):
        apply(database, owner, ROLES_SQL)
