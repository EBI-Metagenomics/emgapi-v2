import threading
import time

import psycopg
import pytest
from django.conf import settings
from django.db import OperationalError, connections

from proteins import routers
from proteins.routers import MGYP, mgyp_connection
from proteins.tests.conftest import role_dsn

pytestmark = pytest.mark.django_db(databases=["default", MGYP], transaction=True)


def on_default(sql):
    # Roles belong to the cluster, and the mgyp alias connects as the role under test.
    with connections["default"].cursor() as cursor:
        cursor.execute(sql)


@pytest.fixture
def limited_role(mgyp_db, worker_id, monkeypatch):
    """The mgyp alias connects as a role with a limit of one connection. Each xdist worker has its own."""
    role = f"proteindb_limited_{worker_id}"
    on_default(f"DROP ROLE IF EXISTS {role}")
    on_default(f"CREATE ROLE {role} LOGIN PASSWORD '{role}' CONNECTION LIMIT 1")
    monkeypatch.setitem(connections[MGYP].settings_dict, "USER", role)
    monkeypatch.setitem(connections[MGYP].settings_dict, "PASSWORD", role)
    monkeypatch.setattr(routers, "RETRY_SECONDS", 0.1)
    connections[MGYP].close()
    yield role
    connections[MGYP].close()
    on_default(f"DROP ROLE {role}")


def test_waits_for_a_free_connection(limited_role):
    holder = psycopg.connect(role_dsn(limited_role))
    threading.Timer(1.0, holder.close).start()
    with mgyp_connection() as conn:
        assert conn.execute("SELECT current_user").fetchone() == (limited_role,)


def test_gives_up_after_connection_wait_seconds(limited_role, monkeypatch):
    monkeypatch.setattr(settings.EMG_CONFIG.proteins, "connection_wait_seconds", 0.5)
    started = time.monotonic()
    with psycopg.connect(role_dsn(limited_role)):
        with pytest.raises(OperationalError, match="too many connections for role"):
            with mgyp_connection():
                pass
    assert time.monotonic() - started >= 0.5


def test_other_errors_fail_at_once(mgyp_db, monkeypatch):
    monkeypatch.setitem(connections[MGYP].settings_dict, "NAME", "does_not_exist")
    connections[MGYP].close()
    started = time.monotonic()
    with pytest.raises(OperationalError, match="does not exist"):
        with mgyp_connection():
            pass
    assert time.monotonic() - started < routers.RETRY_SECONDS
