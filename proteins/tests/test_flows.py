from datetime import datetime, timezone

import psycopg
import pytest
from django.db import connections

from activate_django_first import EMG_CONFIG

from proteins import tier2
from proteins.accession.accession import accession
from proteins.flows import check as check_flow
from proteins.flows import load as load_flow
from proteins.tests.conftest import query, read_published, role_dsn

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)


def test_the_dsn_names_the_proteindb_database():
    with psycopg.connect(load_flow.proteindb_dsn()) as conn:
        assert (
            conn.execute("SELECT current_database()").fetchone()[0]
            == connections["proteindb"].settings_dict["NAME"]
        )


def test_the_load_flow_loads_todays_staging_as_the_owner_of_tier2(
    monkeypatch, tmp_path, connect_accession, slurm, limit
):
    accession(
        connect_accession,
        "ERZ29562087",
        "6.0",
        read_published("ERZ29562087"),
        connections=4,
    )
    monkeypatch.setattr(load_flow, "proteindb_dsn", lambda: role_dsn("proteindb_load"))
    monkeypatch.setattr(EMG_CONFIG.proteindb, "root", str(tmp_path))
    monkeypatch.setattr(EMG_CONFIG.proteindb, "tier2_concurrency_limit", limit())

    before = datetime.now(timezone.utc).date()
    load_flow.proteindb_load()
    after = datetime.now(timezone.utc).date()

    (day,) = tier2.complete_days(tmp_path / "tier2")
    assert day in {before, after}
    # emgapi-v2 has no analysis of it
    assert query("SELECT status, unresolved_assemblies FROM proteindb.load_log") == [
        ("done", 1)
    ]
    assert query("SELECT execution_id FROM proteindb.tier2_owner") == [(None,)]


@pytest.fixture
def checked(monkeypatch, tmp_path, slurm):
    monkeypatch.setattr(check_flow, "proteindb_dsn", lambda: role_dsn("proteindb_load"))
    monkeypatch.setattr(EMG_CONFIG.proteindb, "root", str(tmp_path))


def test_the_check_flow_succeeds_on_a_healthy_system(
    prefect_harness, checked, tmp_path
):
    query(
        "INSERT INTO proteindb.load_log (ingest_date, status, finished_at)"
        " VALUES (current_date, 'done', now())"
    )
    check_flow.proteindb_check()


def test_the_check_flow_fails_on_a_failed_check(prefect_harness, checked):
    with pytest.raises(
        check_flow.CheckFailed,
        match="1 checks failed: the last successful load finished at None",
    ):
        check_flow.proteindb_check()
