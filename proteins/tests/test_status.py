from datetime import date

import pyarrow as pa
import pytest
from django.core.management import CommandError, call_command

from proteins import tier2
from proteins.status import status
from proteins.tests.conftest import SUBMITTED, query, role_dsn, submitted

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)

D = date(2026, 10, 2)


def done(day=D, hours_ago=1):
    query(
        "INSERT INTO proteindb.load_log (ingest_date, status, started_at, finished_at)"
        " VALUES (%s, 'done', now() - make_interval(hours => %s + 1), now() - make_interval(hours => %s))",
        [day, hours_ago, hours_ago],
    )


@pytest.fixture
def root(tmp_path, slurm):
    """Tier 2 with day D complete, whose snapshot has one assembly emgapi-v2 did not know, under two versions."""
    tier2.write(
        tmp_path / "tier2" / "dims" / f"snapshot={D}" / "assembly.parquet",
        "dims/assembly",
        pa.Table.from_pylist(
            [
                {
                    "id": 1,
                    "accession": "ERZ1",
                    "pipeline_version": "6.0",
                    "study_id": 1,
                },
                {"id": 2, "accession": "ERZ9", "pipeline_version": "5.0"},
                {"id": 3, "accession": "ERZ9", "pipeline_version": "6.0"},
            ],
            tier2.SCHEMAS["dims/assembly"],
        ),
    )
    return tmp_path / "tier2"


def read(root):
    return status(role_dsn("proteindb_load"), root)


@pytest.fixture
def protein_id_seq(tier1):
    (before,) = query("SELECT last_value, is_called FROM proteindb.protein_id_seq")
    yield
    query("SELECT setval('proteindb.protein_id_seq', %s, %s)", list(before))


def test_a_healthy_system_has_no_problems(root):
    done()
    query(
        "INSERT INTO proteindb.load_log (ingest_date, status, message) VALUES (%s, 'failed', 'disk full')",
        [date(2026, 10, 3)],
    )
    (root / "occurrence" / "ingest_date=2026-10-03").mkdir(parents=True)

    now = read(root)

    assert now.problems() == []
    assert now.last_day == D
    assert now.failed == [(date(2026, 10, 3), "disk full")]
    assert now.staged == {
        "staging_protein": 0,
        "staging_contig": 0,
        "staging_occurrence": 0,
    }
    assert now.oldest_staged is None
    assert now.owner is None
    assert now.complete_day == D
    assert now.incomplete_days == [date(2026, 10, 3)]
    assert now.unresolved == ["ERZ9"]


def test_no_successful_load_fails(root):
    assert read(root).problems() == ["the last successful load finished at None"]


def test_an_overdue_load_fails(root):
    done(hours_ago=37)
    (problem,) = read(root).problems()
    assert problem.startswith("the last successful load finished at")


def test_rows_staged_long_ago_fail(root):
    done()
    query(
        "INSERT INTO proteindb.assembly (id, accession, pipeline_version) VALUES (1, 'ERZ1', '6.0');"
        "INSERT INTO proteindb.staging_protein (id, hash, sequence, assembly_id, staged_at)"
        " VALUES (1, '\\x00', 'M', 1, now() - interval '49 hours'), (2, '\\x01', 'M', 1, now())"
    )
    now = read(root)
    assert now.staged["staging_protein"] == 2
    (problem,) = now.problems()
    assert problem.startswith("rows have been staged since")


def own(job):
    query(
        "UPDATE proteindb.tier2_owner SET flow = 'load', slurm_job_id = %s, slurm_submit_at = %s",
        [job, submitted(job)],
    )


def test_an_owner_whose_job_runs_is_fine(root, slurm):
    sacct, _ = slurm
    done()
    own(1)
    sacct[1] = f"RUNNING|{SUBMITTED[1]}\n"
    now = read(root)
    assert now.owner == ("load", 1, submitted(1), ["RUNNING"])
    assert now.problems() == []


@pytest.mark.parametrize(
    "sacct_output, states",
    [
        (f"FAILED|{SUBMITTED[1]}\n", "FAILED"),
        (f"RUNNING|{SUBMITTED[2]}\n", "not found"),
        (OSError("no sacct"), "unknown, as sacct failed: no sacct"),
    ],
)
def test_an_owner_whose_job_is_not_running_fails(root, slurm, sacct_output, states):
    sacct, _ = slurm
    done()
    own(1)
    sacct[1] = sacct_output
    assert read(root).problems() == [
        f"Tier 2 is owned by job 1, which is not running: {states}"
    ]


def test_an_incomplete_day_before_the_last_load_fails(root):
    done()
    (root / "contig" / "ingest_date=2026-10-01").mkdir(parents=True)
    assert read(root).problems() == [
        "incomplete Tier 2 days before the last load: 2026-10-01"
    ]


def test_protein_ids_near_the_end_of_the_range_fail(root, protein_id_seq):
    done()
    query("SELECT setval('proteindb.protein_id_seq', 900_000_000_000)")
    (problem,) = read(root).problems()
    assert problem.startswith("protein ids are at 900000000000, above 90%")


def test_command_prints_the_state_and_checks_it(
    root, settings, monkeypatch, capsys, protein_id_seq
):
    monkeypatch.setattr(settings.EMG_CONFIG.proteindb, "root", str(root.parent))
    done()
    call_command("proteindb_status", "--check")
    out = capsys.readouterr().out
    assert f"Last successful load: day {D}" in out
    assert "Unresolved assemblies in the latest snapshot: 1\n  ERZ9" in out

    query("SELECT setval('proteindb.protein_id_seq', 999_000_000_000)")
    call_command("proteindb_status")
    assert "FAILED: protein ids are at" in capsys.readouterr().err
    with pytest.raises(CommandError, match="1 checks failed"):
        call_command("proteindb_status", "--check")
