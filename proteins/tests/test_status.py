import subprocess
from datetime import date, timedelta

import pytest

from analyses.models import Analysis
from proteins.dataset import status as status_module
from proteins.dataset.owner import take
from proteins.dataset.status import status
from proteins.routers import MGYP
from proteins.tests.conftest import (
    SUBMITTED,
    make_analysis,
    make_study,
    query,
    write_v1_analyses,
)
from workflows.flows.proteins import check_proteins_status as flow

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("mgyp_as_accession", "prefect_harness"),
]

D1, D2 = date(2026, 10, 1), date(2026, 10, 2)


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(flow.EMG_CONFIG.proteins, "dataset_dir", str(tmp_path))
    return tmp_path


def record(mgya, staged_ago):
    """An assembly proteins record with one protein staged that long ago."""
    ((id,),) = query(
        "WITH a AS (INSERT INTO proteindb.assembly (accession) VALUES (%s) RETURNING id)"
        " INSERT INTO proteindb.assembly_proteins"
        " (assembly_id, source, pipeline_version, analysis_accession)"
        " SELECT id, 'assembly_analysis', '6.0', %s FROM a RETURNING id",
        [f"ERZ-{mgya}", mgya],
    )
    query(
        "INSERT INTO proteindb.staged_protein"
        " (id, hash, sequence, assembly_proteins_id, staged_at)"
        " VALUES (%s, %s, 'M', %s, now() - %s)",
        [id, bytes(32), id, staged_ago],
    )


def done(day, finished_ago, root):
    query(
        "INSERT INTO proteindb.dataset_update (day, status, finished_at)"
        " VALUES (%s, 'done', now() - %s)",
        [day, finished_ago],
    )
    (root / "metadata" / f"day={day}").mkdir(parents=True)


def test_no_check_fails_on_a_healthy_state(root, slurm):
    sacct, _ = slurm
    study = make_study("PRJ1")
    v6 = make_analysis(study, "ERZ1")
    make_analysis(study, "ERZ2", "V4.1")
    make_analysis(study, "ERZ3", "V5")
    make_analysis(study, "ERZ4", "V4", state=Analysis.AnalysisStates.ANALYSIS_STARTED)
    record(v6.accession, timedelta(hours=47))
    done(D1, timedelta(hours=35), root)
    # A day after the last update: the one being written.
    (root / "occurrence" / f"day={D2}").mkdir(parents=True)
    owner = take("update")
    sacct[2] = f"RUNNING|{SUBMITTED[2]}\n"

    now = status(root)
    assert now.problems() == []
    flow.check_proteins_status()
    lines = now.report().splitlines()
    assert lines[-4:] == [
        "Annotated assembly analyses of pipeline versions with no gene-caller rule: none",
        "MGYAs of assembly proteins records in neither emgapi-v2 nor MGnify v1: 0",
        "MGYAs whose metadata is from MGnify v1: none, as the protein dataset has no v1/analyses.parquet",
        f"Owner of the protein dataset: update, execution {owner.execution_id},"
        f" flow run None, since {now.owner.acquired_at}, SLURM job 2"
        f" submitted at {now.owner.slurm_submit_at}, whose states are RUNNING",
    ]


def test_each_failed_check_is_reported_and_fails_the_flow(root, slurm, monkeypatch):
    sacct, as_job = slurm
    study = make_study("PRJ1")
    make_analysis(study, "ERZ1", "V4")
    make_analysis(study, "ERZ2", "V4")
    make_analysis(study, "ERZ3", "V1")
    record("MGYA99999999", timedelta(hours=49))
    done(D2, timedelta(hours=37), root)
    (root / "occurrence" / f"day={D1}").mkdir(parents=True)
    as_job(1)
    take("update")
    sacct[1] = f"FAILED|{SUBMITTED[1]}\n"

    now = status(root)
    # Protein ids at the limit, without advancing the sequence that other tests share.
    monkeypatch.setattr(status_module, "MAX_PROTEIN_ID", now.last_protein_id)
    problems = [
        f"the last dataset update finished at {now.last_update}",
        f"rows have been staged since {now.oldest_staged}",
        f"incomplete days of the protein dataset before the last dataset update: {D1}",
        f"protein ids are at {now.last_protein_id}, above 90% of {now.last_protein_id}",
        "annotated assembly analyses of pipeline versions with no gene-caller rule,"
        " which the listing skips: V1 1, V4 2",
        "1 MGYAs of assembly proteins records are in neither emgapi-v2 nor MGnify v1,"
        " so releases publish none of their proteins",
        "the protein dataset is owned by job 1, whose states are FAILED",
    ]
    assert now.problems() == problems
    assert (
        "MGYAs of assembly proteins records in neither emgapi-v2 nor MGnify v1: 1,"
        " for example MGYA99999999" in now.report()
    )
    with pytest.raises(flow.StatusCheckFailed, match="7 checks failed"):
        flow.check_proteins_status()


def test_counts_the_mgyas_from_v1_and_reports_its_copy_once_unused(root):
    v6 = make_analysis(make_study("PRJ1"), "ERZ1")
    record(v6.accession, timedelta(hours=1))
    # emgapi-v2 wins over the copy, so no record uses it.
    write_v1_analyses(root, v6.accession, "MGYA99999998")
    assert status(root).problems()[-1] == (
        "no MGYA's metadata is from MGnify v1 any more, so v1/analyses.parquet in the"
        " protein dataset can be deleted"
    )

    record("MGYA99999998", timedelta(hours=1))
    now = status(root)
    assert now.withheld == [] and now.from_v1 == 1
    assert "MGYAs whose metadata is from MGnify v1: 1" in now.report().splitlines()
    assert not any("v1" in problem for problem in now.problems())


@pytest.mark.parametrize(
    "sacct_output, states",
    [
        ("", "not found"),
        (f"COMPLETING|{SUBMITTED[2]}\n", "COMPLETING"),
        (f"NODE_FAIL|{SUBMITTED[2]}\n", "NODE_FAIL"),
        (f"RUNNING|{SUBMITTED[1]}\n", "not found"),
        (subprocess.CalledProcessError(1, "sacct"), "unknown, as sacct failed"),
    ],
    ids=["unknown", "completing", "node-failed", "reused-id", "sacct-fails"],
)
def test_an_owner_whose_job_is_not_running_fails_the_check(
    root, slurm, sacct_output, states
):
    sacct, _ = slurm
    take("update")
    sacct[2] = sacct_output
    problem = status(root).problems()[-1]
    assert problem.startswith(
        f"the protein dataset is owned by job 2, whose states are {states}"
    )
