"""The accessioning core run as processes: concurrent analyses, killed attempts and overlapping attempts."""

import json
import random
import subprocess
import sys
import time
from pathlib import Path

import pytest
from psycopg.conninfo import make_conninfo

from proteins.routers import MGYP
from proteins.tests.conftest import query, role_dsn, write_assembly

pytestmark = pytest.mark.django_db(databases=["default", MGYP], transaction=True)

REPO = Path(__file__).resolve().parent.parent.parent
TIMEOUT = 120  # seconds; an attempt still running then is taken to be deadlocked

# Accessions one assembly, with one function of the core replaced to stop it at a given point.
# Writes each sequence's protein id to <directory>/<name>.json.
RUN = """
import json, os, signal, sys, time
from contextlib import contextmanager
from pathlib import Path

import psycopg

import proteins.accession.accession as core
from proteins.accession.inputs import read_input

dsn, directory, assembly, mgya, name, point, signal_file, release_file = sys.argv[1:]
directory = Path(directory)


@contextmanager
def connect():
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn


copy_rows, stage = core.copy_rows, core.stage


def before_occurrences(conn, table, *rest):
    if table.startswith("staged_occurrence"):
        if point == "kill-before-commit":
            os.kill(os.getpid(), signal.SIGKILL)
        if point == "pause-before-commit":
            Path(signal_file).touch()
            while not Path(release_file).exists():
                time.sleep(0.05)
    return copy_rows(conn, table, *rest)


def stage_then_kill(*args):
    stage(*args)
    os.kill(os.getpid(), signal.SIGKILL)


core.copy_rows = before_occurrences
if point == "kill-after-commit":
    core.stage = stage_then_kill
input = read_input(directory / "in.faa", directory / "in.gff", directory / "contigs.fasta")
ids = core.accession(connect, assembly, "assembly_analysis", "6.0", input, mgya, connections=4)
(directory / f"{name}.json").write_text(json.dumps({s: ids[h] for h, s in input.proteins}))
"""


def start(directory, assembly, mgya, name="out", point="", signal_file="", release=""):
    dsn = make_conninfo(role_dsn("proteindb_accession"), application_name=name)
    return subprocess.Popen(
        [sys.executable, "-c", RUN, dsn, str(directory), assembly, mgya, name, point]
        + [str(signal_file), str(release)],
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def finish(process) -> int:
    try:
        _, stderr = process.communicate(timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        process.kill()
        pytest.fail(f"still running after {TIMEOUT} s")
    assert process.returncode in (0, -9), stderr
    return process.returncode


def ids(directory: Path, name="out") -> dict[str, int]:
    return json.loads((directory / f"{name}.json").read_text())


def count(table):
    return query(f"SELECT count(*) FROM proteindb.{table}")[0][0]


def random_sequences(rng, n):
    return ["M" + "".join(rng.choices("ACDEFGHIKLMNPQRSTVWY", k=40)) for _ in range(n)]


def test_concurrent_analyses_of_overlapping_assemblies(tmp_path, connect_accession):
    rng = random.Random(0)
    for round in range(20):
        shared = random_sequences(rng, 150)
        # Each round's analyses meet a new gene caller version together.
        caller = f"Pyrodigal_v9.{round}"
        directories = [
            write_assembly(
                tmp_path / f"{round}" / f"{job}",
                rng.sample(shared, 120) + random_sequences(rng, 30),
                caller,
            )
            for job in range(8)
        ]
        before = count("protein")

        processes = [
            start(d, f"ERZ{round:02d}{job}", f"MGYA{round:02d}{job}")
            for job, d in enumerate(directories)
        ]
        assert [finish(p) for p in processes] == [0] * 8

        outputs = [ids(d) for d in directories]
        sequences = {s for output in outputs for s in output}
        assert count("protein") - before == len(sequences)
        stored = dict(
            query(
                "SELECT s.sequence, p.id FROM proteindb.staged_protein s"
                " JOIN proteindb.protein p USING (hash)"
            )
        )
        for output in outputs:
            assert output == {s: stored[s] for s in output}
        assert query(
            "SELECT count(*) FROM proteindb.gene_caller WHERE version = %s",
            [caller.split("_v")[1]],
        ) == [(1,)]
        assert query(
            "SELECT count(DISTINCT gene_caller_id) FROM proteindb.staged_occurrence o"
            " JOIN proteindb.assembly_proteins a ON a.id = o.assembly_proteins_id"
            " WHERE a.assembly_accession LIKE %s",
            [f"ERZ{round:02d}_"],
        ) == [(1,)]
    assert count("staged_protein") == count("protein")


@pytest.fixture
def assembly(tmp_path, connect_accession):
    return write_assembly(tmp_path / "ERZ1", random_sequences(random.Random(1), 50))


def staged_counts():
    return [
        count(table)
        for table in (
            "protein",
            "assembly_proteins",
            "staged_protein",
            "staged_contig",
            "staged_occurrence",
        )
    ]


def test_attempt_killed_after_commit_is_rerun(assembly):
    assert finish(start(assembly, "ERZ1", "MGYA1", point="kill-after-commit")) == -9
    committed = staged_counts()
    assert committed == [50, 1, 50, 1, 50]

    assert finish(start(assembly, "ERZ1", "MGYA1")) == 0
    assert staged_counts() == committed
    assert ids(assembly) == dict(
        query("SELECT sequence, id FROM proteindb.staged_protein")
    )


def test_attempt_killed_before_commit_leaves_nothing(assembly):
    assert finish(start(assembly, "ERZ1", "MGYA1", point="kill-before-commit")) == -9
    assert staged_counts() == [0, 0, 0, 0, 0]

    assert finish(start(assembly, "ERZ1", "MGYA1")) == 0
    assert staged_counts() == [50, 1, 50, 1, 50]


def wait_for(condition, what):
    deadline = time.monotonic() + TIMEOUT
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"no {what} after {TIMEOUT} s")
        time.sleep(0.05)


def test_overlapping_attempts_of_one_analysis(tmp_path, assembly):
    inside, release = tmp_path / "inside", tmp_path / "release"
    first = start(
        assembly, "ERZ1", "MGYA1", "first", "pause-before-commit", inside, release
    )
    wait_for(inside.exists, "first attempt inside its transaction")

    second = start(assembly, "ERZ1", "MGYA1", "second")
    wait_for(
        lambda: query(
            "SELECT count(*) FROM pg_stat_activity"
            " WHERE datname = current_database()"
            " AND application_name = 'second' AND wait_event_type = 'Lock'"
        )
        == [(1,)],
        "second attempt waiting on the first",
    )
    release.touch()

    assert finish(first) == 0
    assert finish(second) == 0
    assert ids(assembly, "second") == ids(assembly, "first")
    assert staged_counts() == [50, 1, 50, 1, 50]
