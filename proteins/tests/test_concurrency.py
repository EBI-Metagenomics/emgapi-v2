"""mgyp-accession run as processes: concurrent jobs, killed jobs and overlapping attempts."""

import os
import random
import subprocess
import sys
import time
from pathlib import Path

import pytest
from django.db import connections

from proteins.accession.accession import write_output
from proteins.accession.fasta import read_fasta
from proteins.accession.inputs import read_input
from proteins.tests.conftest import role_dsn

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)

REPO = Path(__file__).resolve().parent.parent.parent
TIMEOUT = 120  # seconds; a job still running then is taken to be deadlocked

# Runs mgyp-accession with one function of its module replaced, to stop it at a given point.
PATCHED = """
import os, signal, sys, time
from pathlib import Path
import proteins.accession.accession as accession
import proteins.accession.cli as cli

copy_rows, point, signal_file, release_file = accession.copy_rows, *sys.argv[1:4]

def before_occurrences(conn, table, *rest):
    if table.startswith("staging_occurrence"):
        if point == "kill-before-commit":
            os.kill(os.getpid(), signal.SIGKILL)
        Path(signal_file).touch()
        while not Path(release_file).exists():
            time.sleep(0.05)
    return copy_rows(conn, table, *rest)

def kill(*args):
    os.kill(os.getpid(), signal.SIGKILL)

accession.copy_rows = before_occurrences
if point == "kill-after-commit":
    cli.write_output = kill
sys.exit(cli.main(sys.argv[4:]))
"""


def query(sql, params=None):
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def count(table):
    return query(f"SELECT count(*) FROM proteindb.{table}")[0][0]


@pytest.fixture
def env(connect_accession):
    return {**os.environ, "PROTEINDB_DSN": role_dsn("proteindb_accession")}


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


def arguments(directory: Path, assembly: str, out="out.faa"):
    return [
        "--assembly",
        assembly,
        "--pipeline-version",
        "6.0",
        "--faa",
        str(directory / "in.faa"),
        "--gff",
        str(directory / "in.gff"),
        "--contigs",
        str(directory / "contigs.fasta"),
        "--out",
        str(directory / out),
        "--connections",
        "4",
    ]


def start(env, args, patched_at=None, signal_file="", release_file=""):
    command = (
        [sys.executable, "-c", PATCHED, patched_at, str(signal_file), str(release_file)]
        if patched_at
        else [sys.executable, "-m", "proteins.accession"]
    )
    return subprocess.Popen(
        [*command, *args],
        cwd=REPO,
        env=env,
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


def mgyps(path: Path) -> dict[str, str]:
    return {r.sequence: r.description.split()[0] for r in read_fasta(path)}


def random_sequences(rng, n):
    return ["M" + "".join(rng.choices("ACDEFGHIKLMNPQRSTVWY", k=40)) for _ in range(n)]


def test_concurrent_jobs_on_overlapping_assemblies(tmp_path, env):
    rng = random.Random(0)
    for round in range(20):
        shared = random_sequences(rng, 150)
        # Each round's jobs meet a new gene caller version together.
        caller = f"Pyrodigal_v9.{round}"
        directories = [
            write_assembly(
                tmp_path / f"{round}" / f"{job}",
                rng.sample(shared, 120) + random_sequences(rng, 30),
                caller,
            )
            for job in range(8)
        ]
        before = count("protein_key")

        processes = [
            start(env, arguments(d, f"ERZ{round:02d}{job}"))
            for job, d in enumerate(directories)
        ]
        assert [finish(p) for p in processes] == [0] * 8

        outputs = [mgyps(d / "out.faa") for d in directories]
        sequences = {s for output in outputs for s in output}
        assert count("protein_key") - before == len(sequences)
        stored = {
            sequence: f"MGYP{id:012d}"
            for sequence, id in query(
                "SELECT p.sequence, k.id FROM proteindb.staging_protein p"
                " JOIN proteindb.protein_key k USING (hash)"
            )
        }
        for output in outputs:
            assert output == {s: stored[s] for s in output}
        assert query(
            "SELECT count(*) FROM proteindb.gene_caller WHERE version = %s",
            [caller.split("_v")[1]],
        ) == [(1,)]
        assert query(
            "SELECT count(DISTINCT gene_caller_id) FROM proteindb.staging_occurrence o"
            " JOIN proteindb.assembly a ON a.id = o.assembly_id"
            " WHERE a.accession LIKE %s",
            [f"ERZ{round:02d}_"],
        ) == [(1,)]
    assert count("staging_protein") == count("protein_key")


@pytest.fixture
def assembly(tmp_path):
    return write_assembly(tmp_path / "ERZ1", random_sequences(random.Random(1), 50))


def expected_output(directory: Path, path: Path) -> bytes:
    """What a clean run writes, given the ids now stored."""
    input = read_input(
        directory / "in.faa", directory / "in.gff", directory / "contigs.fasta"
    )
    ids = dict(query("SELECT hash, id FROM proteindb.protein_key"))
    write_output(path, input.occurrences, ids)
    return path.read_bytes()


def staged_counts():
    return [
        count(table)
        for table in (
            "protein_key",
            "assembly",
            "staging_protein",
            "staging_contig",
            "staging_occurrence",
        )
    ]


def test_job_killed_after_commit_is_rerun(tmp_path, env, assembly):
    assert finish(start(env, arguments(assembly, "ERZ1"), "kill-after-commit")) == -9
    assert sorted(p.name for p in assembly.iterdir()) == [
        "contigs.fasta",
        "in.faa",
        "in.gff",
    ]
    committed = staged_counts()
    assert committed == [50, 1, 50, 1, 50]

    assert finish(start(env, arguments(assembly, "ERZ1"))) == 0
    assert staged_counts() == committed
    assert (assembly / "out.faa").read_bytes() == expected_output(
        assembly, tmp_path / "expected.faa"
    )


def test_job_killed_before_commit_leaves_nothing(env, assembly):
    assert finish(start(env, arguments(assembly, "ERZ1"), "kill-before-commit")) == -9
    assert staged_counts() == [0, 0, 0, 0, 0]

    assert finish(start(env, arguments(assembly, "ERZ1"))) == 0
    assert staged_counts() == [50, 1, 50, 1, 50]


def wait_for(condition, what):
    deadline = time.monotonic() + TIMEOUT
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"no {what} after {TIMEOUT} s")
        time.sleep(0.05)


def test_overlapping_attempts_of_one_analysis(tmp_path, env, assembly):
    inside, release = tmp_path / "inside", tmp_path / "release"
    first = start(
        env,
        arguments(assembly, "ERZ1", "first.faa"),
        "pause-before-commit",
        inside,
        release,
    )
    wait_for(inside.exists, "first attempt inside its transaction")

    second = start(env, arguments(assembly, "ERZ1", "second.faa"))
    wait_for(
        lambda: query(
            "SELECT count(*) FROM pg_stat_activity"
            " WHERE datname = current_database()"
            " AND application_name = 'mgyp-accession ERZ1' AND wait_event_type = 'Lock'"
        )
        == [(1,)],
        "second attempt waiting on the first",
    )
    release.touch()

    assert finish(first) == 0
    assert finish(second) == 0
    assert (assembly / "second.faa").read_bytes() == (
        assembly / "first.faa"
    ).read_bytes()
    assert staged_counts() == [50, 1, 50, 1, 50]
