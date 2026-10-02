import gzip
import subprocess
import sys
from pathlib import Path

import pytest
from django.db import connections

from proteins.accession.cli import main
from proteins.accession.fasta import read_fasta
from proteins.tests.conftest import role_dsn

FIXTURES = Path(__file__).parent.parent / "fixtures"
REPO = Path(__file__).resolve().parent.parent.parent

db = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)


def query(sql, params=None):
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def counts():
    return [
        query(f"SELECT count(*) FROM proteindb.{table}")[0][0]
        for table in (
            "protein_key",
            "assembly",
            "gene_caller",
            "staging_protein",
            "staging_contig",
            "staging_occurrence",
        )
    ]


@pytest.fixture
def dsn(connect_accession, monkeypatch):
    monkeypatch.setenv("PROTEINDB_DSN", role_dsn("proteindb_accession"))


@pytest.fixture
def unreachable(monkeypatch):
    """A DSN that fails any connection, for checks that must come before one."""
    monkeypatch.setenv("PROTEINDB_DSN", "host=unreachable.invalid connect_timeout=1")


def edited(path: Path, tmp_path: Path, edit) -> Path:
    """A copy of a fixture, its text passed through edit."""
    with gzip.open(path, "rt") as f:
        text = edit(f.read())
    copy = tmp_path / path.name
    copy.write_text(text)
    return copy


def args(tmp_path, faa=None, gff=None, version="6.0", out="out.faa.gz", extra=()):
    return [
        "--assembly",
        "ERZ101",
        "--pipeline-version",
        version,
        "--faa",
        str(faa or FIXTURES / "ERZ101.faa.gz"),
        "--gff",
        str(gff or FIXTURES / "ERZ101.gff.gz"),
        "--contigs",
        str(FIXTURES / "ERZ101.fasta.gz"),
        "--contig-map",
        str(FIXTURES / "ERZ101_mapping.csv"),
        "--out",
        str(tmp_path / out),
        *extra,
    ]


def first_sequence_starting_with(character):
    def edit(text):
        header, sequence, rest = text.split("\n", 2)
        return f"{header}\n{character}{sequence[1:]}\n{rest}"

    return edit


def drop_last_cds(text):
    return "\n".join(text.rstrip("\n").split("\n")[:-1]) + "\n"


def duplicate_first_record(text):
    header, sequence, _ = text.split("\n", 2)
    return f"{header}\n{sequence}\n{text}"


@pytest.mark.parametrize(
    "faa_edit, gff_edit, version, message",
    [
        (
            first_sequence_starting_with("ß"),
            None,
            "6.0",
            "ERZ101_2_5: invalid character 'ß' at position 1",
        ),
        (
            first_sequence_starting_with("ı"),
            None,
            "6.0",
            "ERZ101_2_5: invalid character 'ı' at position 1",
        ),
        (
            first_sequence_starting_with("ﬁ"),
            None,
            "6.0",
            "ERZ101_2_5: invalid character 'ﬁ' at position 1",
        ),
        (
            first_sequence_starting_with("*"),
            None,
            "6.0",
            "ERZ101_2_5: invalid character '*' at position 1",
        ),
        (duplicate_first_record, None, "6.0", "ERZ101_2_5: more than one FASTA record"),
        (None, drop_last_cds, "6.0", "not in the GFF"),
        (
            None,
            lambda t: t.replace("ERZ101_8\t", "ERZ101_99\t"),
            "6.0",
            "is not in the contigs file",
        ),
        (
            None,
            lambda t: t.replace("Pyrodigal_v3.6.3", "Pyrodigal"),
            "6.0",
            "has no gene caller version",
        ),
        (None, None, "6.10", "one digit each"),
        (None, None, "10.0", "one digit each"),
    ],
)
def test_invalid_input_exits_2_before_connecting(
    tmp_path, unreachable, caplog, faa_edit, gff_edit, version, message
):
    faa = faa_edit and edited(FIXTURES / "ERZ101.faa.gz", tmp_path, faa_edit)
    gff = gff_edit and edited(FIXTURES / "ERZ101.gff.gz", tmp_path, gff_edit)
    assert main(args(tmp_path, faa, gff, version)) == 2
    assert message in caplog.text
    assert not (tmp_path / "out.faa.gz").exists()


def test_without_dsn_exits_1(tmp_path, monkeypatch):
    monkeypatch.delenv("PROTEINDB_DSN", raising=False)
    assert main(args(tmp_path)) == 1


@pytest.mark.parametrize("connections", ["0", "-1", "x"])
def test_connections_must_be_positive(tmp_path, connections):
    with pytest.raises(SystemExit) as excinfo:
        main(args(tmp_path, extra=["--connections", connections]))
    assert excinfo.value.code == 2


def test_version(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(["--version"])
    assert excinfo.value.code == 0
    assert capsys.readouterr().out.strip()


def test_runs_as_a_module(tmp_path, unreachable):
    result = subprocess.run(
        [sys.executable, "-m", "proteins.accession", *args(tmp_path, version="10.0")],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "one digit each" in result.stderr


@db
def test_accessions_and_writes_the_output(tmp_path, dsn):
    assert main(args(tmp_path, version="6.1.2")) == 0

    assert query("SELECT accession, pipeline_version FROM proteindb.assembly") == [
        ("ERZ101", "6.1")
    ]
    stored = {
        f"MGYP{id:012d}" for (id,) in query("SELECT id FROM proteindb.protein_key")
    }
    records = list(read_fasta(tmp_path / "out.faa.gz"))
    assert len(records) == 105
    assert {r.description.split()[0] for r in records} == stored


@db
def test_rerun_writes_the_same_output_and_stages_nothing(tmp_path, dsn, caplog):
    assert main(args(tmp_path, out="first.faa.gz")) == 0
    before = counts()

    assert main(args(tmp_path, out="second.faa.gz")) == 0
    assert counts() == before
    assert (tmp_path / "first.faa.gz").read_bytes() == (
        tmp_path / "second.faa.gz"
    ).read_bytes()
    assert "already accessioned" in caplog.text


@db
def test_rerun_with_a_protein_without_mgyp_exits_3(tmp_path, dsn):
    assert main(args(tmp_path)) == 0
    before = counts()

    faa = edited(
        FIXTURES / "ERZ101.faa.gz",
        tmp_path,
        lambda t: t + ">ERZ101_1_99 # 1 # 9 # 1 # ID=1_99;partial=00\nMNEWPROTEIN\n",
    )
    gff = edited(
        FIXTURES / "ERZ101.gff.gz",
        tmp_path,
        lambda t: t
        + "ERZ101_1\tPyrodigal_v3.6.3\tCDS\t1\t33\t.\t+\t0\tID=ERZ101_1_99\n",
    )
    assert main(args(tmp_path, faa, gff, out="rerun.faa.gz")) == 3
    assert counts() == before
    assert not (tmp_path / "rerun.faa.gz").exists()


@db
def test_lookup_only_writes_nothing_to_the_database(tmp_path, dsn, capsys):
    before = counts()
    assert main(args(tmp_path, out="before.tsv", extra=["--lookup-only"])) == 0
    assert counts() == before
    assert "found: 0\tnot found: 105" in capsys.readouterr().out
    lines = (tmp_path / "before.tsv").read_text().splitlines()
    assert len(lines) == 105
    assert all(line.endswith("\t") for line in lines)

    assert main(args(tmp_path)) == 0
    before = counts()
    assert main(args(tmp_path, out="after.tsv", extra=["--lookup-only"])) == 0
    assert counts() == before
    assert "found: 105\tnot found: 0" in capsys.readouterr().out
    assert [
        line.split("\t") for line in (tmp_path / "after.tsv").read_text().splitlines()
    ] == [[r.id, r.description.split()[0]] for r in read_fasta(tmp_path / "out.faa.gz")]
