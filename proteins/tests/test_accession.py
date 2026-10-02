import logging
import threading
from pathlib import Path

import psycopg
import pytest
from django.db import connections
from psycopg.conninfo import make_conninfo

from proteins.accession.accession import (
    CannotConnect,
    IncompleteRerun,
    accession,
    connector,
    lookup,
    lookup_partitions,
    register_gene_callers,
    retry_on_connect_failure,
    write_output,
)
from proteins.accession.contract import protein_hash
from proteins.accession.fasta import read_fasta
from proteins.accession.inputs import Contig, Input, Occurrence, read_input
from proteins.tests.conftest import role_dsn

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture(scope="module")
def erz101():
    return read_input(
        FIXTURES / "ERZ101.faa.gz",
        FIXTURES / "ERZ101.gff.gz",
        FIXTURES / "ERZ101.fasta.gz",
        FIXTURES / "ERZ101_mapping.csv",
    )


def query(sql, params=None):
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else None


def counts():
    return {
        table: query(f"SELECT count(*) FROM proteindb.{table}")[0][0]
        for table in (
            "protein_key",
            "assembly",
            "staging_protein",
            "staging_contig",
            "staging_occurrence",
        )
    }


def test_new_assembly_is_staged_exactly(connect_accession, erz101):
    ids = accession(connect_accession, "ERZ101", "6.0", erz101)

    assert set(ids) == {hash for hash, _ in erz101.proteins}
    assert len(set(ids.values())) == len(ids)
    assert dict(query("SELECT hash, id FROM proteindb.protein_key")) == ids
    ((assembly_id, accession_, version),) = query(
        "SELECT id, accession, pipeline_version FROM proteindb.assembly"
    )
    assert (accession_, version) == ("ERZ101", "6.0")
    assert set(
        query("SELECT id, hash, sequence, assembly_id FROM proteindb.staging_protein")
    ) == {
        (ids[hash], hash, sequence, assembly_id) for hash, sequence in erz101.proteins
    }
    assert set(
        query(
            "SELECT name, original_name, length, hash, kmer_coverage"
            " FROM proteindb.staging_contig WHERE assembly_id = %s",
            [assembly_id],
        )
    ) == set(erz101.contigs)
    assert (
        sorted(
            query(
                """
            SELECT o.gene_id, o.protein_id, c.name, g.name, g.version,
                   o.start_position, o.end_position, o.strand, o.truncation
            FROM proteindb.staging_occurrence o
            JOIN proteindb.staging_contig c ON c.id = o.contig_id
            JOIN proteindb.gene_caller g ON g.id = o.gene_caller_id
            WHERE o.assembly_id = %s
            """,
                [assembly_id],
            )
        )
        == sorted(
            (
                o.gene_id,
                ids[o.hash],
                o.contig,
                o.caller_name,
                o.caller_version,
                o.start,
                o.end,
                o.strand,
                o.truncation,
            )
            for o in erz101.occurrences
        )
    )


def test_truncation_is_staged_unchanged(connect_accession):
    genes = [
        (f"g{n}", "MK" + "V" * n, strand, truncation)
        for n, (strand, truncation) in enumerate(
            (strand, truncation)
            for strand in (1, -1)
            for truncation in ("00", "01", "10", "11", None)
        )
    ]
    occurrences = [
        Occurrence(id, "", seq, protein_hash(seq), "c1", 1, 9, strand, "P", "1", t)
        for id, seq, strand, t in genes
    ]
    input = Input(
        occurrences,
        sorted({(o.hash, o.sequence) for o in occurrences}),
        [Contig("c1", None, 9, b"\0" * 32, None)],
    )
    accession(connect_accession, "ERZ1", "6.0", input)

    assert sorted(
        query("SELECT gene_id, strand, truncation FROM proteindb.staging_occurrence")
    ) == sorted((id, strand, t) for id, _, strand, t in genes)


def test_assembly_with_only_known_proteins_adds_none(connect_accession, erz101):
    first = accession(connect_accession, "ERZ101", "6.0", erz101)
    before = counts()

    assert accession(connect_accession, "ERZ102", "6.0", erz101) == first
    after = counts()
    assert after["protein_key"] == before["protein_key"]
    assert after["staging_protein"] == before["staging_protein"]
    assert after["staging_occurrence"] == 2 * before["staging_occurrence"]


def test_rerun_stages_nothing_and_returns_the_same_ids(
    connect_accession, erz101, caplog
):
    first = accession(connect_accession, "ERZ101", "6.0", erz101)
    before = counts()

    with caplog.at_level(logging.WARNING):
        assert accession(connect_accession, "ERZ101", "6.0", erz101) == first
    assert counts() == before
    assert "already accessioned" in caplog.text


def test_rerun_with_a_protein_without_mgyp_fails(connect_accession, erz101):
    accession(connect_accession, "ERZ101", "6.0", erz101)
    before = counts()

    new = (protein_hash("MNEWPROTEIN"), "MNEWPROTEIN")
    with pytest.raises(IncompleteRerun, match="1 of its proteins have no MGYP"):
        accession(
            connect_accession,
            "ERZ101",
            "6.0",
            erz101._replace(proteins=sorted([*erz101.proteins, new])),
        )
    assert counts() == before


def test_failure_before_commit_leaves_nothing_and_a_rerun_succeeds(
    connect_accession, erz101
):
    # The occurrences on the missing contig fail the transaction after its proteins are inserted.
    with pytest.raises(KeyError):
        accession(
            connect_accession,
            "ERZ101",
            "6.0",
            erz101._replace(contigs=erz101.contigs[1:]),
        )
    assert set(counts().values()) == {0}

    ids = accession(connect_accession, "ERZ101", "6.0", erz101)
    assert len(ids) == len(erz101.proteins)


@pytest.fixture
def stored(connect_accession):
    """The lowest and the highest hash for every first byte, stored with ids 1 to 512."""
    query("""
        INSERT INTO proteindb.protein_key (hash, id)
        SELECT set_byte(decode(repeat(tail, 32), 'hex'), 0, b), row_number() OVER ()
        FROM generate_series(0, 255) b, unnest(ARRAY['00', 'ff']) tail
        """)
    return dict(query("SELECT hash, id FROM proteindb.protein_key"))


def test_lookup_finds_stored_hashes_in_every_partition(connect_accession, stored):
    absent = [bytes([b]) + b"\x01" * 31 for b in range(0, 256, 25)]
    hashes = sorted([*stored, *absent])
    assert lookup(connect_accession, hashes, 1) == stored
    assert lookup(connect_accession, hashes, 16) == stored
    assert lookup(connect_accession, [], 16) == {}


def test_lookup_twice_on_one_connection_changes_no_table(connect_accession, stored):
    groups = [(p, [h for h in sorted(stored) if h[0] >> 2 == p]) for p in range(64)]
    before = counts()
    with connect_accession() as conn:
        assert lookup_partitions(conn, groups) == stored
        assert lookup_partitions(conn, groups[:3]) == {
            h: stored[h] for _, hs in groups[:3] for h in hs
        }
    assert counts() == before


def gene_caller_last_value():
    return query("SELECT last_value FROM proteindb.gene_caller_id_seq")[0][0]


def test_registering_known_gene_callers_takes_no_identity_value(connect_accession):
    callers = [("FragGeneScanRS", "1.1.0"), ("Pyrodigal", "3.6.3")]
    with connect_accession() as conn:
        ids = register_gene_callers(conn, callers)
        last_value = gene_caller_last_value()
        for _ in range(1000):
            assert register_gene_callers(conn, callers) == ids
    assert len(set(ids.values())) == 2
    assert gene_caller_last_value() == last_value


def test_known_gene_callers_need_no_identity_value(connect_accession, erz101):
    with connect_accession() as conn:
        register_gene_callers(
            conn, [("FragGeneScanRS", "1.1.0"), ("Pyrodigal", "3.6.3")]
        )
    query("ALTER TABLE proteindb.gene_caller ALTER COLUMN id RESTART WITH 32767")

    assert len(accession(connect_accession, "ERZ101", "6.0", erz101)) == len(
        erz101.proteins
    )


@pytest.fixture
def connect_limited(connect_accession, worker_id):
    """Connections as a role with proteindb_accession's privileges and a limit of one connection.

    Roles belong to the cluster, so each xdist worker gets its own.
    """
    role = f"proteindb_limited_{worker_id}"
    query(f"DROP ROLE IF EXISTS {role}")
    query(
        f"CREATE ROLE {role} LOGIN PASSWORD '{role}' CONNECTION LIMIT 1"
        " IN ROLE proteindb_accession"
    )
    # Role settings are not inherited.
    yield connector(
        make_conninfo(role_dsn(role), options="-c search_path=proteindb"), "test"
    )
    query(f"DROP ROLE {role}")


def test_job_waits_for_the_connection_limit(connect_limited, erz101, caplog):
    holder = connect_limited()
    threading.Timer(1.0, holder.close).start()

    ids = retry_on_connect_failure(
        lambda: accession(connect_limited, "ERZ101", "6.0", erz101, connections=1),
        first_delay=0.2,
    )
    assert len(ids) == len(erz101.proteins)
    assert "cannot connect" in caplog.text


def test_job_gives_up_on_the_connection_limit(connect_limited, erz101):
    with connect_limited():
        with pytest.raises(CannotConnect, match="too many connections"):
            retry_on_connect_failure(
                lambda: accession(
                    connect_limited, "ERZ101", "6.0", erz101, connections=1
                ),
                max_wait=0.5,
                first_delay=0.2,
            )
    assert counts()["assembly"] == 0


def test_errors_after_connecting_are_not_retried():
    calls = []

    def run():
        calls.append(1)
        raise psycopg.errors.QueryCanceled(
            "canceling statement due to statement timeout"
        )

    with pytest.raises(psycopg.errors.QueryCanceled):
        retry_on_connect_failure(run, first_delay=0.01)
    assert len(calls) == 1


def test_failing_to_connect_for_any_reason_is_retried(caplog):
    connect = connector("host=unreachable.invalid connect_timeout=1", "test")
    with pytest.raises(CannotConnect):
        retry_on_connect_failure(connect, max_wait=0.03, first_delay=0.01)
    assert caplog.text.count("cannot connect") == 2


@pytest.mark.parametrize("name", ["out.faa", "out.faa.gz"])
def test_output_has_the_mgyp_after_the_gene_id(tmp_path, erz101, name):
    ids = {hash: n for n, (hash, _) in enumerate(erz101.proteins, 1)}
    write_output(tmp_path / name, erz101.occurrences, ids)

    records = list(read_fasta(tmp_path / name))
    assert [r.id for r in records] == [o.gene_id for o in erz101.occurrences]
    assert [r.sequence for r in records] == [o.sequence for o in erz101.occurrences]
    for record, o in zip(records, erz101.occurrences):
        assert record.description == f"MGYP{ids[o.hash]:012d} {o.description}".rstrip()
    assert [p.name for p in tmp_path.iterdir()] == [name]


def test_output_header_without_description_has_no_trailing_space(tmp_path):
    occurrence = Occurrence(
        "g1", "", "MKV", protein_hash("MKV"), "c1", 1, 9, 1, "P", "1", None
    )
    write_output(tmp_path / "out.faa", [occurrence], {protein_hash("MKV"): 42})
    assert (tmp_path / "out.faa").read_text() == ">g1 MGYP000000000042\nMKV\n"


def test_output_is_the_same_when_written_again(tmp_path, erz101):
    ids = {hash: n for n, (hash, _) in enumerate(erz101.proteins, 1)}
    write_output(tmp_path / "a.faa.gz", erz101.occurrences, ids)
    write_output(tmp_path / "b.faa.gz", erz101.occurrences, ids)
    assert (tmp_path / "a.faa.gz").read_bytes() == (tmp_path / "b.faa.gz").read_bytes()
