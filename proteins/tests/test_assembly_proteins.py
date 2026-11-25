import logging
import re
import time

import psycopg
import pytest
from django.db import connections

from proteins.accessioning.assembly_proteins import (
    AnalysisAlreadyRecorded,
    IncompleteRerun,
    accession_assembly_proteins,
    lookup,
    register_assembly,
    register_gene_callers,
)
from proteins.accessioning.hashing import protein_hash
from proteins.accessioning.predicted_genes import InvalidInput, read_predicted_genes
from proteins.models import Protein
from proteins.routers import MGYP
from proteins.tests.conftest import (
    FIXTURES,
    PUBLISHED_V6,
    owner_dsn,
    read_published,
)

# The core runs as proteindb_accession, as in production.
pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("mgyp_as_accession"),
]


def query(sql, params=None):
    """Runs sql as the owner, who may read the identity sequences and alter the tables."""
    with psycopg.connect(owner_dsn(), autocommit=True) as conn:
        cursor = conn.execute(sql, params)
        return cursor.fetchall() if cursor.description else None


SOURCE = "assembly_analysis"


@pytest.fixture(scope="module")
def erz101():
    return read_predicted_genes(
        FIXTURES / "ERZ101.faa.gz",
        FIXTURES / "ERZ101.gff.gz",
        FIXTURES / "ERZ101.fasta.gz",
        FIXTURES / "ERZ101_mapping.csv",
    )


def counts():
    return {
        table: query(f"SELECT count(*) FROM proteindb.{table}")[0][0]
        for table in (
            "protein",
            "assembly_proteins",
            "staged_protein",
            "staged_contig",
            "staged_occurrence",
        )
    }


@pytest.mark.parametrize("assembly", ["ERZ101", *PUBLISHED_V6])
def test_new_assembly_is_staged_exactly(erz101, assembly):
    input = erz101 if assembly == "ERZ101" else read_published(assembly)
    ids = accession_assembly_proteins(assembly, SOURCE, "6.0", input, "MGYA1")

    assert set(ids) == {hash for hash, _ in input.proteins}
    assert len(set(ids.values())) == len(ids)
    assert dict(query("SELECT hash, id FROM proteindb.protein")) == ids
    ((record_id, *record),) = query(
        "SELECT p.id, a.accession, source, pipeline_version, analysis_accession"
        " FROM proteindb.assembly_proteins p JOIN proteindb.assembly a ON a.id = p.assembly_id"
    )
    assert record == [assembly, SOURCE, "6.0", "MGYA1"]
    assert set(
        query(
            "SELECT id, hash, sequence, assembly_proteins_id FROM proteindb.staged_protein"
        )
    ) == {(ids[hash], hash, sequence, record_id) for hash, sequence in input.proteins}
    assert set(
        query(
            "SELECT name, original_name, length, hash, kmer_coverage"
            " FROM proteindb.staged_contig WHERE assembly_proteins_id = %s",
            [record_id],
        )
    ) == set(input.contigs)
    assert (
        sorted(
            query(
                """
            SELECT o.gene_id, o.protein_id, c.name, g.name, g.version,
                   o.start_position, o.end_position, o.strand, o.truncation
            FROM proteindb.staged_occurrence o
            JOIN proteindb.staged_contig c ON c.id = o.contig_id
            JOIN proteindb.gene_caller g ON g.id = o.gene_caller_id
            WHERE o.assembly_proteins_id = %s
            """,
                [record_id],
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
            for o in input.occurrences
        )
    )


def test_assembly_with_only_known_proteins_adds_none(erz101):
    first = accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    assert (
        accession_assembly_proteins("ERZ102", SOURCE, "6.0", erz101, "MGYA2") == first
    )
    after = counts()
    assert after["protein"] == before["protein"]
    assert after["staged_protein"] == before["staged_protein"]
    assert after["staged_occurrence"] == 2 * before["staged_occurrence"]


def test_rerun_stages_nothing_and_returns_the_same_ids(erz101, caplog):
    first = accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    with caplog.at_level(logging.WARNING):
        assert (
            accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
            == first
        )
    assert counts() == before
    assert "nothing was staged" in caplog.text


def test_rerun_with_a_protein_without_mgyp_fails(erz101):
    accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    new = (protein_hash("MNEWPROTEIN"), "MNEWPROTEIN")
    with pytest.raises(IncompleteRerun, match="1 of its proteins have no MGYP"):
        accession_assembly_proteins(
            "ERZ101",
            SOURCE,
            "6.0",
            erz101._replace(proteins=sorted([*erz101.proteins, new])),
            "MGYA1",
        )
    assert counts() == before


def test_an_mgya_recorded_under_another_version_fails_and_names_its_record(erz101):
    accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    with pytest.raises(
        AnalysisAlreadyRecorded,
        match="MGYA1 already has the assembly proteins record ERZ101 assembly_analysis 6.0",
    ):
        accession_assembly_proteins("ERZ101", SOURCE, "6.1", erz101, "MGYA1")
    assert counts() == before


def test_failure_before_commit_leaves_nothing_and_a_rerun_succeeds(erz101):
    # The occurrences on the missing contig fail the transaction after its proteins are inserted.
    with pytest.raises(KeyError):
        accession_assembly_proteins(
            "ERZ101",
            SOURCE,
            "6.0",
            erz101._replace(contigs=erz101.contigs[1:]),
            "MGYA1",
        )
    assert set(counts().values()) == {0}

    ids = accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    assert len(ids) == len(erz101.proteins)


def test_patch_version_is_recorded_as_major_minor(erz101):
    accession_assembly_proteins("ERZ101", SOURCE, "6.1.2", erz101, "MGYA1")
    assert query("SELECT pipeline_version FROM proteindb.assembly_proteins") == [
        ("6.1",)
    ]


@pytest.mark.parametrize("version", ["6.10", "10.0"])
def test_invalid_version_fails_before_connecting(
    erz101, version, django_assert_num_queries
):
    with django_assert_num_queries(0, connection=connections[MGYP]):
        with pytest.raises(InvalidInput, match="one digit each"):
            accession_assembly_proteins("ERZ101", SOURCE, version, erz101, "MGYA1")


@pytest.fixture
def stored():
    """The lowest and the highest hash for every first byte, stored with ids 1 to 512."""
    query("""
        INSERT INTO proteindb.protein (hash, id)
        SELECT set_byte(decode(repeat(tail, 32), 'hex'), 0, b), row_number() OVER ()
        FROM generate_series(0, 255) b, unnest(ARRAY['00', 'ff']) tail
        """)
    return dict(query("SELECT hash, id FROM proteindb.protein"))


def test_lookup_finds_stored_hashes_in_every_partition(stored):
    absent = [bytes([b]) + b"\x01" * 31 for b in range(0, 256, 25)]
    hashes = sorted([*stored, *absent])
    assert lookup(hashes, 1) == stored
    assert lookup(hashes, 16) == stored
    assert lookup([], 16) == {}


def test_lookup_threads_close_their_connections(stored):
    def open_connections():
        return query(
            "SELECT count(*) FROM pg_stat_activity"
            " WHERE datname = current_database() AND usename = 'proteindb_accession'"
        )[0][0]

    before = open_connections()
    lookup(sorted(stored), 16)
    # A backend leaves pg_stat_activity shortly after its client disconnects.
    deadline = time.monotonic() + 10
    while open_connections() != before and time.monotonic() < deadline:
        time.sleep(0.05)
    assert open_connections() == before


@pytest.mark.parametrize("partition", [0, 17, 63])
def test_lookup_of_one_partition_reads_only_that_partition(partition):
    hashes = [bytes([4 * partition + n % 4]) + bytes([n]) * 31 for n in range(100)]
    plan = Protein.objects.filter(hash__in=hashes).explain()
    assert set(re.findall(r"protein_[0-9a-f]{2}", plan)) == {f"protein_{partition:02x}"}


def gene_caller_last_value():
    return query("SELECT last_value FROM proteindb.gene_caller_id_seq")[0][0]


def test_registering_known_gene_callers_takes_no_identity_value():
    callers = [("FragGeneScanRS", "1.1.0"), ("Pyrodigal", "3.6.3")]
    ids = register_gene_callers(callers)
    last_value = gene_caller_last_value()
    for _ in range(1000):
        assert register_gene_callers(callers) == ids
    assert len(set(ids.values())) == 2
    assert gene_caller_last_value() == last_value


def test_registering_a_known_assembly_takes_no_identity_value():
    id = register_assembly("ERZ1")
    last_value = query("SELECT last_value FROM proteindb.assembly_id_seq")[0][0]
    for _ in range(1000):
        assert register_assembly("ERZ1") == id
    assert query("SELECT last_value FROM proteindb.assembly_id_seq")[0][0] == last_value


def test_two_versions_of_an_assembly_share_its_id(erz101):
    accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    accession_assembly_proteins("ERZ101", SOURCE, "6.1", erz101, "MGYA2")
    assert query(
        "SELECT a.accession, count(*) FROM proteindb.assembly_proteins p"
        " JOIN proteindb.assembly a ON a.id = p.assembly_id GROUP BY a.accession"
    ) == [("ERZ101", 2)]


def test_known_gene_callers_need_no_identity_value(erz101):
    register_gene_callers([("FragGeneScanRS", "1.1.0"), ("Pyrodigal", "3.6.3")])
    query("ALTER TABLE proteindb.gene_caller ALTER COLUMN id RESTART WITH 32767")

    ids = accession_assembly_proteins("ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    assert len(ids) == len(erz101.proteins)
