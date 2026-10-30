import logging

import pytest

from proteins.accession.accession import (
    IncompleteRerun,
    accession,
    lookup,
    lookup_partitions,
    register_gene_callers,
)
from proteins.accession.contract import protein_hash
from proteins.accession.inputs import InvalidInput, read_input
from proteins.routers import MGYP
from proteins.tests.conftest import (
    FIXTURES,
    PUBLISHED_V6,
    query,
    read_assembly,
    read_published,
    write_assembly,
)

pytestmark = pytest.mark.django_db(databases=["default", MGYP], transaction=True)

SOURCE = "assembly_analysis"


@pytest.fixture(scope="module")
def erz101():
    return read_input(
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
def test_new_assembly_is_staged_exactly(connect_accession, erz101, assembly):
    input = erz101 if assembly == "ERZ101" else read_published(assembly)
    ids = accession(connect_accession, assembly, SOURCE, "6.0", input, "MGYA1")

    assert set(ids) == {hash for hash, _ in input.proteins}
    assert len(set(ids.values())) == len(ids)
    assert dict(query("SELECT hash, id FROM proteindb.protein")) == ids
    ((record_id, *record),) = query(
        "SELECT id, assembly_accession, source, pipeline_version, analysis_accession"
        " FROM proteindb.assembly_proteins"
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


def test_lower_case_input_is_staged_upper_cased(connect_accession, tmp_path):
    input = read_assembly(write_assembly(tmp_path / "ERZ1", ["mkv"]))
    accession(connect_accession, "ERZ1", SOURCE, "6.0", input, "MGYA1")
    assert query("SELECT sequence FROM proteindb.staged_protein") == [("MKV",)]


def test_assembly_with_only_known_proteins_adds_none(connect_accession, erz101):
    first = accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    assert (
        accession(connect_accession, "ERZ102", SOURCE, "6.0", erz101, "MGYA2") == first
    )
    after = counts()
    assert after["protein"] == before["protein"]
    assert after["staged_protein"] == before["staged_protein"]
    assert after["staged_occurrence"] == 2 * before["staged_occurrence"]


def test_rerun_stages_nothing_and_returns_the_same_ids(
    connect_accession, erz101, caplog
):
    first = accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    with caplog.at_level(logging.WARNING):
        assert (
            accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
            == first
        )
    assert counts() == before
    assert "nothing was staged" in caplog.text


def test_rerun_with_a_protein_without_mgyp_fails(connect_accession, erz101):
    accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    before = counts()

    new = (protein_hash("MNEWPROTEIN"), "MNEWPROTEIN")
    with pytest.raises(IncompleteRerun, match="1 of its proteins have no MGYP"):
        accession(
            connect_accession,
            "ERZ101",
            SOURCE,
            "6.0",
            erz101._replace(proteins=sorted([*erz101.proteins, new])),
            "MGYA1",
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
            SOURCE,
            "6.0",
            erz101._replace(contigs=erz101.contigs[1:]),
            "MGYA1",
        )
    assert set(counts().values()) == {0}

    ids = accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    assert len(ids) == len(erz101.proteins)


def test_patch_version_is_recorded_as_major_minor(connect_accession, erz101):
    accession(connect_accession, "ERZ101", SOURCE, "6.1.2", erz101, "MGYA1")
    assert query("SELECT pipeline_version FROM proteindb.assembly_proteins") == [
        ("6.1",)
    ]


def refuse_to_connect():
    raise AssertionError("connected")


@pytest.mark.parametrize("version", ["6.10", "10.0"])
def test_invalid_version_fails_before_connecting(erz101, version):
    with pytest.raises(InvalidInput, match="one digit each"):
        accession(refuse_to_connect, "ERZ101", SOURCE, version, erz101, "MGYA1")


@pytest.fixture
def stored(connect_accession):
    """The lowest and the highest hash for every first byte, stored with ids 1 to 512."""
    query("""
        INSERT INTO proteindb.protein (hash, id)
        SELECT set_byte(decode(repeat(tail, 32), 'hex'), 0, b), row_number() OVER ()
        FROM generate_series(0, 255) b, unnest(ARRAY['00', 'ff']) tail
        """)
    return dict(query("SELECT hash, id FROM proteindb.protein"))


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

    ids = accession(connect_accession, "ERZ101", SOURCE, "6.0", erz101, "MGYA1")
    assert len(ids) == len(erz101.proteins)
