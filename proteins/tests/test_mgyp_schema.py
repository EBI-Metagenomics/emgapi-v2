import pytest
from django.apps import apps
from django.db import IntegrityError

from proteins.models import Protein
from proteins.routers import MGYP
from proteins.tests.conftest import query

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP]),
    pytest.mark.usefixtures("mgyp_db"),
]


def test_creates_every_table_and_sequence():
    tables = {
        name
        for (name,) in query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'proteindb'"
        )
    }
    assert tables == {
        "protein",
        *(f"protein_{p:02x}" for p in range(64)),
        "assembly_proteins",
        "gene_caller",
        "staged_protein",
        "staged_contig",
        "staged_occurrence",
        "dataset_update",
        "dataset_owner",
    }
    sequences = {
        name
        for (name,) in query(
            "SELECT sequencename FROM pg_sequences WHERE schemaname = 'proteindb'"
        )
    }
    assert {"protein_id_seq", "contig_id_seq", "occurrence_id_seq"} <= sequences


def test_the_models_match_the_tables():
    for model in apps.get_app_config("proteins").get_models():
        columns = dict(
            query(
                "SELECT column_name, is_nullable = 'YES' FROM information_schema.columns"
                " WHERE table_schema = 'proteindb' AND table_name = %s",
                [model._meta.db_table],
            )
        )
        fields = {field.column: field.null for field in model._meta.concrete_fields}
        assert fields == columns, model.__name__


def test_find_mgyps_gives_the_stored_mgyp_of_known_sequences_only():
    query("INSERT INTO proteindb.protein VALUES (%s, 42)", [Protein.hash_of("MKV")])
    query("SET LOCAL search_path = proteindb")
    assert Protein.objects.find_mgyps(["MKV", "MAA"]) == {"MKV": "MGYP000000000042"}


def test_protein_partitions_are_vacuumed_every_100000_inserts():
    options = query("""
        SELECT c.relname, c.reloptions FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'proteindb.protein'::regclass
        """)
    assert len(options) == 64
    for _, reloptions in options:
        assert sorted(reloptions) == [
            "autovacuum_vacuum_insert_scale_factor=0",
            "autovacuum_vacuum_insert_threshold=100000",
        ]


def test_every_first_byte_routes_to_partition_byte_shifted_by_2():
    # The lowest and the highest hash for each first byte.
    query("""
        INSERT INTO proteindb.protein (hash, id)
        SELECT set_byte(decode(repeat(tail, 32), 'hex'), 0, b), row_number() OVER ()
        FROM generate_series(0, 255) b, unnest(ARRAY['00', 'ff']) tail
        """)
    rows = query("""
        SELECT get_byte(p.hash, 0), c.relname FROM proteindb.protein p
        JOIN pg_class c ON c.oid = p.tableoid
        """)
    assert len(rows) == 512
    assert all(partition == f"protein_{b >> 2:02x}" for b, partition in rows)


# Its first byte, 0xab, routes it to partition 0x2a.
HASH = "'\\x" + "ab" * 32 + "'::bytea"


def record(erz="ERZ1", source="assembly_analysis", version="6.1", mgya="MGYA1"):
    return (
        "INSERT INTO proteindb.assembly_proteins"
        " (assembly_accession, source, pipeline_version, analysis_accession)"
        f" VALUES ('{erz}', '{source}', '{version}', '{mgya}')"
    )


@pytest.mark.parametrize(
    "setup, statement, constraint",
    [
        (
            None,
            "INSERT INTO proteindb.protein VALUES ('\\x" + "ab" * 31 + "', 1)",
            "protein_hash_check",
        ),
        (
            None,
            f"INSERT INTO proteindb.protein VALUES ({HASH}, 0)",
            "protein_id_check",
        ),
        (
            None,
            f"INSERT INTO proteindb.protein VALUES ({HASH}, 1000000000000)",
            "protein_id_check",
        ),
        (
            f"INSERT INTO proteindb.protein VALUES ({HASH}, 1)",
            f"INSERT INTO proteindb.protein VALUES ({HASH}, 2)",
            "protein_2a_pkey",
        ),
        *(
            (None, record(version=v), "assembly_proteins_pipeline_version_check")
            for v in ("6.10", "10.0", "6", "6.1.2")
        ),
        (None, record(source="other"), "assembly_proteins_source_check"),
        (
            record(),
            record(mgya="MGYA2"),
            "assembly_proteins_assembly_accession_source_pipeline_versio_key",
        ),
        (
            record(),
            record(erz="ERZ2"),
            "assembly_proteins_analysis_accession_key",
        ),
        (
            None,
            "INSERT INTO proteindb.staged_occurrence (assembly_proteins_id, gene_id, protein_id,"
            " contig_id, gene_caller_id, start_position, end_position, strand)"
            " VALUES (1, 'g', 1, 1, 1, 1, 9, 0)",
            "staged_occurrence_strand_check",
        ),
        (
            None,
            "INSERT INTO proteindb.staged_occurrence (assembly_proteins_id, gene_id, protein_id,"
            " contig_id, gene_caller_id, start_position, end_position, strand, truncation)"
            " VALUES (1, 'g', 1, 1, 1, 1, 9, 1, '2')",
            "staged_occurrence_truncation_check",
        ),
        (
            None,
            "INSERT INTO proteindb.dataset_update (day, status) VALUES ('2026-10-01', 'other')",
            "dataset_update_status_check",
        ),
        (
            "INSERT INTO proteindb.dataset_update (day, status) VALUES ('2026-10-01', 'done')",
            "INSERT INTO proteindb.dataset_update (day, status) VALUES ('2026-10-01', 'done')",
            "dataset_update_one_done_per_day",
        ),
        (
            None,
            "INSERT INTO proteindb.dataset_owner DEFAULT VALUES",
            "dataset_owner_pkey",
        ),
        (
            None,
            "INSERT INTO proteindb.dataset_owner (id) VALUES (false)",
            "dataset_owner_id_check",
        ),
        (
            None,
            "UPDATE proteindb.dataset_owner SET flow = 'other'",
            "dataset_owner_flow_check",
        ),
    ],
)
def test_constraint_rejects_invalid_rows(setup, statement, constraint):
    if setup:
        query(setup)
    with pytest.raises(IntegrityError) as excinfo:
        query(statement)
    assert excinfo.value.__cause__.diag.constraint_name == constraint


def test_one_assembly_and_version_can_have_a_record_of_each_source():
    query(record())
    query(record(source="gene_calling", mgya="MGYA2"))


def test_dataset_update_allows_failed_attempts_before_the_done_one():
    query("""
        INSERT INTO proteindb.dataset_update (day, status) VALUES
            ('2026-10-01', 'failed'), ('2026-10-01', 'failed'), ('2026-10-01', 'done')
        """)
