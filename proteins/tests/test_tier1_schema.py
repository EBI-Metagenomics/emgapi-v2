import pytest
from django.db import IntegrityError, connections

pytestmark = [
    pytest.mark.django_db(databases=["default", "proteindb"]),
    pytest.mark.usefixtures("tier1"),
]


def query(sql):
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else None


def test_creates_every_table_and_sequence():
    tables = {
        name
        for (name,) in query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'proteindb'"
        )
    }
    assert tables == {
        "protein_key",
        *(f"protein_key_{p:02x}" for p in range(64)),
        "assembly",
        "study",
        "biome",
        "gene_caller",
        "staging_protein",
        "staging_contig",
        "staging_occurrence",
        "load_log",
        "tier2_owner",
    }
    sequences = {
        name
        for (name,) in query(
            "SELECT sequencename FROM pg_sequences WHERE schemaname = 'proteindb'"
        )
    }
    assert {"protein_id_seq", "contig_id_seq", "occurrence_id_seq"} <= sequences


def test_protein_key_partitions_are_vacuumed_every_100000_inserts():
    options = query("""
        SELECT c.relname, c.reloptions FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'proteindb.protein_key'::regclass
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
        INSERT INTO proteindb.protein_key (hash, id)
        SELECT set_byte(decode(repeat(tail, 32), 'hex'), 0, b), row_number() OVER ()
        FROM generate_series(0, 255) b, unnest(ARRAY['00', 'ff']) tail
        """)
    rows = query("""
        SELECT get_byte(k.hash, 0), c.relname FROM proteindb.protein_key k
        JOIN pg_class c ON c.oid = k.tableoid
        """)
    assert len(rows) == 512
    assert all(partition == f"protein_key_{b >> 2:02x}" for b, partition in rows)


def test_tier2_owner_starts_with_one_empty_row():
    assert query("SELECT id, flow, execution_id FROM proteindb.tier2_owner") == [
        (True, None, None)
    ]


# Its first byte, 0xab, routes it to partition 0x2a.
HASH = "'\\x" + "ab" * 32 + "'::bytea"


@pytest.mark.parametrize(
    "setup, statement, constraint",
    [
        (
            None,
            "INSERT INTO proteindb.protein_key VALUES ('\\x" + "ab" * 31 + "', 1)",
            "protein_key_hash_check",
        ),
        (
            None,
            f"INSERT INTO proteindb.protein_key VALUES ({HASH}, 0)",
            "protein_key_id_check",
        ),
        (
            None,
            f"INSERT INTO proteindb.protein_key VALUES ({HASH}, 1000000000000)",
            "protein_key_id_check",
        ),
        (
            f"INSERT INTO proteindb.protein_key VALUES ({HASH}, 1)",
            f"INSERT INTO proteindb.protein_key VALUES ({HASH}, 2)",
            "protein_key_2a_pkey",
        ),
        *(
            (
                None,
                f"INSERT INTO proteindb.assembly (accession, pipeline_version) VALUES ('ERZ1', '{v}')",
                "assembly_pipeline_version_check",
            )
            for v in ("6.10", "10.0", "6", "6.1.2")
        ),
        (
            "INSERT INTO proteindb.assembly (accession, pipeline_version) VALUES ('ERZ1', '6.1')",
            "INSERT INTO proteindb.assembly (accession, pipeline_version) VALUES ('ERZ1', '6.1')",
            "assembly_accession_pipeline_version_key",
        ),
        (
            None,
            "INSERT INTO proteindb.staging_occurrence (assembly_id, gene_id, protein_id, contig_id,"
            " gene_caller_id, start_position, end_position, strand) VALUES (1, 'g', 1, 1, 1, 1, 9, 0)",
            "staging_occurrence_strand_check",
        ),
        (
            None,
            "INSERT INTO proteindb.staging_occurrence (assembly_id, gene_id, protein_id, contig_id,"
            " gene_caller_id, start_position, end_position, strand, truncation)"
            " VALUES (1, 'g', 1, 1, 1, 1, 9, 1, '2')",
            "staging_occurrence_truncation_check",
        ),
        (
            "INSERT INTO proteindb.load_log (ingest_date, status) VALUES ('2026-10-01', 'done')",
            "INSERT INTO proteindb.load_log (ingest_date, status) VALUES ('2026-10-01', 'done')",
            "load_log_one_done_per_day",
        ),
        (
            None,
            "INSERT INTO proteindb.tier2_owner (id) VALUES (false)",
            "tier2_owner_id_check",
        ),
        (
            None,
            "INSERT INTO proteindb.tier2_owner DEFAULT VALUES",
            "tier2_owner_pkey",
        ),
    ],
)
def test_constraint_rejects_invalid_rows(setup, statement, constraint):
    if setup:
        query(setup)
    with pytest.raises(IntegrityError) as excinfo:
        query(statement)
    assert excinfo.value.__cause__.diag.constraint_name == constraint


def test_load_log_allows_failed_attempts_before_the_done_one():
    query("""
        INSERT INTO proteindb.load_log (ingest_date, status) VALUES
            ('2026-10-01', 'failed'), ('2026-10-01', 'failed'), ('2026-10-01', 'done')
        """)
