import pytest
from psycopg.errors import InsufficientPrivilege

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"])

HASH = "'\\x" + "ab" * 32 + "'::bytea"


@pytest.mark.parametrize(
    "role", ["proteindb_accession", "proteindb_load", "proteindb_read"]
)
def test_role_finds_the_schema_at_login(connect_as, role):
    assert connect_as(role).execute("SHOW search_path").fetchone() == ("proteindb",)


def test_accession_role_settings(connect_as):
    conn = connect_as("proteindb_accession")
    settings = {
        name: conn.execute(f"SHOW {name}").fetchone()[0]
        for name in (
            "idle_in_transaction_session_timeout",
            "statement_timeout",
            "work_mem",
            "temp_buffers",
        )
    }
    assert settings == {
        "idle_in_transaction_session_timeout": "10min",
        "statement_timeout": "1h",
        "work_mem": "256MB",
        "temp_buffers": "512MB",
    }
    assert conn.execute(
        "SELECT rolconnlimit FROM pg_roles WHERE rolname = current_user"
    ).fetchone() == (256,)


def test_load_role_settings(connect_as):
    assert connect_as("proteindb_load").execute("SHOW work_mem").fetchone() == ("1GB",)


def test_accession_role_can_accession_a_new_analysis(connect_as):
    conn = connect_as("proteindb_accession")
    conn.execute("CREATE TEMP TABLE q (hash bytea)")
    conn.execute(f"INSERT INTO q VALUES ({HASH})")
    assert (
        conn.execute(
            "SELECT k.id FROM q JOIN proteindb.protein_key_2a k USING (hash)"
        ).fetchall()
        == []
    )

    (gene_caller_id,) = conn.execute(
        "INSERT INTO gene_caller (name, version) VALUES ('Pyrodigal', '3.6.3')"
        " ON CONFLICT DO NOTHING RETURNING id"
    ).fetchone()
    (assembly_id,) = conn.execute(
        "INSERT INTO assembly (accession, pipeline_version) VALUES ('ERZ101', '6.0')"
        " ON CONFLICT DO NOTHING RETURNING id"
    ).fetchone()
    conn.execute(
        "CREATE TEMP TABLE new_hash (hash bytea, sequence text) ON COMMIT DROP"
    )
    conn.execute(f"INSERT INTO new_hash VALUES ({HASH}, 'MKV')")
    conn.execute(
        """
        WITH ins AS (
            INSERT INTO protein_key (hash, id)
            SELECT hash, nextval('protein_id_seq') FROM new_hash ORDER BY hash
            ON CONFLICT (hash) DO NOTHING
            RETURNING hash, id
        )
        INSERT INTO staging_protein (id, hash, sequence, assembly_id)
        SELECT ins.id, ins.hash, n.sequence, %s FROM ins JOIN new_hash n USING (hash)
        """,
        [assembly_id],
    )
    (protein_id,) = conn.execute(
        "SELECT k.id FROM new_hash n JOIN protein_key k USING (hash)"
    ).fetchone()
    conn.execute(
        "INSERT INTO staging_contig (assembly_id, name, length, hash)"
        " VALUES (%s, 'ERZ101_1', 9, '\\x00')",
        [assembly_id],
    )
    (contig_id,) = conn.execute(
        "SELECT id FROM staging_contig WHERE assembly_id = %s", [assembly_id]
    ).fetchone()
    conn.execute(
        "INSERT INTO staging_occurrence (assembly_id, gene_id, protein_id, contig_id,"
        " gene_caller_id, start_position, end_position, strand, truncation)"
        " VALUES (%s, 'ERZ101_1_1', %s, %s, %s, 1, 9, 1, '00')",
        [assembly_id, protein_id, contig_id, gene_caller_id],
    )


def test_load_role_can_load_a_day(connect_as):
    conn = connect_as("proteindb_load")
    for table in ("staging_protein", "staging_contig", "staging_occurrence"):
        conn.execute(f"SELECT * FROM {table}")
    conn.execute("SELECT * FROM assembly JOIN gene_caller ON true")
    conn.execute("SELECT last_value FROM protein_id_seq")
    conn.execute("INSERT INTO study (accession) VALUES ('ERP1')")
    conn.execute("INSERT INTO biome (lineage) VALUES ('root:Environmental')")
    (load_id,) = conn.execute(
        "INSERT INTO load_log (ingest_date, status) VALUES ('2026-10-01', 'running')"
        " RETURNING id"
    ).fetchone()
    conn.execute(
        "UPDATE tier2_owner SET flow = 'load', execution_id = gen_random_uuid()"
    )
    for table in ("staging_occurrence", "staging_contig", "staging_protein"):
        conn.execute(f"DELETE FROM {table}")
    conn.execute(
        "UPDATE load_log SET status = 'done', finished_at = now() WHERE id = %s",
        [load_id],
    )
    conn.execute("UPDATE tier2_owner SET flow = NULL, execution_id = NULL")


def test_read_role_can_read_everything(connect_as):
    conn = connect_as("proteindb_read")
    tables = [
        name
        for (name,) in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'proteindb'"
        )
    ]
    for table in tables:
        conn.execute(f"SELECT * FROM {table} LIMIT 1")
    for sequence in ("protein_id_seq", "contig_id_seq", "occurrence_id_seq"):
        conn.execute(f"SELECT last_value FROM {sequence}")


@pytest.mark.parametrize(
    "role, statement",
    [
        ("proteindb_accession", "DELETE FROM protein_key"),
        ("proteindb_accession", "UPDATE protein_key SET id = id"),
        ("proteindb_accession", "DELETE FROM proteindb.protein_key_2a"),
        ("proteindb_accession", "SELECT * FROM staging_protein"),
        ("proteindb_accession", "DELETE FROM staging_contig"),
        ("proteindb_accession", "UPDATE assembly SET accession = accession"),
        ("proteindb_accession", "SELECT * FROM load_log"),
        ("proteindb_accession", "UPDATE tier2_owner SET flow = NULL"),
        ("proteindb_accession", "INSERT INTO study (accession) VALUES ('ERP1')"),
        ("proteindb_load", "SELECT * FROM protein_key"),
        ("proteindb_load", "SELECT * FROM proteindb.protein_key_2a"),
        ("proteindb_load", f"INSERT INTO protein_key VALUES ({HASH}, 1)"),
        (
            "proteindb_load",
            "INSERT INTO assembly (accession, pipeline_version) VALUES ('ERZ1', '6.0')",
        ),
        (
            "proteindb_load",
            "INSERT INTO staging_contig (assembly_id, name, length, hash) VALUES (1, 'c', 1, '')",
        ),
        ("proteindb_load", "DELETE FROM load_log"),
        ("proteindb_load", "DELETE FROM study"),
        ("proteindb_load", "SELECT nextval('protein_id_seq')"),
        ("proteindb_read", f"INSERT INTO protein_key VALUES ({HASH}, 1)"),
        ("proteindb_read", "INSERT INTO study (accession) VALUES ('ERP1')"),
        ("proteindb_read", "UPDATE tier2_owner SET flow = NULL"),
        ("proteindb_read", "DELETE FROM staging_protein"),
        ("proteindb_read", "SELECT nextval('protein_id_seq')"),
    ],
)
def test_role_is_refused(connect_as, role, statement):
    with pytest.raises(InsufficientPrivilege):
        connect_as(role).execute(statement)
