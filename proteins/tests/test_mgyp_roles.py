import pytest
from django.urls import reverse
from psycopg.errors import InsufficientPrivilege

from proteins.models import Protein
from proteins.routers import MGYP
from proteins.tests.conftest import ROLES, query

pytestmark = pytest.mark.django_db(databases=["default", MGYP])

HASH = "'\\x" + "ab" * 32 + "'::bytea"


@pytest.mark.parametrize("role", ROLES)
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


def test_accession_role_can_accession_a_new_analysis(connect_as):
    conn = connect_as("proteindb_accession")
    conn.execute("CREATE TEMP TABLE q (hash bytea)")
    conn.execute(f"INSERT INTO q VALUES ({HASH})")
    assert (
        conn.execute(
            "SELECT p.id FROM q JOIN proteindb.protein_2a p USING (hash)"
        ).fetchall()
        == []
    )

    (gene_caller_id,) = conn.execute(
        "INSERT INTO gene_caller (name, version) VALUES ('Pyrodigal', '3.6.3')"
        " ON CONFLICT DO NOTHING RETURNING id"
    ).fetchone()
    (assembly_proteins_id,) = conn.execute(
        "INSERT INTO assembly_proteins (assembly_accession, source, pipeline_version, analysis_accession)"
        " VALUES ('ERZ101', 'assembly_analysis', '6.0', 'MGYA101')"
        " ON CONFLICT DO NOTHING RETURNING id"
    ).fetchone()
    conn.execute(
        "CREATE TEMP TABLE new_hash (hash bytea, sequence text) ON COMMIT DROP"
    )
    conn.execute(f"INSERT INTO new_hash VALUES ({HASH}, 'MKV')")
    conn.execute(
        """
        WITH ins AS (
            INSERT INTO protein (hash, id)
            SELECT hash, nextval('protein_id_seq') FROM new_hash ORDER BY hash
            ON CONFLICT (hash) DO NOTHING
            RETURNING hash, id
        )
        INSERT INTO staged_protein (id, hash, sequence, assembly_proteins_id)
        SELECT ins.id, ins.hash, n.sequence, %s FROM ins JOIN new_hash n USING (hash)
        """,
        [assembly_proteins_id],
    )
    (protein_id,) = conn.execute(
        "SELECT p.id FROM new_hash n JOIN protein p USING (hash)"
    ).fetchone()
    (contig_id,) = conn.execute(
        "INSERT INTO staged_contig (assembly_proteins_id, name, length, hash)"
        " VALUES (%s, 'ERZ101_1', 9, '\\x00') RETURNING id",
        [assembly_proteins_id],
    ).fetchone()
    conn.execute(
        "INSERT INTO staged_occurrence (assembly_proteins_id, gene_id, protein_id, contig_id,"
        " gene_caller_id, start_position, end_position, strand, truncation)"
        " VALUES (%s, 'ERZ101_1_1', %s, %s, %s, 1, 9, 1, '00')",
        [assembly_proteins_id, protein_id, contig_id, gene_caller_id],
    )


def test_accession_role_can_update_the_protein_dataset(connect_as):
    conn = connect_as("proteindb_accession")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute(
            "SELECT assembly_proteins_id FROM staged_protein"
            " UNION SELECT assembly_proteins_id FROM staged_contig"
            " UNION SELECT assembly_proteins_id FROM staged_occurrence"
        )
        conn.execute("SELECT * FROM assembly_proteins JOIN gene_caller ON true")
        with conn.cursor(name="read_staged_protein") as rows:
            rows.execute("SELECT id, hash, sequence FROM staged_protein")
            rows.fetchall()
    conn.execute("SELECT last_value FROM protein_id_seq")
    conn.execute("SELECT count(*), min(staged_at) FROM staged_occurrence")
    conn.execute("UPDATE dataset_update SET status = 'failed' WHERE status = 'running'")
    (update_id,) = conn.execute(
        "INSERT INTO dataset_update (day, status) VALUES ('2026-10-01', 'running')"
        " RETURNING id"
    ).fetchone()
    for table in ("staged_occurrence", "staged_contig", "staged_protein"):
        conn.execute(f"DELETE FROM {table} WHERE assembly_proteins_id = ANY(%s)", [[1]])
    conn.execute(
        "UPDATE dataset_update SET status = 'done', finished_at = now() WHERE id = %s",
        [update_id],
    )


@pytest.fixture
def as_accession_role(mgyp_db):
    """Django's mgyp connection as proteindb_accession, until the test's transaction ends."""
    query("SET LOCAL ROLE proteindb_accession")
    query("SET LOCAL search_path = proteindb")


@pytest.mark.usefixtures("as_accession_role")
def test_accession_role_can_find_mgyps():
    assert query("SELECT current_user") == [("proteindb_accession",)]
    assert Protein.objects.find_mgyps(["MKV"]) == {}


@pytest.mark.usefixtures("as_accession_role")
@pytest.mark.parametrize("model", ["assemblyproteins", "datasetupdate"])
def test_accession_role_can_show_the_admin(admin_client, model):
    response = admin_client.get(reverse(f"admin:proteins_{model}_changelist"))
    assert response.status_code == 200


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
    conn.execute(f"SELECT id FROM proteindb.protein_2a WHERE hash = {HASH}")


@pytest.mark.parametrize(
    "role, statement",
    [
        ("proteindb_accession", "DELETE FROM protein"),
        ("proteindb_accession", "UPDATE protein SET id = id"),
        ("proteindb_accession", "DELETE FROM proteindb.protein_2a"),
        ("proteindb_accession", "TRUNCATE staged_protein"),
        ("proteindb_accession", "UPDATE staged_contig SET name = name"),
        ("proteindb_accession", "UPDATE assembly_proteins SET source = source"),
        ("proteindb_accession", "DELETE FROM assembly_proteins"),
        ("proteindb_accession", "DELETE FROM gene_caller"),
        ("proteindb_accession", "DELETE FROM dataset_update"),
        ("proteindb_load", "SELECT * FROM protein"),
        ("proteindb_load", "SELECT * FROM proteindb.protein_2a"),
        ("proteindb_load", "SELECT * FROM staged_protein"),
        ("proteindb_load", "SELECT * FROM dataset_update"),
        ("proteindb_load", "SELECT nextval('protein_id_seq')"),
        ("proteindb_read", f"INSERT INTO protein VALUES ({HASH}, 1)"),
        ("proteindb_read", "DELETE FROM staged_protein"),
        ("proteindb_read", "UPDATE dataset_update SET message = NULL"),
        ("proteindb_read", "SELECT nextval('protein_id_seq')"),
    ],
)
def test_role_is_refused(connect_as, role, statement):
    with pytest.raises(InsufficientPrivilege):
        connect_as(role).execute(statement)
