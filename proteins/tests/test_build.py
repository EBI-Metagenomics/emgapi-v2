import hashlib
from datetime import date

import pyarrow as pa
import pytest
from django.core.management import call_command

from proteins.dataset import layout, update
from proteins.dataset.build import BuildError, build_mgyp_database
from proteins.routers import MGYP
from proteins.tests.conftest import owner_dsn, query

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("emptied_mgyp_db"),
]

M, D = date(2026, 11, 2), date(2026, 11, 3)


def hash_of(first_byte, n):
    return bytes([first_byte]) + hashlib.sha256(str(n).encode()).digest()[1:]


# Both ends of the first and last partitions, and of one in between.
MIGRATED = {
    hash_of(b, b): id
    for b, id in [(0x00, 3), (0x03, 10), (0x04, 11), (0x7F, 25), (0xFC, 40), (0xFF, 41)]
}
RECORDS = [
    {
        "assembly_proteins_id": 1,
        "assembly_accession": "ERZ1",
        "pipeline_version": "5.0",
        "analysis_accession": "MGYA1",
    },
    {
        "assembly_proteins_id": 5,
        "assembly_accession": "ERZ2",
        "pipeline_version": "6.1",
        "analysis_accession": "MGYA2",
    },
]
GENE_CALLERS = [
    {"id": 1, "name": "Pyrodigal", "version": "3.6.3"},
    {"id": 2, "name": "FragGeneScanRS", "version": "1.1.0"},
]


def write_day(root, day, proteins, kind, records=RECORDS):
    for prefix in {layout.prefix(hash) for hash in proteins}:
        rows = [
            (id, hash) for hash, id in proteins.items() if layout.prefix(hash) == prefix
        ]
        layout.write(
            root / "protein" / f"prefix={prefix}" / f"{kind}-{day}.parquet",
            "protein",
            pa.table(
                {
                    "id": [id for id, _ in rows],
                    "hash": [hash for _, hash in rows],
                    "sequence": ["M"] * len(rows),
                }
            ),
        )
    contig_id, occurrence_id = (7, 12) if day == M else (8, 13)
    layout.write(
        root / "contig" / f"day={day}" / "part-000.parquet",
        "contig",
        pa.table(
            {
                "id": [contig_id],
                "assembly_proteins_id": [1],
                "name": ["ERZ1_1"],
                "original_name": [None],
                "length": [100],
                "hash": [hash_of(0, 0)],
                "kmer_coverage": [None],
            }
        ),
    )
    layout.write(
        root / "occurrence" / f"day={day}" / "part-000.parquet",
        "occurrence",
        pa.table(
            {
                "id": [occurrence_id],
                "protein_id": [3],
                "contig_id": [contig_id],
                "assembly_proteins_id": [1],
                "gene_caller_id": [1],
                "start_position": [1],
                "end_position": [99],
                "strand": [1],
                "truncation": ["00"],
            }
        ),
    )
    snapshot = [
        {
            **record,
            "source": "assembly_analysis",
            "study_accession": "MGYS1",
            "biome_lineage": None,
            "is_private": False,
            "is_suppressed": False,
        }
        for record in records
    ]
    for table, rows in [
        ("metadata/assembly_proteins", snapshot),
        ("metadata/gene_callers", GENE_CALLERS),
    ]:
        layout.write(
            root
            / "metadata"
            / f"day={day}"
            / f"{table.removeprefix('metadata/')}.parquet",
            table,
            pa.Table.from_pylist(rows, layout.SCHEMAS[table]),
        )


@pytest.fixture
def migrated(tmp_path):
    write_day(tmp_path, M, MIGRATED, "base")
    return tmp_path


def partitions():
    """What mgyp_schema.sql makes of each partition, which the build must reproduce."""
    return query("""
        SELECT c.relname, c.relpersistence, c.reloptions, c.relowner::regrole::text,
               pg_get_expr(c.relpartbound, c.oid),
               (SELECT array_agg(a.grantee::regrole::text || ' ' || a.privilege_type
                                 ORDER BY a.grantee::regrole::text, a.privilege_type)
                FROM aclexplode(c.relacl) a),
               (SELECT array_agg(conname || ' ' || pg_get_constraintdef(oid) ORDER BY conname)
                FROM pg_constraint WHERE conrelid = c.oid),
               (SELECT array_agg(pg_get_indexdef(x.indexrelid)) FROM pg_index x WHERE x.indrelid = c.oid),
               (SELECT array_agg(inhparent::regclass::text) FROM pg_inherits
                WHERE inhrelid IN (SELECT indexrelid FROM pg_index WHERE indrelid = c.oid))
        FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'proteindb.protein'::regclass
        ORDER BY c.relname
        """)


def loaded():
    return {
        bytes(hash): (id, partition)
        for hash, id, partition in query(
            "SELECT p.hash, p.id, c.relname FROM proteindb.protein p JOIN pg_class c ON c.oid = p.tableoid"
        )
    }


def next_ids():
    return query(
        "SELECT nextval('proteindb.protein_id_seq'), nextval('proteindb.contig_id_seq'),"
        " nextval('proteindb.occurrence_id_seq')"
    )[0]


def test_builds_the_mgyp_database_from_the_migrated_day(migrated):
    before = partitions()
    assert build_mgyp_database(owner_dsn(), migrated) == M

    assert partitions() == before
    assert loaded() == {
        hash: (id, f"protein_{hash[0] >> 2:02x}") for hash, id in MIGRATED.items()
    }
    assert query(
        "SELECT id, assembly_accession, source, pipeline_version, analysis_accession"
        " FROM proteindb.assembly_proteins ORDER BY id"
    ) == [
        (1, "ERZ1", "assembly_analysis", "5.0", "MGYA1"),
        (5, "ERZ2", "assembly_analysis", "6.1", "MGYA2"),
    ]
    assert query("SELECT id, name, version FROM proteindb.gene_caller ORDER BY id") == [
        (1, "Pyrodigal", "3.6.3"),
        (2, "FragGeneScanRS", "1.1.0"),
    ]
    assert query(
        "INSERT INTO proteindb.gene_caller (name, version) VALUES ('Prodigal', '2.6.3') RETURNING id"
    ) == [(3,)]
    assert next_ids() == (42, 8, 13)
    assert query("SELECT day, status FROM proteindb.dataset_update") == [(M, "done")]


def test_refuses_a_schema_that_is_not_empty(migrated):
    build_mgyp_database(owner_dsn(), migrated)
    with pytest.raises(
        BuildError,
        match="protein, assembly_proteins, gene_caller, dataset_update not empty",
    ):
        build_mgyp_database(owner_dsn(), migrated)


def test_a_duplicate_in_a_registry_stops_the_build_before_the_partitions(migrated):
    write_day(
        migrated,
        M,
        MIGRATED,
        "base",
        [RECORDS[0], {**RECORDS[1], "analysis_accession": "MGYA1"}],
    )
    before = partitions()
    with pytest.raises(Exception, match="duplicate key"):
        build_mgyp_database(owner_dsn(), migrated)
    assert partitions() == before
    assert loaded() == {}
    assert query("SELECT count(*) FROM proteindb.assembly_proteins") == [(0,)]


def test_a_build_after_the_migration_refuses_days_after_it(migrated):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "day")
    with pytest.raises(BuildError, match="needs --protein-id-start"):
        build_mgyp_database(owner_dsn(), migrated)
    assert loaded() == {}


def test_a_rebuild_refuses_a_protein_id_start_at_or_below_the_highest_protein_id(
    migrated,
):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "day")
    with pytest.raises(
        BuildError, match="not above the protein dataset's highest protein id, 50"
    ):
        build_mgyp_database(owner_dsn(), migrated, protein_id_start=50)
    assert loaded() == {}


@pytest.mark.usefixtures("mgyp_as_accession")
def test_a_rebuild_builds_the_latest_day_and_starts_protein_ids_at_the_value_given(
    migrated,
):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "day")
    assert build_mgyp_database(owner_dsn(), migrated, protein_id_start=1000) == D

    assert loaded() == {
        hash: (id, f"protein_{hash[0] >> 2:02x}")
        for hash, id in {**MIGRATED, hash_of(0x80, 1): 50}.items()
    }
    assert next_ids() == (1000, 9, 14)
    assert query("SELECT day, status FROM proteindb.dataset_update ORDER BY day") == [
        (M, "done"),
        (D, "done"),
    ]
    # The next dataset update's first step finds the MGYP database level with the protein dataset.
    update.clean_up_failed_update(migrated)


def test_command_builds_from_the_configured_protein_dataset(
    migrated, settings, monkeypatch
):
    monkeypatch.setattr(settings.EMG_CONFIG.proteins, "dataset_dir", str(migrated))
    call_command("build_mgyp_database", "--dsn", owner_dsn())
    assert len(loaded()) == len(MIGRATED)
