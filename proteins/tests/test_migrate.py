import hashlib
from datetime import date

import pyarrow as pa
import pytest
from django.core.management import CommandError, call_command

from proteins import tier2
from proteins.flows.load import proteindb_dsn
from proteins.migrate import MigrateError, build_tier1
from proteins.tests.conftest import query

pytestmark = [
    pytest.mark.django_db(databases=["default", "proteindb"], transaction=True),
    pytest.mark.usefixtures("emptied_tier1"),
]

M, D = date(2026, 11, 2), date(2026, 11, 3)


def hash_of(first_byte, n):
    return bytes([first_byte]) + hashlib.sha256(str(n).encode()).digest()[1:]


# Both ends of the first and last partitions, and of one in between.
MIGRATED = {
    hash_of(b, b): id
    for b, id in [(0x00, 3), (0x03, 10), (0x04, 11), (0x7F, 25), (0xFC, 40), (0xFF, 41)]
}
DIMS = {
    "assembly": [
        {"id": 1, "accession": "ERZ1", "pipeline_version": "5.0"},
        {"id": 5, "accession": "ERZ2", "pipeline_version": "6.1"},
    ],
    "study": [{"id": 3, "accession": "MGYS1", "private": False, "suppressed": False}],
    "biome": [{"id": 2, "lineage": "root:Engineered"}],
    "gene_caller": [
        {"id": 1, "name": "Pyrodigal", "version": "3.6.3"},
        {"id": 2, "name": "FragGeneScanRS", "version": "1.1.0"},
    ],
}


def write_day(root, day, proteins, kind):
    for prefix in {tier2.prefix(hash) for hash in proteins}:
        rows = [
            (id, hash) for hash, id in proteins.items() if tier2.prefix(hash) == prefix
        ]
        tier2.write(
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
    tier2.write(
        root / "contig" / f"ingest_date={day}" / "part-000.parquet",
        "contig",
        pa.table(
            {
                "id": [contig_id],
                "assembly_id": [1],
                "name": ["ERZ1_1"],
                "original_name": [None],
                "length": [100],
                "hash": [hash_of(0, 0)],
                "kmer_coverage": [None],
            }
        ),
    )
    tier2.write(
        root / "occurrence" / f"ingest_date={day}" / "part-000.parquet",
        "occurrence",
        pa.table(
            {
                "id": [occurrence_id],
                "protein_id": [3],
                "contig_id": [contig_id],
                "assembly_id": [1],
                "gene_caller_id": [1],
                "start_position": [1],
                "end_position": [99],
                "strand": [1],
                "truncation": ["00"],
            }
        ),
    )
    for name, rows in DIMS.items():
        tier2.write(
            root / "dims" / f"snapshot={day}" / f"{name}.parquet",
            f"dims/{name}",
            pa.Table.from_pylist(rows, tier2.SCHEMAS[f"dims/{name}"]),
        )


@pytest.fixture
def migrated(tmp_path):
    write_day(tmp_path / "tier2", M, MIGRATED, "base")
    return tmp_path / "tier2"


def partitions():
    """What tier1.sql makes of each partition, which the build must reproduce."""
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
        WHERE i.inhparent = 'proteindb.protein_key'::regclass
        ORDER BY c.relname
        """)


def loaded():
    return {
        bytes(hash): (id, partition)
        for hash, id, partition in query(
            "SELECT k.hash, k.id, c.relname FROM proteindb.protein_key k JOIN pg_class c ON c.oid = k.tableoid"
        )
    }


def next_ids():
    return query(
        "SELECT nextval('proteindb.protein_id_seq'), nextval('proteindb.contig_id_seq'),"
        " nextval('proteindb.occurrence_id_seq')"
    )[0]


def test_builds_tier1_from_the_migrated_day(migrated):
    before = partitions()
    assert build_tier1(proteindb_dsn(), migrated) == M

    assert partitions() == before
    assert loaded() == {
        hash: (id, f"protein_key_{hash[0] >> 2:02x}") for hash, id in MIGRATED.items()
    }
    assert query(
        "SELECT id, accession, pipeline_version FROM proteindb.assembly ORDER BY id"
    ) == [
        (1, "ERZ1", "5.0"),
        (5, "ERZ2", "6.1"),
    ]
    assert query("SELECT id, accession FROM proteindb.study") == [(3, "MGYS1")]
    assert query("SELECT id, lineage FROM proteindb.biome") == [(2, "root:Engineered")]
    assert query("SELECT id, name, version FROM proteindb.gene_caller ORDER BY id") == [
        (1, "Pyrodigal", "3.6.3"),
        (2, "FragGeneScanRS", "1.1.0"),
    ]
    assert query(
        "INSERT INTO proteindb.study (accession) VALUES ('MGYS2') RETURNING id"
    ) == [(4,)]
    assert next_ids() == (42, 8, 13)
    assert query("SELECT ingest_date, status FROM proteindb.load_log") == [(M, "done")]


def test_refuses_a_schema_that_is_not_empty(migrated):
    build_tier1(proteindb_dsn(), migrated)
    with pytest.raises(
        MigrateError,
        match="protein_key, assembly, study, biome, gene_caller, load_log not empty",
    ):
        build_tier1(proteindb_dsn(), migrated)


def test_a_migration_refuses_tier2_with_days_after_it(migrated):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "part")
    with pytest.raises(MigrateError, match="needs --protein-id-start"):
        build_tier1(proteindb_dsn(), migrated)
    assert loaded() == {}


def test_a_rebuild_refuses_a_protein_id_start_at_or_below_tier2s_highest(migrated):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "part")
    with pytest.raises(MigrateError, match="not above Tier 2's highest protein id, 50"):
        build_tier1(proteindb_dsn(), migrated, protein_id_start=50)
    assert loaded() == {}


def test_a_rebuild_builds_the_latest_day_and_starts_protein_ids_at_the_value_given(
    migrated,
):
    write_day(migrated, D, {hash_of(0x80, 1): 50}, "part")
    assert build_tier1(proteindb_dsn(), migrated, protein_id_start=1000) == D

    assert loaded() == {
        hash: (id, f"protein_key_{hash[0] >> 2:02x}")
        for hash, id in {**MIGRATED, hash_of(0x80, 1): 50}.items()
    }
    assert next_ids() == (1000, 9, 14)
    assert query("SELECT ingest_date, status FROM proteindb.load_log") == [(D, "done")]


def test_command_builds_tier1_from_the_configured_root(migrated, settings, monkeypatch):
    monkeypatch.setattr(settings.EMG_CONFIG.proteindb, "root", str(migrated.parent))
    call_command("proteindb_migrate", "tier1")
    assert len(loaded()) == len(MIGRATED)


def test_command_takes_rebuild_and_protein_id_start_together():
    with pytest.raises(CommandError, match="go together"):
        call_command("proteindb_migrate", "tier1", "--rebuild")
    with pytest.raises(CommandError, match="go together"):
        call_command("proteindb_migrate", "tier1", "--protein-id-start", "1000")
