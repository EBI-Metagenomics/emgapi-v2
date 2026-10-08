import gzip
import hashlib
import re
import subprocess
from datetime import date

import psycopg
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from django.core.management import CommandError, call_command

import analyses.models as mg_models
from proteins import tier2
from proteins.accession.cli import main as mgyp_accession
from proteins.flows.load import proteindb_dsn
from proteins.load import recover
from proteins.migrate import (
    EXPORTS,
    MigrateError,
    build_tier1,
    build_tier2,
    export,
    export_dims,
    unresolved,
    verify,
)
from proteins.tests.conftest import (
    FIXTURES,
    PUBLISHED_V6,
    empty_tier1,
    query,
    role_connection,
    role_dsn,
)

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
    assert query(
        "SELECT ingest_date, status FROM proteindb.load_log ORDER BY ingest_date"
    ) == [(M, "done"), (D, "done")]
    with psycopg.connect(role_dsn("proteindb_load"), autocommit=True) as conn:
        recover(conn, migrated)


def test_command_builds_tier1_from_the_configured_root(migrated, settings, monkeypatch):
    monkeypatch.setattr(settings.EMG_CONFIG.proteindb, "root", str(migrated.parent))
    call_command("proteindb_migrate", "tier1")
    assert len(loaded()) == len(MIGRATED)


def test_command_takes_rebuild_and_protein_id_start_together():
    with pytest.raises(CommandError, match="go together"):
        call_command("proteindb_migrate", "tier1", "--rebuild")
    with pytest.raises(CommandError, match="go together"):
        call_command("proteindb_migrate", "tier1", "--protein-id-start", "1000")


SEQUENCES = ["MKV", "MALW", "MSTTP", "MGGA", "MPEQ", "MRRL", "MDD", "MYYH"]
SEQUENCES_1 = ["MWWC", "MHHK", "MNNQ", "MCCE"]
CURRENT = """
CREATE SCHEMA mgnprotein_ingestion;
SET search_path = mgnprotein_ingestion;
CREATE TYPE truncation_enum AS ENUM ('00', '01', '10', '11');
CREATE TABLE protein_hash_0 (id bigint NOT NULL, sequence_sha256sum character(64) NOT NULL,
    sequence text NOT NULL, suppressed boolean DEFAULT false, private boolean DEFAULT false);
CREATE TABLE protein_contig_occurrence_hash_0 (id bigint NOT NULL, protein_id bigint NOT NULL,
    contig_id bigint NOT NULL, assembly_id integer NOT NULL, gene_caller_id smallint,
    truncation truncation_enum, start_position integer NOT NULL, end_position integer NOT NULL,
    strand smallint NOT NULL);
CREATE TABLE contig_hash_0 (id bigint NOT NULL, contig_name varchar(200), assembly_id integer NOT NULL,
    sequence_sha256sum character(64) NOT NULL, kmer_coverage double precision, contig_length integer);
CREATE TABLE protein_hash_1 (LIKE protein_hash_0 INCLUDING DEFAULTS);
CREATE TABLE protein_contig_occurrence_hash_1 (LIKE protein_contig_occurrence_hash_0);
CREATE TABLE contig_hash_1 (LIKE contig_hash_0);
CREATE TABLE assembly (id integer, accession varchar(45), study_id integer,
    pipeline_version numeric(2,1), biome_id smallint, suppressed boolean, private boolean);
CREATE TABLE study (id integer, accession varchar(45), private boolean, suppressed boolean);
CREATE TABLE biome (id smallint, lineage varchar(255));
CREATE TABLE gene_caller (id smallint, gene_caller varchar, version varchar);

INSERT INTO protein_hash_0 (id, sequence_sha256sum, sequence)
    SELECT 10 * n, encode(sha256(s::bytea), 'hex'), s FROM unnest(%s::text[]) WITH ORDINALITY AS t(s, n);
INSERT INTO protein_contig_occurrence_hash_0 VALUES
    (1, 10, 7, 1, 1, '01', 3, 101, -1), (2, 20, 7, 1, 2, NULL, 200, 290, 1);
INSERT INTO contig_hash_0 VALUES (7, 'ERZ1.1', 1, encode(sha256('ACGT'), 'hex'), 2.5, 400);
INSERT INTO protein_hash_1 (id, sequence_sha256sum, sequence)
    SELECT 100 + 10 * n, encode(sha256(s::bytea), 'hex'), s FROM unnest(%s::text[]) WITH ORDINALITY AS t(s, n);
INSERT INTO protein_contig_occurrence_hash_1 VALUES
    (3, 110, 9, 2, 1, '11', 500, 600, 1), (4, 30, 9, 2, 1, NULL, 5, 50, -1);
INSERT INTO contig_hash_1 VALUES (9, 'ERZ1.9', 2, encode(sha256('GGCC'), 'hex'), NULL, 300),
    (8, 'ERZ1.8', 2, encode(sha256('TTAA'), 'hex'), 1.0, 200);
INSERT INTO assembly VALUES (1, 'ERZ1', 1, 4.1, 2, false, false), (2, 'ERZ1', 1, 6.0, 2, false, false);
INSERT INTO study VALUES (1, 'MGYS1', false, false);
INSERT INTO biome VALUES (2, 'root:Engineered');
INSERT INTO gene_caller VALUES (1, 'Prodigal', '2.6.3'), (2, 'FragGeneScan', '1.20');
CREATE VIEW protein AS SELECT * FROM protein_hash_0 UNION ALL SELECT * FROM protein_hash_1;
CREATE VIEW protein_contig_occurrence AS
    SELECT * FROM protein_contig_occurrence_hash_0 UNION ALL SELECT * FROM protein_contig_occurrence_hash_1;
CREATE VIEW contig AS SELECT * FROM contig_hash_0 UNION ALL SELECT * FROM contig_hash_1;
RESET search_path;
"""


@pytest.fixture
def current():
    """A small copy of the frozen current database, with children 0 and 1 of each large table."""
    query(CURRENT, [SEQUENCES, SEQUENCES_1])
    yield proteindb_dsn()
    query("DROP SCHEMA mgnprotein_ingestion CASCADE")


def read(directory):
    return ds.dataset(directory, partitioning="hive").to_table()


def test_exports_proteins_split_by_prefix_with_32_byte_hashes(current, tmp_path):
    assert export(current, tmp_path, "protein", 0) == len(SEQUENCES)

    pieces = sorted((tmp_path / "protein" / "child=0").glob("prefix=*"))
    exported = {}
    for piece in pieces:
        table = pq.read_table(list(piece.glob("*.parquet"))[0])
        assert table.schema == tier2.SCHEMAS["protein"]
        for row in table.to_pylist():
            assert piece.name == f"prefix={tier2.prefix(row['hash'])}"
            exported[row["sequence"]] = (row["id"], row["hash"])
    assert exported == {
        s: (10 * n, hashlib.sha256(s.encode()).digest())
        for n, s in enumerate(SEQUENCES, 1)
    }
    assert len(pieces) > 1


def test_exports_occurrences_and_contigs_in_tier2s_types(current, tmp_path):
    export(current, tmp_path, "occurrence", 0)
    export(current, tmp_path, "contig", 0)

    occurrences = read(tmp_path / "occurrence" / "child=0")
    assert occurrences.schema == tier2.SCHEMAS["occurrence"]
    assert occurrences.sort_by("id").to_pylist() == [
        {
            "id": 1,
            "protein_id": 10,
            "contig_id": 7,
            "assembly_id": 1,
            "gene_caller_id": 1,
            "start_position": 3,
            "end_position": 101,
            "strand": -1,
            "truncation": "01",
        },
        {
            "id": 2,
            "protein_id": 20,
            "contig_id": 7,
            "assembly_id": 1,
            "gene_caller_id": 2,
            "start_position": 200,
            "end_position": 290,
            "strand": 1,
            "truncation": None,
        },
    ]
    contigs = read(tmp_path / "contig" / "child=0")
    assert contigs.schema == tier2.SCHEMAS["contig"]
    assert contigs.to_pylist() == [
        {
            "id": 7,
            "assembly_id": 1,
            "name": "ERZ1.1",
            "original_name": None,
            "length": 400,
            "hash": hashlib.sha256(b"ACGT").digest(),
            "kmer_coverage": 2.5,
        }
    ]


def test_exports_the_reference_tables_as_snapshot_files(current, tmp_path):
    export_dims(current, tmp_path)

    def rows(name):
        table = pq.read_table(tmp_path / "dims" / f"{name}.parquet")
        assert table.schema == tier2.SCHEMAS[f"dims/{name}"]
        return table.to_pylist()

    assert rows("assembly") == [
        {
            "id": 1,
            "accession": "ERZ1",
            "pipeline_version": "4.1",
            "study_id": 1,
            "biome_id": 2,
            "private": False,
            "suppressed": False,
        },
        {
            "id": 2,
            "accession": "ERZ1",
            "pipeline_version": "6.0",
            "study_id": 1,
            "biome_id": 2,
            "private": False,
            "suppressed": False,
        },
    ]
    assert rows("study") == [
        {"id": 1, "accession": "MGYS1", "private": False, "suppressed": False}
    ]
    assert rows("biome") == [{"id": 2, "lineage": "root:Engineered"}]
    assert rows("gene_caller") == [
        {"id": 1, "name": "Prodigal", "version": "2.6.3"},
        {"id": 2, "name": "FragGeneScan", "version": "1.20"},
    ]


def test_refuses_to_export_a_child_twice(current, tmp_path):
    export(current, tmp_path, "contig", 0)
    with pytest.raises(MigrateError, match="already exported"):
        export(current, tmp_path, "contig", 0)


def test_refuses_nulls_that_tier2_does_not_allow(current, tmp_path):
    query("UPDATE mgnprotein_ingestion.contig_hash_0 SET contig_name = NULL")
    with pytest.raises(MigrateError, match="contig.name has NULLs"):
        export(current, tmp_path, "contig", 0)
    assert not (tmp_path / "contig" / "child=0").exists()


def test_command_exports_a_child(current, tmp_path):
    call_command(
        "proteindb_migrate",
        "export",
        "--table",
        "contig",
        "--child",
        "0",
        "--source",
        current,
        "--out",
        str(tmp_path),
    )
    assert read(tmp_path / "contig" / "child=0").num_rows == 1


def test_command_takes_a_child_for_every_table_but_dims(tmp_path):
    for table, child in [("contig", []), ("dims", ["--child", "0"])]:
        with pytest.raises(CommandError, match="--child is needed"):
            call_command(
                "proteindb_migrate",
                "export",
                "--table",
                table,
                *child,
                "--source",
                "postgresql://",
                "--out",
                str(tmp_path),
            )


@pytest.fixture
def exported(current, tmp_path):
    """The export of the current database, whose children but 0 and 1 are empty."""
    out = tmp_path / "migration"
    for table in EXPORTS:
        for child in range(64):
            if child < 2:
                export(current, out, table, child)
            else:
                (out / table / f"child={child}").mkdir(parents=True)
    export_dims(current, out)
    return out


def proteins():
    return {
        s: (id, hashlib.sha256(s.encode()).digest())
        for id, s in [
            *((10 * n, s) for n, s in enumerate(SEQUENCES, 1)),
            *((100 + 10 * n, s) for n, s in enumerate(SEQUENCES_1, 1)),
        ]
    }


def test_builds_tier2s_first_day_from_the_export(exported, tmp_path):
    root = tmp_path / "tier2"
    build_tier2(exported, root, M)

    assert tier2.complete_days(root) == [M]
    built = {}
    for path in tier2.files(root, "protein"):
        assert path.name == f"base-{M}.parquet"
        table = pq.read_table(path)
        assert table.schema == tier2.SCHEMAS["protein"]
        hashes = table["hash"].to_pylist()
        assert hashes == sorted(hashes)
        assert {f"prefix={tier2.prefix(h)}" for h in hashes} == {path.parent.name}
        built |= {r["sequence"]: (r["id"], r["hash"]) for r in table.to_pylist()}
    assert built == proteins()

    for table, ids in [("occurrence", [[1, 2], [4, 3]]), ("contig", [[7], [8, 9]])]:
        parts = tier2.files(root, table)
        assert [p.name for p in parts] == ["part-000.parquet", "part-001.parquet"]
        for part, expected in zip(parts, ids):
            rows = pq.read_table(part)
            assert rows.schema == tier2.SCHEMAS[table]
            assert rows["id"].to_pylist() == expected
    for name in ("assembly", "study", "biome", "gene_caller"):
        assert pq.read_table(tier2.files(root, f"dims/{name}")[0]) == pq.read_table(
            exported / "dims" / f"{name}.parquet"
        )


def test_tier1_built_from_the_migrated_tier2_holds_every_protein(exported, tmp_path):
    build_tier2(exported, tmp_path / "tier2", M)
    build_tier1(proteindb_dsn(), tmp_path / "tier2")

    assert {h: id for h, (id, _) in loaded().items()} == {
        h: id for id, h in proteins().values()
    }
    assert next_ids() == (141, 10, 5)


def test_refuses_an_incomplete_export(exported, tmp_path):
    (exported / "contig" / "child=63").rmdir()
    with pytest.raises(MigrateError, match="missing 1: contig/child=63"):
        build_tier2(exported, tmp_path / "tier2", M)
    assert not (tmp_path / "tier2").exists()


def test_refuses_a_tier2_that_has_a_complete_day(exported, tmp_path):
    build_tier2(exported, tmp_path / "tier2", M)
    with pytest.raises(MigrateError, match="already has a complete day"):
        build_tier2(exported, tmp_path / "tier2", M)


def test_resumes_an_interrupted_build(exported, tmp_path):
    root = tmp_path / "tier2"
    build_tier2(exported, root, M)
    before = {p: p.read_bytes() for p in root.rglob("*.parquet")}
    kept, lost = tier2.files(root, "protein")[:2]
    kept_at = kept.stat().st_mtime_ns
    (root / "dims" / f"snapshot={M}").rename(root / "dims" / f".tmp-snapshot={M}")
    lost.unlink()
    (root / "occurrence" / f"ingest_date={M}" / "part-001.parquet").unlink()

    build_tier2(exported, root, M)

    assert {p: p.read_bytes() for p in root.rglob("*.parquet")} == before
    assert kept.stat().st_mtime_ns == kept_at
    assert tier2.complete_days(root) == [M]


def test_command_builds_tier2_into_the_configured_root(
    exported, tmp_path, settings, monkeypatch
):
    monkeypatch.setattr(settings.EMG_CONFIG.proteindb, "root", str(tmp_path))
    call_command(
        "proteindb_migrate", "tier2", "--date", str(M), "--export", str(exported)
    )
    assert tier2.complete_days(tmp_path / "tier2") == [M]


@pytest.fixture
def rehearsed(exported, tmp_path):
    """Tier 2 and Tier 1 migrated from the current database."""
    build_tier2(exported, tmp_path / "tier2", M)
    build_tier1(proteindb_dsn(), tmp_path / "tier2")
    return tmp_path / "tier2"


def test_a_rehearsal_passes_every_check(rehearsed):
    assert verify(proteindb_dsn(), proteindb_dsn(), rehearsed) == []


def lose_a_protein_key_row(root):
    query("DELETE FROM proteindb.protein_key WHERE id = 10")


def change_a_sequence(root):
    path = tier2.files(root, "protein")[0]
    rows = pq.read_table(path)
    sequences = ["MXXX", *rows["sequence"].to_pylist()[1:]]
    tier2.write(path, "protein", rows.set_column(2, "sequence", pa.array(sequences)))


def duplicate_an_id(root):
    path = next(
        p for p in tier2.files(root, "protein") if pq.read_metadata(p).num_rows > 1
    )
    rows = pq.read_table(path)
    ids = rows["id"].to_pylist()
    tier2.write(
        path,
        "protein",
        rows.set_column(0, "id", pa.array([ids[0], ids[0], *ids[2:]], pa.int64())),
    )
    query("UPDATE proteindb.protein_key SET id = %s WHERE id = %s", [ids[0], ids[1]])


def add_a_contig_after_the_export(root):
    query(
        "INSERT INTO mgnprotein_ingestion.contig_hash_1 VALUES"
        " (10, 'ERZ1.10', 2, encode(sha256('A'), 'hex'), NULL, 1)"
    )


@pytest.mark.parametrize(
    "fault, failure",
    [
        (lose_a_protein_key_row, "protein_key has 11 rows, and Tier 2 protein 12"),
        (
            lose_a_protein_key_row,
            r"prefix \w\w: \(rows, sum of ids\) \(\d+, \d+\) in Tier 1",
        ),
        (change_a_sequence, "1 sampled rows whose hash is not sha256"),
        (duplicate_an_id, "Tier 2 protein has 12 rows, but 11 distinct ids"),
        (
            add_a_contig_after_the_export,
            "Tier 2 contig has 3 rows, and the frozen table 4",
        ),
    ],
)
def test_verify_finds_a_fault(rehearsed, fault, failure):
    fault(rehearsed)
    failed = verify(proteindb_dsn(), proteindb_dsn(), rehearsed)
    assert [f for f in failed if re.search(failure, f)], failed


def test_verify_checks_the_migration_only(rehearsed):
    write_day(rehearsed, D, {hash_of(0x80, 1): 500}, "part")
    with pytest.raises(MigrateError, match="2 complete days"):
        verify(proteindb_dsn(), proteindb_dsn(), rehearsed)


def test_command_verifies(rehearsed, current, settings, monkeypatch):
    monkeypatch.setattr(settings.EMG_CONFIG.proteindb, "root", str(rehearsed.parent))
    call_command("proteindb_migrate", "verify", "--source", current)
    add_a_contig_after_the_export(rehearsed)
    with pytest.raises(CommandError, match="1 checks failed"):
        call_command("proteindb_migrate", "verify", "--source", current)


@pytest.fixture
def analysed(current, mgnify_assemblies, raw_reads_mgnify_study):
    """ERZ1, under 4.1 and 6.0, analysed in emgapi-v2, and ERZ5 under 5.0, not."""
    assembly = mgnify_assemblies[0]
    assembly.ena_accessions = ["ERZ1"]
    assembly.save()
    mg_models.Analysis.objects.create(
        study=raw_reads_mgnify_study,
        sample=assembly.sample,
        assembly=assembly,
        ena_study=raw_reads_mgnify_study.ena_study,
        pipeline_version=mg_models.Analysis.PipelineVersions.v6,
    )
    query(
        "INSERT INTO mgnprotein_ingestion.assembly VALUES (3, 'ERZ5', 1, 5.0, 2, false, false)"
    )
    return current


def test_lists_the_assemblies_emgapi_v2_does_not_know(analysed):
    assert unresolved(analysed) == (2, [("5.0", "ERZ5")])


def test_command_lists_the_assemblies_emgapi_v2_does_not_know(analysed, capsys):
    with pytest.raises(CommandError, match="1 of 2 assembly accessions resolve"):
        call_command("proteindb_migrate", "resolve", "--source", analysed)
    assert capsys.readouterr().out == "5.0\tERZ5\n"

    query("DELETE FROM mgnprotein_ingestion.assembly WHERE accession = 'ERZ5'")
    call_command("proteindb_migrate", "resolve", "--source", analysed)
    assert "1 of 1 assembly accessions resolve" in capsys.readouterr().out


# Step 3 of restarting allocation after lost commits.
HIGHEST_MGYP = (
    "set -o pipefail; xargs -a outputs.txt zcat -f"
    " | grep -o '^>[^ ]* MGYP[0-9]\\{12\\}' | grep -o 'MGYP[0-9]*' | sort | tail -n 1"
)


def accession_published(assembly, out):
    assert (
        mgyp_accession(
            [
                *("--assembly", assembly, "--pipeline-version", "6.0"),
                *("--faa", str(FIXTURES / f"{assembly}.faa.gz")),
                *("--gff", str(FIXTURES / f"{assembly}.gff.gz")),
                *("--contigs", str(FIXTURES / f"{assembly}.fasta.gz")),
                *("--out", str(out)),
            ]
        )
        == 0
    )
    with gzip.open(out, "rt") as fasta:
        return [int(m) for m in re.findall(r"^>\S+ MGYP(\d{12})", fasta.read(), re.M)]


def test_allocation_restarts_above_every_mgyp_emitted_after_tier1_is_lost(
    rehearsed, tmp_path, monkeypatch
):
    monkeypatch.setenv("PROTEINDB_DSN", role_dsn("proteindb_accession"))
    first, last, after = PUBLISHED_V6
    before = accession_published(first, tmp_path / f"{first}.faa.gz")
    with role_connection("proteindb_accession") as conn:
        conn.execute(
            "INSERT INTO proteindb.protein_key (hash, id) SELECT sha256(n::text::bytea),"
            " nextval('proteindb.protein_id_seq') FROM generate_series(1, 500) n"
        )
        conn.rollback()
        conn.execute(
            "INSERT INTO proteindb.protein_key (hash, id) SELECT hash,"
            " nextval('proteindb.protein_id_seq') FROM proteindb.protein_key"
            " ON CONFLICT DO NOTHING"
        )
    emitted = accession_published(last, tmp_path / f"{last}.faa.gz")
    assert min(emitted) > max(before) + 500

    empty_tier1()
    (tmp_path / "outputs.txt").write_text(f"{first}.faa.gz\n{last}.faa.gz\n")
    highest = subprocess.run(
        ["bash", "-c", HIGHEST_MGYP],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert highest == f"MGYP{max(emitted):012d}"

    build_tier1(proteindb_dsn(), rehearsed, protein_id_start=int(highest[4:]) + 1)
    again = accession_published(last, tmp_path / f"recovered-{last}.faa.gz")
    assert min(again) == max(emitted) + 1
    assert min(accession_published(after, tmp_path / f"{after}.faa.gz")) > max(again)


def test_reading_the_highest_mgyp_fails_on_a_missing_output(tmp_path):
    (tmp_path / "outputs.txt").write_text("missing.faa.gz\n")
    assert subprocess.run(["bash", "-c", HIGHEST_MGYP], cwd=tmp_path).returncode != 0
