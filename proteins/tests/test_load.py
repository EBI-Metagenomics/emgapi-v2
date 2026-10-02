import hashlib
import shutil
from datetime import date, timedelta
from functools import cache

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from django.db import connections

from proteins import load as load_module
from proteins import tier2
from proteins.accession.accession import accession
from proteins.accession.inputs import read_input
from proteins.dims import Source
from proteins.load import LoadError, load
from proteins.tests.conftest import FIXTURES, read_published, role_dsn

pytestmark = pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)

D1, D2 = date(2026, 10, 1), date(2026, 10, 2)
D31, D32 = D1 + timedelta(days=31), D1 + timedelta(days=32)

SOURCES = {
    "ERZ101": Source("ERZ101", "MGYS1", "root:Engineered", False, False, False, False),
    "ERZ29562087": Source(
        "ERZ29562087", "MGYS1", "root:Engineered", False, True, False, False
    ),
    "ERZ25069264": Source(
        "ERZ25069264",
        "MGYS2",
        "root:Host-associated:Porifera",
        True,
        False,
        True,
        False,
    ),
}  # ERZ29895170 is unknown to emgapi-v2


def resolve(accessions):
    return [SOURCES[a] for a in accessions if a in SOURCES]


@cache
def read(assembly):
    if assembly == "ERZ101":
        return read_input(
            FIXTURES / "ERZ101.faa.gz",
            FIXTURES / "ERZ101.gff.gz",
            FIXTURES / "ERZ101.fasta.gz",
            FIXTURES / "ERZ101_mapping.csv",
        )
    return read_published(assembly)


@pytest.fixture
def stage(connect_accession):
    def stage(assembly, version="6.0"):
        accession(connect_accession, assembly, version, read(assembly))

    return stage


def run(root, day):
    return load(role_dsn("proteindb_load"), root, day, resolve)


class Killed(BaseException):
    """Ends a load as a kill would: no handler of the load's own runs."""


def query(sql, params=None):
    with connections["proteindb"].cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else None


TIER2_COLUMNS = {
    "protein": "id, hash, sequence",
    "contig": "id, assembly_id, name, original_name, length, hash, kmer_coverage",
    "occurrence": "id, protein_id, contig_id, assembly_id, gene_caller_id,"
    " start_position, end_position, strand, truncation",
}


def staged():
    return {
        table: sorted(
            query(f"SELECT {columns} FROM proteindb.staging_{table}"), key=repr
        )
        for table, columns in TIER2_COLUMNS.items()
    }


def in_tier2(root, as_of=None):
    return {
        table: sorted(
            (
                tuple(row.values())
                for path in tier2.files(root, table, as_of)
                for row in pq.read_table(path).to_pylist()
            ),
            key=repr,
        )
        for table in TIER2_COLUMNS
    }


def joined(*days):
    return {
        table: sorted((row for day in days for row in day[table]), key=repr)
        for table in TIER2_COLUMNS
    }


def listing(root):
    """Every file under root, with what would change if it were rewritten."""
    return {
        str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def layout(day, proteins):
    """The files an uninterrupted load of `proteins` writes for `day`."""
    return {
        *(
            f"protein/prefix={tier2.prefix(hash)}/part-{day}.parquet"
            for _, hash, _ in proteins
        ),
        f"occurrence/ingest_date={day}/part-000.parquet",
        f"contig/ingest_date={day}/part-000.parquet",
        *(
            f"dims/snapshot={day}/{name}.parquet"
            for name in ("assembly", "study", "biome", "gene_caller")
        ),
    }


def statuses():
    return query("SELECT ingest_date, status FROM proteindb.load_log ORDER BY id")


def dims(root, name, as_of=None):
    (path,) = tier2.files(root, f"dims/{name}", as_of)
    return pq.read_table(path).to_pylist()


def test_two_days_load_every_staged_row_once(tmp_path, stage):
    stage("ERZ101")
    stage("ERZ29562087")
    day1 = staged()
    assert run(tmp_path, D1)
    stage("ERZ25069264")
    stage("ERZ29895170")
    stage("ERZ101", "6.1")
    day2 = staged()
    assert run(tmp_path, D2)

    assert in_tier2(tmp_path) == joined(day1, day2)
    assert in_tier2(tmp_path, D1) == day1
    assert staged() == {table: [] for table in TIER2_COLUMNS}
    for path in tier2.files(tmp_path, "protein"):
        assert {
            tier2.prefix(hash) for hash in pq.read_table(path)["hash"].to_pylist()
        } == {path.parent.name.removeprefix("prefix=")}
    assert set(listing(tmp_path)) == layout(D1, day1["protein"]) | layout(
        D2, day2["protein"]
    )
    assert query(
        "SELECT ingest_date, status, assemblies, proteins, contigs, occurrences,"
        " unresolved_assemblies FROM proteindb.load_log ORDER BY id"
    ) == [
        (
            D1,
            "done",
            2,
            *(len(day1[t]) for t in ("protein", "contig", "occurrence")),
            0,
        ),
        (
            D2,
            "done",
            3,
            *(len(day2[t]) for t in ("protein", "contig", "occurrence")),
            1,
        ),
    ]


def test_dimensions_snapshot_the_registries_and_the_source(tmp_path, stage):
    stage("ERZ101")
    stage("ERZ29562087")
    stage("ERZ25069264")
    stage("ERZ29895170")
    run(tmp_path, D1)

    studies = {row["accession"]: row for row in dims(tmp_path, "study")}
    biomes = {row["lineage"]: row["id"] for row in dims(tmp_path, "biome")}
    assert {
        (
            row["accession"],
            row["study_id"],
            row["biome_id"],
            row["private"],
            row["suppressed"],
        )
        for row in dims(tmp_path, "assembly")
    } == {
        ("ERZ101", studies["MGYS1"]["id"], biomes["root:Engineered"], False, False),
        ("ERZ29562087", studies["MGYS1"]["id"], biomes["root:Engineered"], False, True),
        (
            "ERZ25069264",
            studies["MGYS2"]["id"],
            biomes["root:Host-associated:Porifera"],
            True,
            False,
        ),
        ("ERZ29895170", None, None, None, None),
    }
    assert (studies["MGYS2"]["private"], studies["MGYS2"]["suppressed"]) == (
        True,
        False,
    )
    assert sorted(
        (row["id"], row["name"], row["version"])
        for row in dims(tmp_path, "gene_caller")
    ) == query("SELECT id, name, version FROM proteindb.gene_caller ORDER BY id")
    assert query("SELECT accession FROM proteindb.study ORDER BY 1") == [
        ("MGYS1",),
        ("MGYS2",),
    ]


def test_known_studies_and_biomes_take_no_identity_value(tmp_path, stage):
    def last_values():
        return query(
            "SELECT (SELECT last_value FROM proteindb.study_id_seq),"
            " (SELECT last_value FROM proteindb.biome_id_seq)"
        )

    stage("ERZ101")
    run(tmp_path, D1)
    before = last_values()
    stage("ERZ101", "6.1")
    stage("ERZ29562087")
    run(tmp_path, D2)
    assert last_values() == before


def test_a_second_load_on_the_same_date_writes_nothing(tmp_path, stage):
    stage("ERZ101")
    run(tmp_path, D1)
    before = listing(tmp_path)
    stage("ERZ29562087")
    waiting = staged()

    assert run(tmp_path, D1) is None
    assert listing(tmp_path) == before
    assert staged() == waiting
    assert statuses() == [(D1, "done")]


STEPS = ["read_staging", "snapshot", "write_day", "commit", "publish", "compact"]


@pytest.mark.parametrize(
    "error", [RuntimeError("crash"), Killed()], ids=["error", "kill"]
)
@pytest.mark.parametrize("when", ["before", "after"])
@pytest.mark.parametrize("step", STEPS)
def test_a_crash_at_any_step_ends_as_an_uninterrupted_load(
    tmp_path, stage, monkeypatch, step, when, error
):
    stage("ERZ101")
    stage("ERZ29562087")
    expected = staged()
    original = getattr(load_module, step)

    def crashing(*args, **kwargs):
        if when == "after":
            original(*args, **kwargs)
        raise error

    monkeypatch.setattr(load_module, step, crashing)
    with pytest.raises(type(error)):
        run(tmp_path, D1)
    monkeypatch.undo()
    committed = step in ("publish", "compact") or (step, when) == ("commit", "after")
    if committed:
        assert statuses() == [(D1, "done")]
    elif isinstance(error, Killed):
        assert statuses() == [(D1, "running")]
    else:
        assert query("SELECT status, message FROM proteindb.load_log") == [
            ("failed", "RuntimeError: crash")
        ]
    data_files = {
        path: stat for path, stat in listing(tmp_path).items() if "dims" not in path
    }

    run(tmp_path, D1)

    assert in_tier2(tmp_path) == expected
    assert staged() == {table: [] for table in TIER2_COLUMNS}
    assert set(listing(tmp_path)) == layout(D1, expected["protein"])
    assert [status for _, status in statuses()] == (
        ["done"] if committed else ["failed", "done"]
    )
    if committed:
        assert {
            path: stat for path, stat in listing(tmp_path).items() if path in data_files
        } == data_files

    after = listing(tmp_path)
    assert run(tmp_path, D1) is None
    assert listing(tmp_path) == after


def test_a_load_whose_row_was_marked_failed_does_not_commit(
    tmp_path, stage, monkeypatch
):
    stage("ERZ101")
    expected = staged()
    write_day = load_module.write_day

    def write_then_lose_the_row(*args):
        counts = write_day(*args)
        query("UPDATE proteindb.load_log SET status = 'failed'")
        return counts

    monkeypatch.setattr(load_module, "write_day", write_then_lose_the_row)
    with pytest.raises(LoadError, match="no longer running"):
        run(tmp_path, D1)
    assert staged() == expected
    assert tier2.complete_days(tmp_path) == []


def test_a_rename_that_always_fails_deletes_nothing(tmp_path, stage, monkeypatch):
    stage("ERZ101")
    expected = staged()

    def failing(root, day):
        raise OSError("rename failed")

    monkeypatch.setattr(load_module, "publish", failing)
    for _ in range(2):
        before = listing(tmp_path)
        with pytest.raises(OSError):
            run(tmp_path, D1)
        assert statuses() == [(D1, "done")]
    assert listing(tmp_path) == before

    monkeypatch.undo()
    assert run(tmp_path, D1) is None
    assert in_tier2(tmp_path) == expected


def test_a_done_day_without_its_snapshot_fails_and_names_the_day(
    tmp_path, stage, monkeypatch
):
    stage("ERZ101")

    def killed(root, day):
        raise Killed

    monkeypatch.setattr(load_module, "publish", killed)
    with pytest.raises(Killed):
        run(tmp_path, D1)
    monkeypatch.undo()
    shutil.rmtree(tmp_path / "dims" / f".tmp-snapshot={D1}")

    for day in (D1, D2):
        with pytest.raises(LoadError, match=str(D1)):
            run(tmp_path, day)


def test_leftovers_of_failed_loads_are_deleted(tmp_path, stage):
    stage("ERZ101")
    run(tmp_path, D1)
    before = listing(tmp_path)
    leftovers = [
        f"protein/prefix=00/part-{D2}.parquet",
        f"protein/prefix=00/.part-{D2}.parquet.tmp",
        f"occurrence/ingest_date={D2}/part-000.parquet",
        f"contig/ingest_date={D2}/.part-000.parquet.tmp",
        f"dims/.tmp-snapshot={D2}/assembly.parquet",
        f"occurrence/ingest_date={D1}/.part-000.parquet.tmp",
    ]
    for leftover in leftovers:
        (tmp_path / leftover).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / leftover).touch()

    assert run(tmp_path, D1) is None
    assert listing(tmp_path) == before


def test_protein_files_superseded_by_a_base_are_deleted(tmp_path, stage):
    stage("ERZ101")
    run(tmp_path, D1)
    prefix = tmp_path / "protein" / "prefix=00"
    prefix.mkdir(parents=True, exist_ok=True)
    for name in (
        "base-2026-09-01.parquet",
        "part-2026-09-02.parquet",
        "base-2026-09-03.parquet",
        f"part-{D1}.parquet",
    ):
        (prefix / name).touch()

    run(tmp_path, D1)
    assert sorted(p.name for p in prefix.iterdir()) == [
        "base-2026-09-03.parquet",
        f"part-{D1}.parquet",
    ]


@pytest.mark.parametrize(
    "late",
    [("ERZ101", "6.1"), ("ERZ29562087", "6.0")],
    ids=["known-proteins", "new-protein"],
)
def test_an_analysis_committed_while_a_load_reads_waits_for_the_next(
    tmp_path, stage, monkeypatch, late
):
    stage("ERZ101")
    expected = staged()
    choose = load_module.choose_assemblies

    def choose_then_commit_another(cursor):
        chosen = choose(cursor)
        stage(*late)
        return chosen

    monkeypatch.setattr(load_module, "choose_assemblies", choose_then_commit_another)
    run(tmp_path, D1)
    monkeypatch.undo()

    assert in_tier2(tmp_path) == expected
    waiting = staged()
    assert waiting["occurrence"]
    assert ("ERZ101", "6.0") in {
        (row["accession"], row["pipeline_version"])
        for row in dims(tmp_path, "assembly")
    }
    assert late not in {
        (row["accession"], row["pipeline_version"])
        for row in dims(tmp_path, "assembly")
    }

    run(tmp_path, D2)
    assert in_tier2(tmp_path) == joined(expected, waiting)
    assert late in {
        (row["accession"], row["pipeline_version"])
        for row in dims(tmp_path, "assembly")
    }
    assert staged() == {table: [] for table in TIER2_COLUMNS}


def test_a_day_with_nothing_staged_is_complete(tmp_path, stage):
    run(tmp_path, D1)
    assert tier2.complete_days(tmp_path) == [D1]
    assert in_tier2(tmp_path) == {table: [] for table in TIER2_COLUMNS}
    assert set(listing(tmp_path)) == layout(D1, [])


def protein_files(root):
    return sorted(str(p.relative_to(root)) for p in root.glob("protein/*/*.parquet"))


def prefixes(proteins):
    return {tier2.prefix(hash) for _, hash, _ in proteins}


def test_compaction_merges_each_due_prefix_into_one_base(tmp_path, stage):
    stage("ERZ101")
    day1 = staged()
    run(tmp_path, D1)
    stage("ERZ29562087")
    stage("ERZ25069264")
    day31 = staged()
    run(tmp_path, D31)

    assert in_tier2(tmp_path) == joined(day1, day31)
    old = prefixes(day1["protein"])
    assert protein_files(tmp_path) == sorted(
        [f"protein/prefix={p}/base-{D31}.parquet" for p in old]
        + [
            f"protein/prefix={p}/part-{D31}.parquet"
            for p in prefixes(day31["protein"]) - old
        ]
    )
    for path in tier2.files(tmp_path, "protein"):
        stored = pq.read_table(path)
        assert stored.schema == tier2.SCHEMAS["protein"]
        assert stored["hash"].to_pylist() == sorted(stored["hash"].to_pylist())
    assert query(
        "SELECT ingest_date, compacted FROM proteindb.load_log ORDER BY id"
    ) == [(D1, False), (D31, True)]
    with pytest.raises(tier2.Tier2Error, match="oldest base"):
        tier2.files(tmp_path, "protein", D1)


def test_prefixes_are_not_compacted_within_30_days(tmp_path, stage):
    stage("ERZ101")
    run(tmp_path, D1)
    stage("ERZ29562087")
    run(tmp_path, D1 + timedelta(days=30))
    assert not [f for f in protein_files(tmp_path) if "base-" in f]
    assert query("SELECT compacted FROM proteindb.load_log") == [(False,), (False,)]


@pytest.mark.parametrize(
    "error", [RuntimeError("crash"), Killed()], ids=["error", "kill"]
)
@pytest.mark.parametrize("when", ["before", "after"])
def test_a_crash_during_compaction_never_changes_what_readers_see(
    tmp_path, stage, monkeypatch, when, error
):
    stage("ERZ101")
    day1 = staged()
    run(tmp_path, D1)
    stage("ERZ29562087")
    expected = joined(day1, staged())
    write_sorted = tier2.write_sorted
    bases = []

    def crash_on_the_second_base(path, *args):
        if path.name.startswith("base-"):
            bases.append(path)
            if len(bases) == 2:
                if when == "after":
                    write_sorted(path, *args)
                raise error
        write_sorted(path, *args)

    monkeypatch.setattr(tier2, "write_sorted", crash_on_the_second_base)
    with pytest.raises(type(error)):
        run(tmp_path, D31)
    monkeypatch.undo()

    assert in_tier2(tmp_path) == expected
    assert query(
        f"SELECT status, compacted, message FROM proteindb.load_log WHERE ingest_date = '{D31}'"
    ) == [("done", False, None if isinstance(error, Killed) else "RuntimeError: crash")]
    before = listing(tmp_path)
    assert run(tmp_path, D31) is None
    after = listing(tmp_path)
    assert {path: before[path] for path in after} == after
    assert in_tier2(tmp_path) == expected

    run(tmp_path, D32)
    assert in_tier2(tmp_path) == expected
    for prefix in prefixes(day1["protein"]):
        (only,) = (tmp_path / "protein" / f"prefix={prefix}").iterdir()
        assert only.name.startswith("base-")


def test_compaction_stops_at_its_time_budget_and_the_next_load_continues(
    tmp_path, stage, monkeypatch
):
    stage("ERZ101")
    day1 = staged()
    run(tmp_path, D1)
    clock = [0.0]
    compact_prefix = load_module.compact_prefix

    def five_hours_each(*args):
        compact_prefix(*args)
        clock[0] += 5 * 3600

    monkeypatch.setattr(load_module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(load_module, "compact_prefix", five_hours_each)
    run(tmp_path, D31)
    monkeypatch.undo()

    assert len([f for f in protein_files(tmp_path) if "base-" in f]) == 2
    assert query("SELECT compacted FROM proteindb.load_log ORDER BY id") == [
        (False,),
        (True,),
    ]
    run(tmp_path, D32)
    assert protein_files(tmp_path) == sorted(
        f"protein/prefix={p}/base-{D31 if n < 2 else D32}.parquet"
        for n, p in enumerate(sorted(prefixes(day1["protein"])))
    )
    assert in_tier2(tmp_path) == day1


def test_a_compacted_base_is_sorted_in_full_row_groups(tmp_path):
    def proteins(ids):
        return pa.table(
            {
                "id": ids,
                "hash": [
                    bytes([0]) + hashlib.sha256(str(n).encode()).digest()[1:]
                    for n in ids
                ],
                "sequence": ["M"] * len(ids),
            }
        )

    prefix = tmp_path / "prefix=00"
    tier2.write(prefix / f"base-{D1}.parquet", "protein", proteins(range(150_000)))
    tier2.write(
        prefix / f"part-{D2}.parquet", "protein", proteins(range(150_000, 250_000))
    )
    load_module.compact_prefix(prefix, sorted(prefix.iterdir()), D31)

    assert [p.name for p in prefix.iterdir()] == [f"base-{D31}.parquet"]
    path = prefix / f"base-{D31}.parquet"
    stored = pq.read_table(path)
    assert stored.schema == tier2.SCHEMAS["protein"]
    assert sorted(stored["id"].to_pylist()) == list(range(250_000))
    assert stored["hash"].to_pylist() == sorted(stored["hash"].to_pylist())
    metadata = pq.ParquetFile(path).metadata
    assert [metadata.row_group(g).num_rows for g in range(metadata.num_row_groups)] == [
        tier2.ROW_GROUP_SIZE,
        tier2.ROW_GROUP_SIZE,
        250_000 - 2 * tier2.ROW_GROUP_SIZE,
    ]
