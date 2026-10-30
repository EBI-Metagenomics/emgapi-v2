import shutil
from datetime import date, datetime, timezone
from functools import cache
from unittest.mock import patch
from uuid import uuid4

import psycopg
import pyarrow.parquet as pq
import pytest

from analyses.models import Study
from proteins.accession.accession import accession
from proteins.accession.inputs import read_input
from proteins.dataset import layout, merge, update
from proteins.dataset.build import build_mgyp_database
from proteins.dataset.owner import OwnershipError, take
from proteins.routers import MGYP
from proteins.tests.conftest import (
    FIXTURES,
    SCHEMA_SQL,
    SUBMITTED,
    Killed,
    create_limit,
    make_analysis,
    make_study,
    owner_dsn,
    query,
    read_published,
)
from workflows.flows.proteins import merge_protein_dataset as merge_flow
from workflows.flows.proteins import update_protein_dataset as flows

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("mgyp_as_accession", "prefect_harness", "slurm"),
]

D0, D1, D2, D3 = (date(2026, 9, 30), *(date(2026, 10, n) for n in (1, 2, 3)))
TABLES = ("protein", "contig", "occurrence")


@cache
def read(assembly):
    if assembly == "ERZ101":
        return read_input(
            *(FIXTURES / f"ERZ101.{suffix}" for suffix in ("faa.gz", "gff.gz")),
            FIXTURES / "ERZ101.fasta.gz",
            FIXTURES / "ERZ101_mapping.csv",
        )
    return read_published(assembly)


@pytest.fixture
def study():
    return make_study("PRJ1")


@pytest.fixture
def stage(connect_accession, study):
    """Accessions an assembly for a new analysis of it, by default one emgapi-v2 knows."""

    def stage(assembly, version="V6", known=True):
        analysis = make_analysis(study, assembly, version)
        # Few connections, so that 8 xdist workers stay within the server's limit.
        accession(
            connect_accession,
            assembly,
            "assembly_analysis",
            analysis.pipeline_major_minor,
            read(assembly),
            analysis.accession,
            connections=4,
        )
        if not known:
            id = analysis.id
            analysis.delete()
            analysis.id = id
        return analysis

    return stage


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(flows.EMG_CONFIG.proteins, "dataset_dir", str(tmp_path))
    monkeypatch.setattr(
        flows.EMG_CONFIG.proteins, "dataset_concurrency_limit", create_limit()
    )
    return tmp_path


def run(day):
    with patch.object(flows, "datetime") as now:
        now.now.return_value = datetime(
            day.year, day.month, day.day, 2, tzinfo=timezone.utc
        )
        return flows.update_protein_dataset()


def staged():
    return {
        table: sorted(
            query(
                f"SELECT {', '.join(layout.SCHEMAS[table].names)}"
                f" FROM proteindb.staged_{table}"
            ),
            key=repr,
        )
        for table in TABLES
    }


NOTHING_STAGED = {table: [] for table in TABLES}


def in_dataset(root, as_of=None):
    return {
        table: sorted(
            (
                tuple(row.values())
                for path in layout.files(root, table, as_of)
                for row in pq.read_table(path).to_pylist()
            ),
            key=repr,
        )
        for table in TABLES
    }


def joined(*days):
    return {
        table: sorted((row for day in days for row in day[table]), key=repr)
        for table in TABLES
    }


def listing(root):
    """Every file under root, with what would change if it were rewritten."""
    return {
        str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def day_layout(day, proteins):
    """The files an uninterrupted update writes for `day`."""
    return {
        *(
            f"protein/prefix={layout.prefix(hash)}/day-{day}.parquet"
            for _, hash, _ in proteins
        ),
        f"occurrence/day={day}/part-000.parquet",
        f"contig/day={day}/part-000.parquet",
        f"metadata/day={day}/assembly_proteins.parquet",
        f"metadata/day={day}/assembly_proteins_registry.parquet",
        f"metadata/day={day}/gene_callers.parquet",
    }


def snapshot(root, as_of=None):
    (path,) = layout.files(root, "metadata/assembly_proteins", as_of)
    return {
        (row["assembly_accession"], row["pipeline_version"], row["analysis_accession"])
        for row in pq.read_table(path).to_pylist()
    }


def records(analyses):
    return {
        (a.assembly.first_accession, a.pipeline_major_minor, a.accession)
        for a in analyses
    }


def statuses():
    return query("SELECT day, status FROM proteindb.dataset_update ORDER BY id")


def test_two_days_of_updates_hold_every_staged_row_once(root, stage):
    first = [stage("ERZ101"), stage("ERZ29562087")]
    day1 = staged()
    run(D1)
    second = [stage("ERZ25069264"), stage("ERZ29895170"), stage("ERZ101", "V6.1")]
    day2 = staged()
    run(D2)

    assert in_dataset(root) == joined(day1, day2)
    assert in_dataset(root, D1) == day1
    assert staged() == NOTHING_STAGED
    for path in layout.files(root, "protein"):
        assert {
            layout.prefix(hash) for hash in pq.read_table(path)["hash"].to_pylist()
        } == {path.parent.name.removeprefix("prefix=")}
    assert set(listing(root)) == day_layout(D1, day1["protein"]) | day_layout(
        D2, day2["protein"]
    )
    assert snapshot(root) == records(first + second)
    assert snapshot(root, D1) == records(first)
    assert query(
        "SELECT day, status, assembly_proteins, proteins, contigs, occurrences"
        " FROM proteindb.dataset_update ORDER BY id"
    ) == [
        (D1, "done", 2, *(len(day1[t]) for t in TABLES)),
        (D2, "done", 3, *(len(day2[t]) for t in TABLES)),
    ]


def test_a_day_with_nothing_staged_is_complete(root):
    run(D1)
    assert layout.complete_days(root) == [D1]
    assert in_dataset(root) == NOTHING_STAGED
    assert set(listing(root)) == day_layout(D1, [])


def test_a_second_update_on_the_same_date_writes_nothing(root, stage):
    stage("ERZ101")
    run(D1)
    before = listing(root)
    stage("ERZ29562087")
    waiting = staged()

    run(D1)
    assert listing(root) == before
    assert staged() == waiting
    assert statuses() == [(D1, "done")]


STEPS = [
    "read_staged_rows",
    "write_day_files",
    "write_metadata_snapshot",
    "finish_update",
    "mark_day_complete",
]


@pytest.mark.parametrize(
    "error", [RuntimeError("crash"), Killed()], ids=["error", "kill"]
)
@pytest.mark.parametrize("when", ["before", "after"])
@pytest.mark.parametrize("step", STEPS)
def test_a_crash_at_each_step_ends_as_an_uninterrupted_update(
    root, stage, step, when, error
):
    stage("ERZ101")
    stage("ERZ29562087")
    expected = staged()
    original = getattr(update, step)

    def crashing(*args, **kwargs):
        if when == "after":
            original(*args, **kwargs)
        raise error

    with patch.object(update, step, crashing), pytest.raises(type(error)):
        run(D1)
    committed = step == "mark_day_complete" or (step, when) == (
        "finish_update",
        "after",
    )
    if committed:
        assert statuses() == [(D1, "done")]
    elif isinstance(error, Killed):
        assert statuses() == [(D1, "running")]
    else:
        assert query("SELECT status, message FROM proteindb.dataset_update") == [
            ("failed", "RuntimeError: crash")
        ]
    written = {
        path: stat for path, stat in listing(root).items() if "metadata" not in path
    }

    run(D1)

    assert in_dataset(root) == expected
    assert staged() == NOTHING_STAGED
    assert set(listing(root)) == day_layout(D1, expected["protein"])
    assert [status for _, status in statuses()] == (
        ["done"] if committed else ["failed", "done"]
    )
    if committed:
        # The committed day's files are the first run's.
        assert {
            path: stat for path, stat in listing(root).items() if path in written
        } == written

    after = listing(root)
    run(D1)
    assert listing(root) == after


def test_an_update_whose_row_was_marked_failed_does_not_commit(root, stage):
    stage("ERZ101")
    expected = staged()
    write_day_files = update.write_day_files

    def write_then_lose_the_row(*args):
        counts = write_day_files(*args)
        query("UPDATE proteindb.dataset_update SET status = 'failed'")
        return counts

    with (
        patch.object(update, "write_day_files", write_then_lose_the_row),
        pytest.raises(update.DatasetUpdateError, match="no longer running"),
    ):
        run(D1)
    assert staged() == expected
    assert layout.complete_days(root) == []


def test_a_rename_that_always_fails_deletes_nothing(root, stage):
    stage("ERZ101")
    expected = staged()

    def failing(root, day):
        raise OSError("rename failed")

    with patch.object(update, "mark_day_complete", failing):
        with pytest.raises(OSError):
            run(D1)
        before = listing(root)
        with pytest.raises(OSError):
            run(D1)
    assert statuses() == [(D1, "done")]
    assert listing(root) == before

    run(D1)
    assert in_dataset(root) == expected


def test_a_done_day_without_its_snapshot_fails_and_names_the_day(root, stage):
    stage("ERZ101")
    with (
        patch.object(update, "mark_day_complete", side_effect=Killed),
        pytest.raises(Killed),
    ):
        run(D1)
    shutil.rmtree(root / "metadata" / f".tmp-day={D1}")

    for day in (D1, D2):
        with pytest.raises(update.DatasetUpdateError, match=str(D1)):
            run(day)


def test_a_complete_day_not_done_stops_the_update_and_names_the_day(root, stage):
    stage("ERZ101")
    run(D1)
    # As after a restore of the MGYP database to before the update of D0.
    (root / "metadata" / f"day={D0}").mkdir()
    stage("ERZ29562087")
    before, waiting = listing(root), staged()

    with pytest.raises(update.DatasetUpdateError, match=str(D0)):
        run(D2)
    assert listing(root) == before
    assert staged() == waiting
    assert statuses() == [(D1, "done")]


def test_leftovers_of_failed_updates_are_deleted(root, stage):
    stage("ERZ101")
    run(D1)
    before = listing(root)
    for leftover in (
        f"protein/prefix=00/day-{D2}.parquet",
        f"protein/prefix=00/.day-{D2}.parquet.tmp",
        f"occurrence/day={D2}/part-000.parquet",
        f"contig/day={D2}/.part-000.parquet.tmp",
        f"metadata/.tmp-day={D2}/assembly_proteins.parquet",
        f"occurrence/day={D1}/.part-000.parquet.tmp",
    ):
        (root / leftover).parent.mkdir(parents=True, exist_ok=True)
        (root / leftover).touch()

    run(D1)
    assert listing(root) == before


@pytest.mark.parametrize(
    "late",
    [("ERZ101", "V6.1"), ("ERZ29562087", "V6")],
    ids=["known-proteins", "new-protein"],
)
def test_an_analysis_committed_while_an_update_reads_waits_for_the_next(
    root, stage, late
):
    first = stage("ERZ101")
    expected = staged()
    choose = update.choose_assembly_proteins
    committed = []

    def choose_then_commit_another(cursor):
        chosen = choose(cursor)
        committed.append(stage(*late))
        return chosen

    with patch.object(update, "choose_assembly_proteins", choose_then_commit_another):
        run(D1)

    assert in_dataset(root) == expected
    waiting = staged()
    assert waiting["occurrence"]
    assert {mgya for _, _, mgya in snapshot(root)} == {first.accession}

    run(D2)
    assert in_dataset(root) == joined(expected, waiting)
    assert {mgya for _, _, mgya in snapshot(root)} == {
        first.accession,
        committed[0].accession,
    }
    assert staged() == NOTHING_STAGED


@pytest.mark.parametrize(
    "late",
    [("ERZ101", "V6.1"), ("ERZ29562087", "V6")],
    ids=["known-proteins", "new-protein"],
)
def test_a_rebuild_after_an_analysis_committed_while_an_update_read_stages_it_once(
    root, stage, late, connect_accession
):
    stage("ERZ101")
    choose = update.choose_assembly_proteins
    committed = []

    def choose_then_commit_another(cursor):
        chosen = choose(cursor)
        committed.append(stage(*late))
        return chosen

    with patch.object(update, "choose_assembly_proteins", choose_then_commit_another):
        run(D1)
    expected, waiting = in_dataset(root), staged()
    ((last,),) = query("SELECT last_value FROM proteindb.protein_id_seq")

    # The MGYP database is lost, with the late analysis's record, and rebuilt from day D1.
    with psycopg.connect(owner_dsn(), autocommit=True) as conn:
        conn.execute("DROP SCHEMA proteindb CASCADE")
        conn.execute(SCHEMA_SQL.read_text())
    build_mgyp_database(owner_dsn(), root, rebuild=True, protein_id_start=last + 1)
    analysis = committed[0]
    accession(
        connect_accession,
        late[0],
        "assembly_analysis",
        analysis.pipeline_major_minor,
        read(late[0]),
        analysis.accession,
        connections=4,
    )
    assert {table: len(rows) for table, rows in staged().items()} == {
        table: len(rows) for table, rows in waiting.items()
    }

    run(D2)
    assert {table: len(rows) for table, rows in in_dataset(root).items()} == {
        table: len(expected[table]) + len(waiting[table]) for table in TABLES
    }
    assert staged() == NOTHING_STAGED


def test_a_record_whose_mgya_is_not_in_emgapi_is_withheld_and_the_rest_loaded(
    root, stage, study
):
    known = stage("ERZ101")
    unknown = stage("ERZ29562087", known=False)
    expected = staged()

    run(D1)
    assert in_dataset(root) == expected
    assert {mgya for _, _, mgya in snapshot(root)} == {known.accession}

    # Back in the next day's snapshot once emgapi-v2 has the analysis again.
    unknown.save(force_insert=True)
    run(D2)
    assert {mgya for _, _, mgya in snapshot(root)} == {
        known.accession,
        unknown.accession,
    }


def test_a_rebuild_restores_a_withheld_record_and_never_gives_its_id_to_another(
    root, stage, connect_accession
):
    stage("ERZ101")
    withheld = stage("ERZ29562087", known=False)
    run(D1)
    before = in_dataset(root)
    ((last,),) = query("SELECT last_value FROM proteindb.protein_id_seq")
    ((withheld_id,),) = query(
        "SELECT id FROM proteindb.assembly_proteins WHERE analysis_accession = %s",
        [withheld.accession],
    )

    # The MGYP database is lost and rebuilt from day D1, whose snapshot withholds the record.
    with psycopg.connect(owner_dsn(), autocommit=True) as conn:
        conn.execute("DROP SCHEMA proteindb CASCADE")
        conn.execute(SCHEMA_SQL.read_text())
    build_mgyp_database(owner_dsn(), root, rebuild=True, protein_id_start=last + 1)
    unrelated = stage("ERZ25069264")
    withheld.save(force_insert=True)
    accession(
        connect_accession,
        "ERZ29562087",
        "assembly_analysis",
        withheld.pipeline_major_minor,
        read("ERZ29562087"),
        withheld.accession,
        connections=4,
    )
    added = staged()

    run(D2)
    ids = dict(query("SELECT analysis_accession, id FROM proteindb.assembly_proteins"))
    assert ids[withheld.accession] == withheld_id != ids[unrelated.accession]
    (path,) = layout.files(root, "metadata/assembly_proteins")
    assert (withheld_id, withheld.accession) in {
        (row["assembly_proteins_id"], row["analysis_accession"])
        for row in pq.read_table(path).to_pylist()
    }
    assert in_dataset(root) == joined(before, added)


def test_a_flag_changed_in_emgapi_is_in_the_next_snapshot_both_ways(root, stage, study):
    stage("ERZ101")
    privacy = []
    for day, private in ((D1, False), (D2, True), (D3, False)):
        Study.objects.filter(pk=study.pk).update(is_private=private)
        run(day)
        (path,) = layout.files(root, "metadata/assembly_proteins")
        privacy += pq.read_table(path)["is_private"].to_pylist()
    assert privacy == [False, True, False]


@pytest.mark.parametrize(
    "slots, active, error",
    [(None, True, "does not exist"), (1, False, "inactive"), (2, True, "with 2")],
    ids=["missing", "inactive", "two-slots"],
)
@pytest.mark.parametrize("flow", ["update", "merge"])
def test_a_limit_that_is_not_one_active_slot_stops_the_flow_first(
    root, stage, monkeypatch, flow, slots, active, error
):
    stage("ERZ101")
    waiting = staged()
    name = create_limit(slots, active) if slots else f"protein-dataset-{uuid4()}"
    monkeypatch.setattr(flows.EMG_CONFIG.proteins, "dataset_concurrency_limit", name)

    with pytest.raises(OwnershipError, match=error):
        run(D1) if flow == "update" else merge_flow.merge_protein_dataset()
    assert statuses() == []
    assert staged() == waiting
    assert listing(root) == {}


def lose_ownership_after(module, step, root, lost):
    """Patches a step to give ownership to another execution once it has run, and records the files then."""
    original = getattr(module, step)

    def then_lose_ownership(*args):
        result = original(*args)
        query("UPDATE proteindb.dataset_owner SET execution_id = gen_random_uuid()")
        lost.update(listing(root))
        return result

    return patch.object(module, step, then_lose_ownership)


@pytest.mark.parametrize(
    "step, error, status",
    [
        ("read_staged_rows", Killed, "running"),
        ("write_day_files", Killed, "running"),
        ("write_metadata_snapshot", OwnershipError, "failed"),
        ("finish_update", Killed, "done"),
    ],
)
def test_an_update_that_loses_ownership_writes_nothing_more(
    root, stage, slurm, exits, step, error, status
):
    sacct, as_job = slurm
    stage("ERZ101")
    expected = staged()
    as_job(1)
    lost = {}
    with lose_ownership_after(update, step, root, lost), pytest.raises(error):
        run(D1)

    assert listing(root) == lost
    assert statuses() == [(D1, status)]
    if status != "done":
        assert staged() == expected
    # The other execution was job 1's too, and its job has ended.
    sacct[1] = f"COMPLETED|{SUBMITTED[1]}\n"
    as_job(2)
    run(D1)
    assert in_dataset(root) == expected


def test_a_merge_that_loses_ownership_writes_nothing_more(root, stage, slurm, exits):
    sacct, as_job = slurm
    stage("ERZ101")
    run(D1)
    stage("ERZ29562087")
    run(D2)
    expected = in_dataset(root)
    as_job(1)
    lost = {}
    with lose_ownership_after(merge, "merge_prefix", root, lost), pytest.raises(Killed):
        merge_flow.merge_protein_dataset()

    assert listing(root) == lost
    assert in_dataset(root) == expected
    sacct[1] = f"COMPLETED|{SUBMITTED[1]}\n"
    as_job(2)
    merge_flow.merge_protein_dataset()
    assert in_dataset(root) == expected
    assert not list(root.glob("protein/*/day-*"))


@pytest.mark.parametrize("flow", ["update", "merge"])
def test_a_flow_whose_previous_owner_may_run_touches_nothing(root, stage, slurm, flow):
    sacct, as_job = slurm
    stage("ERZ101")
    waiting = staged()
    as_job(1)
    take("update")  # and killed: nothing releases it
    sacct[1] = f"RUNNING|{SUBMITTED[1]}\n"
    as_job(2)

    with pytest.raises(OwnershipError, match="owned by job 1,"):
        run(D1) if flow == "update" else merge_flow.merge_protein_dataset()
    assert statuses() == []
    assert staged() == waiting
    assert listing(root) == {}
