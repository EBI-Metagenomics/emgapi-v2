from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from proteins.dataset import layout, merge

DAY1, DAY2, DAY3, DAY4 = (date(2026, 9, n) for n in range(1, 5))
# Ownership is tested through the flows, in test_update and test_owner.
OWNED = SimpleNamespace(check=lambda: None)


def write_proteins(root, prefix, name, ids):
    """Proteins whose hashes fall in a prefix, each id's sequence M<id>."""
    first = int(prefix, 16) << 2
    layout.write(
        root / "protein" / f"prefix={prefix}" / f"{name}.parquet",
        "protein",
        pa.table(
            {
                "id": ids,
                "hash": [bytes([first]) + id.to_bytes(31, "big") for id in ids],
                "sequence": [f"M{id}" for id in ids],
            }
        ),
    )


def complete(root, day):
    (root / "metadata" / f"day={day}").mkdir(parents=True)


def build(root):
    """Days 1 to 3 complete and day 4 not. Prefix 00 has a base and three day files, 01 only
    a day file, 02 only a base."""
    write_proteins(root, "00", f"base-{DAY1}", [2, 1])
    write_proteins(root, "00", f"day-{DAY2}", [3])
    write_proteins(root, "00", f"day-{DAY3}", [4])
    write_proteins(root, "00", f"day-{DAY4}", [5])
    write_proteins(root, "01", f"day-{DAY2}", [6])
    write_proteins(root, "02", f"base-{DAY1}", [7])
    for day in DAY2, DAY4:
        part = root / "occurrence" / f"day={day}" / "part-000.parquet"
        part.parent.mkdir(parents=True)
        part.touch()
    for day in DAY1, DAY2, DAY3:
        complete(root, day)
    return root


@pytest.fixture
def dataset(tmp_path):
    return build(tmp_path / "dataset")


def names(root, table, as_of=None):
    return [str(f.relative_to(root)) for f in layout.files(root, table, as_of)]


def ids(root, as_of=None):
    return sorted(
        id
        for f in layout.files(root, "protein", as_of)
        for id in pq.read_table(f)["id"].to_pylist()
    )


def state(root):
    """Every protein file of the dataset, with its rows."""
    return {
        str(f.relative_to(root)): pq.read_table(f).to_pylist()
        for f in sorted((root / "protein").rglob("*"))
        if f.is_file()
    }


def test_files_are_those_of_complete_days_up_to_as_of(dataset):
    assert names(dataset, "protein", DAY1) == [
        f"protein/prefix=00/base-{DAY1}.parquet",
        f"protein/prefix=02/base-{DAY1}.parquet",
    ]
    assert names(dataset, "protein") == [
        f"protein/prefix=00/base-{DAY1}.parquet",
        f"protein/prefix=00/day-{DAY2}.parquet",
        f"protein/prefix=00/day-{DAY3}.parquet",
        f"protein/prefix=01/day-{DAY2}.parquet",
        f"protein/prefix=02/base-{DAY1}.parquet",
    ]
    assert names(dataset, "occurrence") == [f"occurrence/day={DAY2}/part-000.parquet"]
    assert names(dataset, "metadata/assembly_proteins", DAY2) == [
        f"metadata/day={DAY2}/assembly_proteins.parquet"
    ]


def test_files_refuse_an_incomplete_day_an_unknown_table_and_no_complete_day(
    dataset, tmp_path
):
    with pytest.raises(layout.DatasetError, match="not a complete day"):
        layout.files(dataset, "protein", DAY4)
    with pytest.raises(ValueError, match="unknown"):
        layout.files(dataset, "proteins")
    with pytest.raises(layout.DatasetError, match="no complete day"):
        layout.files(tmp_path, "protein")


def test_files_refuse_a_day_older_than_the_oldest_base(dataset):
    merge.merge_dataset(dataset, OWNED)
    assert ids(dataset, DAY3) == [1, 2, 3, 4, 6, 7]
    with pytest.raises(layout.DatasetError, match="older than the oldest base"):
        layout.files(dataset, "protein", DAY2)


def test_merge_writes_one_base_per_prefix_and_keeps_incomplete_days(dataset):
    before = ids(dataset)
    assert merge.merge_dataset(dataset, OWNED) == 2
    assert sorted(state(dataset)) == [
        f"protein/prefix=00/base-{DAY3}.parquet",
        f"protein/prefix=00/day-{DAY4}.parquet",
        f"protein/prefix=01/base-{DAY3}.parquet",
        f"protein/prefix=02/base-{DAY1}.parquet",
    ]
    assert ids(dataset) == before
    base = pq.read_table(dataset / "protein" / "prefix=00" / f"base-{DAY3}.parquet")
    assert base["id"].to_pylist() == [1, 2, 3, 4]
    assert base.schema == layout.SCHEMAS["protein"]

    complete(dataset, DAY4)
    assert ids(dataset) == [*before[:4], 5, *before[4:]]


def test_a_second_merge_changes_nothing(dataset):
    merge.merge_dataset(dataset, OWNED)
    merged = state(dataset)
    merge.clean_up_interrupted_merge(dataset)
    assert merge.merge_dataset(dataset, OWNED) == 0
    assert state(dataset) == merged


class Crash(Exception):
    pass


def test_a_crash_at_each_step_of_the_merge(tmp_path):
    """Each rename and delete of the merge, crashed in turn: readers see the same rows after
    the crash and after the clean-up, and the rerun ends as an uninterrupted merge."""
    root = build(tmp_path / "uninterrupted")
    before = ids(root)
    merge.merge_dataset(root, OWNED)
    uninterrupted = state(root)

    rename, unlink = Path.rename, Path.unlink
    for step in range(10):
        root = build(tmp_path / f"crash-{step}")
        calls = 0

        def crash_at_step(original):
            def operation(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls > step:
                    raise Crash
                return original(*args, **kwargs)

            return operation

        try:
            with (
                patch.object(Path, "rename", crash_at_step(rename)),
                patch.object(Path, "unlink", crash_at_step(unlink)),
            ):
                merge.merge_dataset(root, OWNED)
        except Crash:
            pass
        else:
            break
        assert ids(root) == before, step
        merge.clean_up_interrupted_merge(root)
        assert ids(root) == before, step
        merge.merge_dataset(root, OWNED)
        assert state(root) == uninterrupted, step
    # Each prefix: the base's rename, then deleting what it merged.
    assert step == (1 + 3) + (1 + 1)
