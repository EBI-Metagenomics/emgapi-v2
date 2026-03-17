import logging
import os
from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest

from workflows.flows.housekeeping.clean_nextflow_workdirs import (
    youngest_mtime_in_dir,
    collect_old_workdir_candidates,
    delete_workdirs,
)


def make_dir_with_mtime(path: Path, age_days: float) -> Path:
    """Create a directory and backdate its mtime to age_days ago.

    :param path: Directory path to create.
    :param age_days: How many days ago to set the mtime.
    :return: The created path.
    :rtype: Path
    """
    path.mkdir(parents=True, exist_ok=True)
    mtime = (pendulum.now() - pendulum.duration(days=age_days)).timestamp()
    os.utime(path, times=(mtime, mtime))
    return path


class TestYoungestMtimeInDir:
    """Tests for the youngest_mtime_in_dir helper."""

    def test_old_dir_returns_old_age(self, tmp_path: Path):
        """A directory with no young subdirs should return an age >= its mtime age."""
        old_dir = make_dir_with_mtime(tmp_path / "old", age_days=100)
        now = pendulum.now()
        age = youngest_mtime_in_dir(old_dir, min_age=pendulum.duration(days=2), now=now)
        assert age.in_days() >= 99

    def test_young_dir_triggers_early_exit(self, tmp_path: Path):
        """A directory whose mtime is recent should return an age below the threshold."""
        young_dir = make_dir_with_mtime(tmp_path / "young", age_days=0.0001)
        now = pendulum.now()
        min_age = pendulum.duration(days=2)
        age = youngest_mtime_in_dir(young_dir, min_age=min_age, now=now)
        assert age < min_age

    def test_young_subdir_triggers_early_exit(self, tmp_path: Path):
        """A young nested subdir should cause the whole tree to be excluded."""
        root = make_dir_with_mtime(tmp_path / "root", age_days=100)
        make_dir_with_mtime(root / "recent_subdir", age_days=0.0001)
        now = pendulum.now()
        min_age = pendulum.duration(days=2)
        age = youngest_mtime_in_dir(root, min_age=min_age, now=now)
        assert age < min_age

    def test_all_old_subdirs_return_old_age(self, tmp_path: Path):
        """A tree where every subdir is old should return an old age."""
        root = tmp_path / "root"
        root.mkdir()
        make_dir_with_mtime(root / "sub_a", age_days=60)
        make_dir_with_mtime(root / "sub_b", age_days=70)
        # Backdate root after children are created — creating sub_a/sub_b
        # would otherwise reset root's mtime to now.
        make_dir_with_mtime(root, age_days=50)
        now = pendulum.now()
        age = youngest_mtime_in_dir(
            str(root), min_age=pendulum.duration(days=2), now=now
        )
        # youngest is root itself at ~50 days
        assert age.in_days() >= 49


@pytest.fixture()
def mock_prefect():
    """Patch Prefect-specific calls so tasks can be invoked outside a Prefect context."""
    with (
        patch(
            "workflows.flows.housekeeping.clean_nextflow_workdirs.get_run_logger",
            return_value=logging.getLogger("test"),
        ),
        patch(
            "workflows.flows.housekeeping.clean_nextflow_workdirs.create_table_artifact"
        ),
    ):
        yield


class TestCollectOldWorkdirCandidates:
    """Tests for collect_old_workdir_candidates task logic."""

    def test_old_dir_is_returned(self, tmp_path: Path, mock_prefect):
        """An old directory at the exact scan depth should appear in candidates."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "dir_old", age_days=100)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 1)

        assert len(candidates) == 1
        assert candidates[0]["path"] == str(results / "dir_old")

    def test_young_dir_is_excluded(self, tmp_path: Path, mock_prefect):
        """A recently modified directory should not appear in candidates."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "dir_young", age_days=1)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 1)

        assert candidates == []

    def test_dirs_at_wrong_depth_are_excluded(self, tmp_path: Path, mock_prefect):
        """Directories deeper than scan_depth should not be collected."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "dir_old", age_days=100)
        make_dir_with_mtime(results / "subdir" / "dir_nested_old", age_days=100)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 1)

        paths = [c["path"] for c in candidates]
        assert str(results / "dir_old") in paths
        assert str(results / "subdir" / "dir_nested_old") not in paths

    def test_scan_depth_2_finds_nested_dirs(self, tmp_path: Path, mock_prefect):
        """With scan_depth=2, directories two levels deep should be collected."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "parent" / "dir_old", age_days=100)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 2)

        paths = [c["path"] for c in candidates]
        assert str(results / "parent" / "dir_old") in paths

    def test_dir_with_young_subdir_is_excluded(self, tmp_path: Path, mock_prefect):
        """A directory tree containing any young subdir should not be a candidate."""
        results = tmp_path / "results"
        old_root = make_dir_with_mtime(results / "dir_old_root", age_days=100)
        make_dir_with_mtime(old_root / "recent_child", age_days=1)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 1)

        assert candidates == []

    def test_ignore_patterns_exclude_matching_dirs(self, tmp_path: Path, mock_prefect):
        """Directories whose basename matches an ignore pattern should be skipped."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "keep_this", age_days=100)
        make_dir_with_mtime(results / "delete_this", age_days=100)

        candidates = collect_old_workdir_candidates.fn(
            str(results), 30, 1, ignore_patterns=["keep_*"]
        )

        paths = [c["path"] for c in candidates]
        assert str(results / "delete_this") in paths
        assert str(results / "keep_this") not in paths

    def test_candidate_dict_has_required_fields(self, tmp_path: Path, mock_prefect):
        """Each candidate dict must contain the expected keys."""
        results = tmp_path / "results"
        make_dir_with_mtime(results / "dir_old", age_days=100)

        candidates = collect_old_workdir_candidates.fn(str(results), 30, 1)

        assert len(candidates) == 1
        c = candidates[0]
        for field in ("path", "created", "last_modified", "age_days"):
            assert field in c, f"Missing field: {field}"
        assert c["age_days"] >= 30


class TestDeleteWorkdirs:
    """Tests for delete_workdirs task logic."""

    def test_deletes_directory(self, tmp_path: Path, mock_prefect):
        """A listed directory should be removed from the filesystem."""
        target = tmp_path / "to_delete"
        target.mkdir()
        (target / "file.txt").touch()

        count = delete_workdirs.fn([{"path": str(target)}])

        assert count == 1
        assert not target.exists()

    def test_returns_count_of_successful_deletions(self, tmp_path: Path, mock_prefect):
        """Deleted count should reflect only successful deletions."""
        d1 = tmp_path / "d1"
        d1.mkdir()
        d2 = tmp_path / "d2"
        d2.mkdir()

        count = delete_workdirs.fn([{"path": str(d1)}, {"path": str(d2)}])

        assert count == 2

    def test_continues_after_failed_deletion(self, tmp_path: Path, mock_prefect):
        """A missing directory should be logged but should not prevent subsequent deletions."""
        missing = tmp_path / "does_not_exist"
        real = tmp_path / "real"
        real.mkdir()

        count = delete_workdirs.fn([{"path": str(missing)}, {"path": str(real)}])

        assert count == 1
        assert not real.exists()
