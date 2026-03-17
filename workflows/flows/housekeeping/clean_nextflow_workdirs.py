import fnmatch
import glob as glob_module
import logging
import os
import shutil
from textwrap import dedent

import pendulum

from prefect import flow, suspend_flow_run, task
from prefect.artifacts import create_table_artifact
from prefect.input import RunInput
from pydantic import Field

logger = logging.getLogger(__name__)


def youngest_mtime_in_dir(
    dir_path: str, min_age: pendulum.Duration, now: pendulum.DateTime
) -> pendulum.Duration:
    """Walk all subdirectories of dir_path; return age of the most recently modified entry.

    Uses ``os.walk`` so only directory modification times are considered — a directory's
    mtime is updated when files are created or removed inside it, which is used as a
    proxy for Nextflow cache freshness. Short-circuits as soon as any subdirectory is found
    to be younger than ``min_age``.

    :param dir_path: Directory to walk.
    :param min_age: Early-exit threshold as a pendulum Duration.
    :param now: Current time as a pendulum DateTime.
    :return: Age of the youngest subdirectory mtime found.
    """
    youngest_age: pendulum.Duration | None = None
    for subdir_path, _, _ in os.walk(dir_path):
        try:
            mtime = pendulum.from_timestamp(os.path.getmtime(subdir_path))
        except OSError:
            continue
        age = now - mtime
        if youngest_age is None or age < youngest_age:
            youngest_age = age
            if youngest_age <= min_age:
                return youngest_age
    # youngest_age is None only if every getmtime call raised OSError;
    # treat unreadable directories as age zero (i.e. do not flag for deletion).
    return youngest_age if youngest_age is not None else pendulum.duration()


@task(name="Collect old workdir candidates")
def collect_old_workdir_candidates(
    path: str,
    min_age_days: int,
    scan_depth: int,
    max_candidates: int = 10_000,
    ignore_patterns: list[str] | None = None,
) -> list[dict]:
    """Walk subdirectories of path at exactly scan_depth levels and return those older than min_age_days.

    A folder "age" is determined by the youngest subdirectory mtime found anywhere within each candidate
    directory tree (via ``youngest_mtime_in_dir``), not just the top-level directory mtime.
    This is to accommodate Nextflow's nested ``work/ab/cde123.../`` structure.

    Uses ``iglob`` so the filesystem is traversed lazily — no full directory listing is
    loaded into memory. Collection stops once ``max_candidates`` old directories are found.

    :param path: Root path to scan for old work directories.
    :param min_age_days: Minimum age in days for a directory to be a candidate.
    :param scan_depth: Exact number of subdirectory levels below path to scan.
    :param max_candidates: Stop collecting after these many candidates are found (default 10 000).
    :param ignore_patterns: List of fnmatch patterns matched against each directory's basename.
        Matching directories are skipped entirely (e.g. ``["tmp_*", "keep_*"]``).
    :return: List of dicts with path, created, last_modified, and age_days.
    """
    now = pendulum.now()
    min_age = pendulum.duration(days=min_age_days)
    patterns = ignore_patterns or []

    glob_pattern = os.path.join(path, *(["*"] * scan_depth))
    candidates = []
    for dir_path in glob_module.iglob(glob_pattern):
        if not os.path.isdir(dir_path):
            continue
        if any(fnmatch.fnmatch(os.path.basename(dir_path), p) for p in patterns):
            logger.debug(f"Skipping {dir_path} (matches ignore pattern)")
            continue
        if len(candidates) >= max_candidates:
            logger.warning(
                f"Reached max_candidates limit ({max_candidates}). "
                f"Re-run to process remaining directories."
            )
            break
        age = youngest_mtime_in_dir(dir_path, min_age, now)
        if age < min_age:
            continue

        try:
            stat = os.stat(dir_path)
            created = pendulum.from_timestamp(stat.st_ctime).to_datetime_string()
        except OSError:
            created = "unknown"

        candidates.append(
            {
                "path": dir_path,
                "created": created,
                "last_modified": (now - age).to_datetime_string(),
                "age_days": age.in_days(),
            }
        )

    logger.info(
        f"Found {len(candidates)} candidate directories older than {min_age_days} days"
    )

    create_table_artifact(
        key="old-nextflow-workdirs",
        table=candidates,
        description=(
            f"Candidate Nextflow work directories under {path} older than "
            f"{min_age_days} days ({len(candidates)} found)"
        ),
    )

    return candidates


@task(name="Delete workdirs")
def delete_workdirs(candidates: list[dict]) -> int:
    """Delete each directory in candidates using shutil.rmtree.

    Logs failures without raising so that a single bad deletion does not abort the rest.

    :param candidates: List of candidate dicts as returned by collect_old_workdir_candidates.
    :return: Count of successfully deleted directories.
    """
    deleted = 0
    for candidate in candidates:
        dir_path = candidate["path"]
        logger.info(f"Deleting {dir_path}")
        try:
            shutil.rmtree(dir_path)
            deleted += 1
        except Exception as e:
            logger.warning(f"Failed to delete {dir_path}: {e}")
    return deleted


@flow(flow_run_name="Clean workdirs: {path}")
def clean_old_nextflow_workdirs(
    path: str,
    min_age_days: int = 30,
    scan_depth: int = 1,
    max_candidates: int = 10_000,
    ignore_patterns: list[str] | None = None,
):
    """Scan a directory for directories and delete them after human confirmation.

    Useful to delete Nextflow work directories that are no longer needed.

    Collects candidate directories at exactly ``scan_depth`` levels below ``path`` into a
    Prefect table artifact for review, then suspends waiting for confirmation to delete (or not).

    :param path: Root path to scan for old Nextflow work directories.
    :param min_age_days: Directories not modified in this many days are candidates (default 30).
    :param scan_depth: Exact number of subdirectory levels below path to scan (default 1).
    :param max_candidates: Cap on directories collected per run (default 10 000).
    :param ignore_patterns: fnmatch patterns matched against each directory basename to skip
        (e.g. ``["tmp_*", "keep_*"]``).
    """
    candidates = collect_old_workdir_candidates(
        path, min_age_days, scan_depth, max_candidates, ignore_patterns
    )

    if not candidates:
        logger.info("No candidate directories found. Nothing to do.")
        return

    description = dedent(
        f"""\
        Found {len(candidates)} directories under {path}
        not modified in the last {min_age_days} days.

        Review the 'old-nextflow-workdirs' table artifact above before confirming.
        Set confirm_deletion=True to proceed with deletion.
        """
    )

    class ConfirmDeletionInput(RunInput):
        """Input model for confirming deletion of old Nextflow work directories."""

        confirm_deletion: bool = Field(False, description=description)

    confirm = suspend_flow_run(
        wait_for_input=ConfirmDeletionInput.with_initial_data(
            confirm_deletion=False,
            description=description,
        )
    )

    if not confirm.confirm_deletion:
        logger.info("Deletion not confirmed. Flow complete without deleting anything.")
        return

    deleted_count = delete_workdirs(candidates)
    logger.info(f"Deleted {deleted_count} of {len(candidates)} directories.")
