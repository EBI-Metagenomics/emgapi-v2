"""The merge: for each hash prefix, merges the base file and the day files of the protein dataset into one new base file.

It is the only step that deletes the files of a complete day. Do not run it while a protein release reads the protein dataset.
"""

import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from django.conf import settings

from proteins.dataset import layout
from proteins.dataset.owner import Owner

logger = logging.getLogger(__name__)


def clean_up_interrupted_merge(root: Path) -> None:
    """Deletes the files that an interrupted merge left: older base files, and day files that the newest base file includes.

    :param root: The protein dataset's directory.
    """
    for leftover in root.rglob(".*.tmp"):
        leftover.unlink()
    for prefix in (root / "protein").glob("prefix=*"):
        bases = sorted(prefix.glob("base-*.parquet"))
        if bases:
            newest = bases[-1].name.removeprefix("base-")
            for path in bases[:-1]:
                path.unlink()
            for path in prefix.glob("day-*.parquet"):
                if path.name.removeprefix("day-") <= newest:
                    path.unlink()


def merge_dataset(root: Path, owner: Owner) -> int:
    """Merges the base file and the day files of each hash prefix into a new base file, dated with the latest complete day.

    :param root: The protein dataset's directory.
    :param owner: This run's ownership of the protein dataset, checked before each prefix.
    :return: The number of prefixes merged.
    """
    day = layout.complete_days(root)[-1]
    by_prefix = defaultdict(list)
    for path in layout.files(root, "protein", day):
        by_prefix[path.parent].append(path)
    merged = 0
    for prefix, files in sorted(by_prefix.items()):
        if not any(f.name.startswith("day-") for f in files):
            continue
        owner.check()
        merge_prefix(prefix, files, day)
        merged += 1
    return merged


def merge_prefix(prefix: Path, files: list[Path], day: date) -> None:
    """Merges the base file and the day files of a prefix into base-`day`, then deletes them.

    :param prefix: The prefix's directory.
    :param files: Its base file and day files.
    :param day: The date of the new base file.
    """
    logger.info("merging %s: %d files", prefix.name, len(files))
    base = prefix / f"base-{day}.parquet"
    layout.merge(
        base, "protein", files, settings.EMG_CONFIG.proteins.duckdb_memory_limit
    )
    for path in files:
        if path != base:
            path.unlink()
