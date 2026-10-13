"""The state of the proteins app, and the checks of it that fail."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from django.db import connections
from django.db.models import Count, Min

from proteins.dataset import layout
from proteins.models import (
    DatasetUpdate,
    StagedContig,
    StagedOccurrence,
    StagedProtein,
)
from proteins.routers import MGYP

UPDATE_OVERDUE = timedelta(hours=36)
STAGED_OVERDUE = timedelta(hours=48)
# An MGYP has 12 digits.
MAX_PROTEIN_ID = 10**12 - 1
HEADROOM_WARNING = 0.9
STAGED_MODELS = (StagedProtein, StagedContig, StagedOccurrence)


class Status(NamedTuple):
    last_update: datetime | None  # when the latest done dataset update finished
    last_day: date | None
    failed: list[tuple[date, str | None]]  # dataset updates that failed since then
    staged: dict[str, int]
    oldest_staged: datetime | None
    complete_day: date | None
    incomplete_days: list[date]
    last_protein_id: int

    def report(self) -> str:
        return "\n".join(
            [
                f"Last dataset update: day {self.last_day}, finished at {self.last_update}",
                *(
                    f"  failed since: day {day}: {message}"
                    for day, message in self.failed
                ),
                "Staged rows: "
                + ", ".join(f"{table} {rows}" for table, rows in self.staged.items())
                + (
                    f", the oldest at {self.oldest_staged}"
                    if self.oldest_staged
                    else ""
                ),
                f"Latest complete day of the protein dataset: {self.complete_day}",
                f"Incomplete days of the protein dataset: {', '.join(map(str, self.incomplete_days)) or 'none'}",
                f"Last protein id: {self.last_protein_id}, {self.last_protein_id / MAX_PROTEIN_ID:.4%} of the MGYP range",
            ]
        )

    def problems(self, now: datetime | None = None) -> list[str]:
        """The checks that fail.

        :param now: The time to check against, by default now.
        """
        now = now or datetime.now(timezone.utc)
        found = []
        if self.last_update is None or now - self.last_update > UPDATE_OVERDUE:
            found.append(f"the last dataset update finished at {self.last_update}")
        if self.oldest_staged and now - self.oldest_staged > STAGED_OVERDUE:
            found.append(f"rows have been staged since {self.oldest_staged}")
        stale = [d for d in self.incomplete_days if self.last_day and d < self.last_day]
        if stale:
            found.append(
                f"incomplete days of the protein dataset before the last dataset update: {', '.join(map(str, stale))}"
            )
        if self.last_protein_id > HEADROOM_WARNING * MAX_PROTEIN_ID:
            found.append(
                f"protein ids are at {self.last_protein_id}, above {HEADROOM_WARNING:.0%} of {MAX_PROTEIN_ID}"
            )
        return found


def status(root: Path) -> Status:
    """The state of the MGYP database and of the protein dataset.

    :param root: The protein dataset's directory.
    """
    last = (
        DatasetUpdate.objects.filter(status=DatasetUpdate.Status.DONE)
        .order_by("-day", "-id")
        .first()
    )
    failed = DatasetUpdate.objects.filter(status=DatasetUpdate.Status.FAILED)
    if last:
        failed = failed.filter(started_at__gt=last.finished_at)
    failed = list(failed.order_by("id").values_list("day", "message"))

    staged, oldest = {}, []
    for model in STAGED_MODELS:
        counts = model.objects.aggregate(rows=Count("id"), first=Min("staged_at"))
        staged[model._meta.db_table] = counts["rows"]
        oldest += [counts["first"]] if counts["first"] else []

    with connections[MGYP].cursor() as cursor:
        cursor.execute("SELECT last_value FROM protein_id_seq")
        (last_protein_id,) = cursor.fetchone()

    complete = layout.complete_days(root)
    return Status(
        last.finished_at if last else None,
        last.day if last else None,
        failed,
        staged,
        min(oldest, default=None),
        complete[-1] if complete else None,
        sorted(layout.days_with_files(root) - set(complete)),
        last_protein_id,
    )
