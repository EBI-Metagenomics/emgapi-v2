"""The state of the protein DB, and the checks of it that fail."""

import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

import psycopg
import pyarrow.compute as pc
import pyarrow.parquet as pq

from proteins import tier2
from proteins.load import days_with_files
from proteins.owner import job_states

LOAD_OVERDUE = timedelta(hours=36)
STAGED_OVERDUE = timedelta(hours=48)
# An MGYP has 12 digits.
MAX_PROTEIN_ID = 10**12 - 1
HEADROOM_WARNING = 0.9
STAGING = ("staging_protein", "staging_contig", "staging_occurrence")


class Owner(NamedTuple):
    flow: str
    job: int
    submit_at: datetime
    states: list[str]


class Status(NamedTuple):
    last_load: datetime | None  # when the latest done load finished
    last_day: date | None
    failed: list[tuple[date, str | None]]  # loads that failed since then
    staged: dict[str, int]
    oldest_staged: datetime | None
    owner: Owner | None
    complete_day: date | None
    incomplete_days: list[date]
    last_protein_id: int
    unresolved: list[str]  # accessions emgapi-v2 did not know, in the latest snapshot

    def problems(self, now: datetime | None = None) -> list[str]:
        now = now or datetime.now(timezone.utc)
        found = []
        if self.last_load is None or now - self.last_load > LOAD_OVERDUE:
            found.append(f"the last successful load finished at {self.last_load}")
        if self.oldest_staged and now - self.oldest_staged > STAGED_OVERDUE:
            found.append(f"rows have been staged since {self.oldest_staged}")
        if self.owner and "RUNNING" not in self.owner.states:
            found.append(
                f"Tier 2 is owned by job {self.owner.job}, which is not running:"
                f" {', '.join(self.owner.states) or 'not found'}"
            )
        stale = [d for d in self.incomplete_days if self.last_day and d < self.last_day]
        if stale:
            found.append(
                f"incomplete Tier 2 days before the last load: {', '.join(map(str, stale))}"
            )
        if self.last_protein_id > HEADROOM_WARNING * MAX_PROTEIN_ID:
            found.append(
                f"protein ids are at {self.last_protein_id}, above {HEADROOM_WARNING:.0%} of {MAX_PROTEIN_ID}"
            )
        return found


def status(dsn: str, root: Path) -> Status:
    """Read as proteindb_load."""
    with psycopg.connect(dsn) as conn:
        last_load, last_day = conn.execute(
            "SELECT finished_at, ingest_date FROM proteindb.load_log WHERE status = 'done'"
            " ORDER BY ingest_date DESC, id DESC LIMIT 1"
        ).fetchone() or (None, None)
        failed = conn.execute(
            "SELECT ingest_date, message FROM proteindb.load_log WHERE status = 'failed'"
            " AND started_at > coalesce(%s::timestamptz, '-infinity') ORDER BY id",
            [last_load],
        ).fetchall()
        staged, oldest = {}, []
        for table in STAGING:
            staged[table], first = conn.execute(
                f"SELECT count(*), min(staged_at) FROM proteindb.{table}"
            ).fetchone()
            oldest += [first] if first else []
        recorded = conn.execute(
            "SELECT flow, slurm_job_id, slurm_submit_at FROM proteindb.tier2_owner"
            " WHERE slurm_job_id IS NOT NULL"
        ).fetchone()
        (last_protein_id,) = conn.execute(
            "SELECT last_value FROM proteindb.protein_id_seq"
        ).fetchone()

    owner = None
    if recorded:
        flow, job, submit_at = recorded
        try:
            states = job_states(job).get(submit_at, [])
        except (OSError, subprocess.SubprocessError, ValueError) as e:
            states = [f"unknown, as sacct failed: {e}"]
        owner = Owner(flow, job, submit_at, states)

    complete = tier2.complete_days(root)
    unresolved = []
    if complete:
        assemblies = pq.read_table(
            tier2.files(root, "dims/assembly")[0], columns=["accession", "study_id"]
        )
        missing = assemblies.filter(pc.is_null(assemblies["study_id"]))
        unresolved = sorted(set(missing["accession"].to_pylist()))
    return Status(
        last_load,
        last_day,
        failed,
        staged,
        min(oldest, default=None),
        owner,
        complete[-1] if complete else None,
        sorted(days_with_files(root) - set(complete)),
        last_protein_id,
        unresolved,
    )
