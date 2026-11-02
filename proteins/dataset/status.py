"""The state of the proteins app, and the checks of it that fail."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from django.db import connections
from django.db.models import Count, Min

from analyses.models import Analysis
from proteins.dataset import layout, metadata
from proteins.dataset.owner import states_of
from proteins.ingestion.analyses import annotated_assembly_analyses, versions_with_rules
from proteins.models import (
    AssemblyProteins,
    DatasetOwner,
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
# Their records come from the migration of the current protein database, not from the listing.
MIGRATED_VERSIONS = (Analysis.PipelineVersions.v4_1, Analysis.PipelineVersions.v5)


class Status(NamedTuple):
    last_update: datetime | None  # when the latest done dataset update finished
    last_day: date | None
    failed: list[tuple[date, str | None]]  # dataset updates that failed since then
    staged: dict[str, int]
    oldest_staged: datetime | None
    complete_day: date | None
    incomplete_days: list[date]
    last_protein_id: int
    no_rule: dict[
        str, int
    ]  # annotated assembly analyses by pipeline version, of versions without gene-caller rules
    withheld: list[str]  # MGYAs of assembly proteins records that are not in emgapi-v2
    owner: DatasetOwner
    owner_states: list[str]  # of the owner's SLURM job

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
                "Annotated assembly analyses of pipeline versions with no gene-caller rule: "
                + (", ".join(f"{v} {n}" for v, n in self.no_rule.items()) or "none"),
                f"MGYAs of assembly proteins records not in emgapi-v2: {len(self.withheld)}"
                + (
                    f", for example {', '.join(self.withheld[:10])}"
                    if self.withheld
                    else ""
                ),
                "Owner of the protein dataset: "
                + (
                    f"{self.owner.flow}, execution {self.owner.execution_id}, flow run {self.owner.flow_run_id},"
                    f" since {self.owner.acquired_at}, SLURM job {self.owner.slurm_job_id}"
                    f" submitted at {self.owner.slurm_submit_at}, whose states are {', '.join(self.owner_states) or 'not found'}"
                    if self.owner.slurm_job_id is not None
                    else "none"
                ),
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
        if self.no_rule:
            found.append(
                "annotated assembly analyses of pipeline versions with no gene-caller rule, which the listing skips: "
                + ", ".join(f"{v} {n}" for v, n in self.no_rule.items())
            )
        if self.withheld:
            found.append(
                f"{len(self.withheld)} MGYAs of assembly proteins records are not in emgapi-v2,"
                " so releases publish none of their proteins"
            )
        # A requeued job has a record for each time it ran, the current one last.
        running = self.owner_states[-1:] == ["RUNNING"]
        if self.owner.slurm_job_id is not None and not running:
            found.append(
                f"the protein dataset is owned by job {self.owner.slurm_job_id}, whose states are"
                f" {', '.join(self.owner_states) or 'not found'}"
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

    no_rule = dict(
        annotated_assembly_analyses()
        .exclude(pipeline_version__in=[*versions_with_rules(), *MIGRATED_VERSIONS])
        .values_list("pipeline_version")
        .annotate(Count("id"))
        .order_by("pipeline_version")
    )

    mgyas = list(
        AssemblyProteins.objects.order_by("analysis_accession").values_list(
            "analysis_accession", flat=True
        )
    )
    known = metadata.find_analyses(mgyas)

    owner = DatasetOwner.objects.get()
    owner_states = (
        states_of(owner.slurm_job_id, owner.slurm_submit_at)
        if owner.slurm_job_id is not None
        else []
    )

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
        no_rule,
        [mgya for mgya in mgyas if mgya not in known],
        owner,
        owner_states,
    )
