from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from proteins.flows.load import proteindb_dsn
from proteins.status import MAX_PROTEIN_ID, status


class Command(BaseCommand):
    help = "The state of the protein DB, and the checks of it that fail."

    def add_arguments(self, parser):
        parser.add_argument(
            "--check", action="store_true", help="Exit non-zero if any check fails"
        )

    def handle(self, *args, check, **options):
        now = status(
            proteindb_dsn(), Path(settings.EMG_CONFIG.proteindb.root) / "tier2"
        )
        owner = now.owner
        lines = [
            f"Last successful load: day {now.last_day}, finished at {now.last_load}",
            *(f"  failed since: day {day}: {message}" for day, message in now.failed),
            "Staged rows: "
            + ", ".join(f"{table} {rows}" for table, rows in now.staged.items())
            + (f", the oldest at {now.oldest_staged}" if now.oldest_staged else ""),
            "Owner of Tier 2: "
            + (
                f"{owner.flow}, job {owner.job} submitted at {owner.submit_at},"
                f" {', '.join(owner.states) or 'not found by sacct'}"
                if owner
                else "none"
            ),
            f"Latest complete Tier 2 day: {now.complete_day}",
            f"Incomplete Tier 2 days: {', '.join(map(str, now.incomplete_days)) or 'none'}",
            f"Last protein id: {now.last_protein_id}, {now.last_protein_id / MAX_PROTEIN_ID:.4%} of the MGYP range",
            f"Unresolved assemblies in the latest snapshot: {len(now.unresolved)}",
            *(f"  {accession}" for accession in now.unresolved),
        ]
        self.stdout.write("\n".join(lines))
        problems = now.problems()
        for problem in problems:
            self.stderr.write(f"FAILED: {problem}")
        if check and problems:
            raise CommandError(f"{len(problems)} checks failed")
