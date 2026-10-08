from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from proteins.flows.load import proteindb_dsn
from proteins.status import status


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
        self.stdout.write(now.report())
        problems = now.problems()
        for problem in problems:
            self.stderr.write(f"FAILED: {problem}")
        if check and problems:
            raise CommandError(f"{len(problems)} checks failed")
