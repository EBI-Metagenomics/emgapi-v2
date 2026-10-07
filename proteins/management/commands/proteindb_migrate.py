from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from proteins.flows.load import proteindb_dsn
from proteins.migrate import MigrateError, build_tier1


class Command(BaseCommand):
    help = "The protein DB's migration steps. Run as proteindb_owner."

    def add_arguments(self, parser):
        steps = parser.add_subparsers(dest="step", required=True)
        tier1 = steps.add_parser(
            "tier1",
            help="Build Tier 1 from Tier 2, into a schema just created by tier1.sql",
        )
        tier1.add_argument(
            "--rebuild",
            action="store_true",
            help="Rebuild from the latest complete day after Tier 1 was lost",
        )
        tier1.add_argument(
            "--protein-id-start",
            type=int,
            help="With --rebuild, the first protein id to allocate: above every MGYP emitted",
        )

    def handle(self, *args, step, **options):
        if options["rebuild"] != (options["protein_id_start"] is not None):
            raise CommandError("--rebuild and --protein-id-start go together")
        try:
            day = build_tier1(
                proteindb_dsn(),
                Path(settings.EMG_CONFIG.proteindb.root) / "tier2",
                options["protein_id_start"],
            )
        except MigrateError as e:
            raise CommandError(e)
        self.stdout.write(self.style.SUCCESS(f"Built Tier 1 from Tier 2 as of {day}"))
