from datetime import date
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from proteins.flows.load import proteindb_dsn
from proteins.migrate import (
    EXPORTS,
    MigrateError,
    build_tier1,
    build_tier2,
    export,
    export_dims,
    resolution,
    verify,
)


class Command(BaseCommand):
    help = "The protein DB's migration steps. Run as proteindb_owner."

    def add_arguments(self, parser):
        steps = parser.add_subparsers(dest="step", required=True)
        exporting = steps.add_parser(
            "export",
            help="Export one child of a table of the frozen current database, or its reference tables",
        )
        exporting.add_argument("--table", required=True, choices=[*EXPORTS, "dims"])
        exporting.add_argument(
            "--child",
            type=int,
            choices=range(64),
            metavar="0..63",
            help="The child table, for every table but dims",
        )
        exporting.add_argument(
            "--source",
            required=True,
            help="libpq URI of the current database, with the password in ~/.pgpass",
        )
        exporting.add_argument("--out", required=True, type=Path)

        tier2 = steps.add_parser(
            "tier2", help="Build Tier 2's first day from the complete export"
        )
        tier2.add_argument(
            "--date",
            required=True,
            type=date.fromisoformat,
            help="M, the date of the freeze, as YYYY-MM-DD",
        )
        tier2.add_argument("--export", required=True, type=Path)

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

        verifying = steps.add_parser(
            "verify",
            help="Compare Tier 2 and Tier 1 with the frozen current database",
        )
        verifying.add_argument(
            "--source",
            required=True,
            help="libpq URI of the current database, with the password in ~/.pgpass",
        )
        verifying.add_argument(
            "--sample",
            type=int,
            default=1_000_000,
            help="Rows of each prefix whose hash is checked (default: %(default)s)",
        )

        resolving = steps.add_parser(
            "resolve",
            help="List the current database's assemblies that emgapi-v2 does not know, or knows with another study or biome",
        )
        resolving.add_argument(
            "--source",
            required=True,
            help="libpq URI of the current database, with the password in ~/.pgpass",
        )

    def handle(self, *args, step, **options):
        try:
            if step == "export":
                self.export(**options)
            elif step == "tier2":
                build_tier2(options["export"], tier2_root(), options["date"])
                self.stdout.write(
                    self.style.SUCCESS(f"Built Tier 2 as of {options['date']}")
                )
            elif step == "tier1":
                self.tier1(**options)
            elif step == "resolve":
                self.resolve(**options)
            else:
                self.verify(**options)
        except MigrateError as e:
            raise CommandError(e)

    def export(self, table, child, source, out, **options):
        if (table == "dims") != (child is None):
            raise CommandError(
                "--child is needed for every table but dims, which takes none"
            )
        if table == "dims":
            export_dims(source, out)
            self.stdout.write(
                self.style.SUCCESS(f"Exported the reference tables to {out}")
            )
        else:
            rows = export(source, out, table, child)
            self.stdout.write(
                self.style.SUCCESS(f"Exported {rows} rows of {table} child {child}")
            )

    def tier1(self, rebuild, protein_id_start, **options):
        if rebuild != (protein_id_start is not None):
            raise CommandError("--rebuild and --protein-id-start go together")
        day = build_tier1(proteindb_dsn(), tier2_root(), protein_id_start)
        self.stdout.write(self.style.SUCCESS(f"Built Tier 1 from Tier 2 as of {day}"))

    def verify(self, source, sample, **options):
        failed = verify(source, proteindb_dsn(), tier2_root(), sample)
        for failure in failed:
            self.stderr.write(failure)
        if failed:
            raise CommandError(f"{len(failed)} checks failed")
        self.stdout.write(
            self.style.SUCCESS("Tier 2 and Tier 1 match the current database")
        )

    def resolve(self, source, **options):
        total, differing = resolution(source)
        for row in differing:
            self.stdout.write("\t".join(row))
        message = f"{len(differing)} assemblies, of {total} accessions, are not in emgapi-v2 or differ in study or biome"
        if differing:
            raise CommandError(message)
        self.stdout.write(self.style.SUCCESS(message))


def tier2_root() -> Path:
    return Path(settings.EMG_CONFIG.proteindb.root) / "tier2"
