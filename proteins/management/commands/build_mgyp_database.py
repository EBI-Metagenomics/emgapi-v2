from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from proteins.dataset.build import BuildError, build_mgyp_database


class Command(BaseCommand):
    help = (
        "Build the MGYP database from the latest complete day of the protein dataset,"
        " into a schema just created by mgyp_schema.sql. Run as proteindb_owner."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dsn",
            required=True,
            help="libpq connection string of the MGYP database as proteindb_owner,"
            " such as 'host=H dbname=D user=proteindb_owner'",
        )
        parser.add_argument(
            "--rebuild",
            action="store_true",
            help="To rebuild a lost MGYP database from the protein dataset",
        )
        parser.add_argument(
            "--protein-id-start",
            type=int,
            help="The first protein id to allocate, above every MGYP issued. By default the protein"
            " dataset's highest + 1, or for a rebuild + 10^8, for MGYPs the lost database could have given",
        )

    def handle(self, *args, dsn, rebuild, protein_id_start, **options):
        root = Path(settings.EMG_CONFIG.proteins.dataset_dir)
        try:
            day = build_mgyp_database(dsn, root, rebuild, protein_id_start)
        except BuildError as e:
            raise CommandError(e)
        self.stdout.write(
            self.style.SUCCESS(
                f"Built the MGYP database from the protein dataset as of {day}"
            )
        )
