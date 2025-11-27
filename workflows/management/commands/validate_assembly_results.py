import logging
from pathlib import Path

from django.core.management.base import BaseCommand

from workflows.data_io_utils.schemas.assembly import AssemblyResultSchema

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Validate assembly analysis pipeline results for an assembly."

    def add_arguments(self, parser):
        parser.add_argument(
            "-f",
            "--folder-path",
            "folder_path",
            type=Path,
            help="Path to the folder containing assembly results",
            required=True,
        )
        parser.add_argument(
            "-a",
            "--assembly-id",
            "assembly_id",
            type=str,
            help="Assembly ID (e.g. ERZ123456)",
            required=True,
        )

    def handle(self, *args, **options):
        try:
            schema = AssemblyResultSchema()
            schema.validate_directory_structure(
                base_path=options["folder_path"], identifier=options["assembly_id"]
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Validation successful for assembly results in '{options["folder_path"]}'"
                )
            )

        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f"Error validating assembly results: {e}")
            )
            return
