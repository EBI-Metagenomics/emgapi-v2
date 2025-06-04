from pathlib import Path
import logging

from django.core.management.base import BaseCommand

from workflows.flows.analyse_study_tasks.sanity_check_assembly_results import (
    AssemblyAnalysisResultsSchema,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Validate assembly analysis pipeline results for an assembly.."

    def add_arguments(self, parser):
        parser.add_argument(
            "folder_path",
            type=str,
            help="Path to the folder containing assembly results",
            nargs="?",  # Make it optional when using --schema-only
        )
        parser.add_argument(
            "assembly_id",
            type=str,
            help="Assembly ID (e.g. ERZ123456)",
            nargs="?",  # Make it optional when using --schema-only
        )

    def handle(self, *args, **options):

        # Check if required arguments are provided when not using --schema-only
        if not options["folder_path"] or not options["assembly_id"]:
            self.stderr.write(
                self.style.ERROR(
                    "Error: folder_path and assembly_id are required when not using --schema-only"
                )
            )
            return

        # Convert folder path to Path object
        folder_path = Path(options["folder_path"])

        try:
            AssemblyAnalysisResultsSchema.create_schema(
                assembly_current_outdir=folder_path, assembly_id=options["assembly_id"]
            )

            self.stderr.write(
                self.style.SUCCESS(
                    f"Validation successful for assembly results in '{folder_path}'"
                )
            )

        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f"Error validating assembly results: {str(e)}")
            )
            return
