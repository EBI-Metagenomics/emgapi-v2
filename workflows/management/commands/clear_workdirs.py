import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Max
from django.utils.timezone import now

from analyses.models import Study
from workflows.flows.assemble_study_tasks.archive_assembly_dirs import (
    archive_assembly_dirs,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = "Clear some known work directories. Currently: miassembler only."

    def add_arguments(self, parser):
        parser.add_argument(
            "-t",
            "--tolerance_days",
            type=int,
            help="How many days must have elapsed since a study or its assemblies were last updated, to consider it for cleaning.",
            required=True,
        )
        parser.add_argument(
            "-n",
            "--dry_run",
            help="If set, just print what would be done without deleting anything.",
            action="store_true",
        )

    def handle(self, *args, **options):
        self.before = now() - timedelta(days=options["tolerance_days"])
        logger.info(f"Will clean workdirs for objects not updated since {self.before}")

        self._clean_assemblies(*args, **options)

    def _clean_assemblies(self, *args, **options):
        cleanable_studies = Study.objects.annotate(
            last_updated_assembly=Max("assemblies_reads__updated_at")
        ).filter(
            updated_at__lt=self.before,
            last_updated_assembly__isnull=False,
            last_updated_assembly__lt=self.before,
        )

        for study in cleanable_studies:
            logger.info(f"Cleaning study {study}")
            archive_assembly_dirs(study.accession, dry_run=options["dry_run"])
