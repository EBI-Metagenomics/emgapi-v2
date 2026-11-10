from django.core.management.base import BaseCommand
from django.utils import timezone

from analyses.models import Analysis


class Command(BaseCommand):
    help = "Make public assembly analyses private when their ENA study is private."

    def add_arguments(self, parser):
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            default=True,
            help="List the broken analyses without modifying the database (default).",
        )
        mode_group.add_argument(
            "--apply",
            action="store_false",
            dest="dry_run",
            help="Make the broken analyses private.",
        )

    def handle(self, *args, **options):
        broken_analyses = Analysis.objects.filter(
            assembly__isnull=False,
            is_private=False,
            ena_study__is_private=True,
        )

        ena_studies = {}
        for analysis in broken_analyses.select_related("ena_study").order_by("id"):
            self.stdout.write(f"{analysis.accession}\t{analysis.ena_study.accession}")
            ena_studies[analysis.ena_study.accession] = analysis.ena_study

        if options["dry_run"]:
            self.stdout.write(
                f"Found {broken_analyses.count()} public assembly analyses in "
                f"{len(ena_studies)} private ENA studies. Use --apply to fix them."
            )
            return

        updated = 0
        for ena_study in ena_studies.values():
            if not ena_study.webin_submitter:
                self.stdout.write(
                    self.style.WARNING(f"{ena_study.accession} has no webin_submitter.")
                )
            updated += broken_analyses.filter(ena_study=ena_study).update(
                is_private=True,
                webin_submitter=ena_study.webin_submitter,
                # The EBI Search dump finds changed analyses by updated_at.
                updated_at=timezone.now(),
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Made {updated} assembly analyses private in "
                f"{len(ena_studies)} ENA studies."
            )
        )
