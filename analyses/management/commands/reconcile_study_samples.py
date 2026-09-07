from django.core.management.base import BaseCommand

from analyses.models import Sample, Study


class Command(BaseCommand):
    help = "Link studies that have analyses but no samples to their analyses' samples."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report intended changes without modifying the database.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        studies = list(
            Study.objects.filter(
                analyses__isnull=False,
                samples__isnull=True,
            ).distinct()
        )

        linked_samples = 0
        for study in studies:
            samples = list(Sample.objects.filter(analyses__study=study).distinct())
            self.stdout.write(f"{study.accession}: {len(samples)} samples to link")
            if not dry_run:
                study.samples.add(*samples)
            linked_samples += len(samples)

        action = "Would link" if dry_run else "Linked"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} {linked_samples} samples across {len(studies)} studies."
            )
        )
