from django.core.management.base import BaseCommand, CommandError

from genomes.models import GenomeCatalogue
from genomes.search_indexes import (
    resolve_sourmash_artifact_path,
    upsert_sourmash_search_index,
)


class Command(BaseCommand):
    help = (
        "Backfill sourmash GenomeSearchIndex records for published catalogue releases."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "catalogue_ids",
            nargs="*",
            help="Optional catalogue IDs to backfill. Defaults to all published releases.",
        )
        parser.add_argument(
            "--include-ready",
            action="store_true",
            help="Include READY catalogue releases in addition to PUBLISHED ones.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be registered without writing any database changes.",
        )

    def handle(self, *args, **options):
        catalogue_ids = options["catalogue_ids"]
        include_ready = options["include_ready"]
        dry_run = options["dry_run"]

        statuses = [GenomeCatalogue.Status.PUBLISHED]
        if include_ready:
            statuses.append(GenomeCatalogue.Status.READY)

        queryset = GenomeCatalogue.objects.filter(status__in=statuses).order_by(
            "catalogue_id"
        )
        if catalogue_ids:
            queryset = queryset.filter(catalogue_id__in=catalogue_ids)
            found_ids = set(queryset.values_list("catalogue_id", flat=True))
            missing_ids = sorted(set(catalogue_ids) - found_ids)
            if missing_ids:
                raise CommandError(
                    "Unknown or ineligible catalogue IDs: " + ", ".join(missing_ids)
                )

        processed = 0
        skipped = 0
        for catalogue in queryset:
            try:
                artifact_path = resolve_sourmash_artifact_path(catalogue.catalogue_id)
            except FileNotFoundError as exc:
                self.stderr.write(f"Skipping {catalogue.catalogue_id}: {exc}")
                skipped += 1
                continue
            if dry_run:
                self.stdout.write(
                    f"Would register sourmash index for {catalogue.catalogue_id}: {artifact_path}"
                )
            else:
                index, created, retired_count = upsert_sourmash_search_index(catalogue)
                self.stdout.write(
                    f"{catalogue.catalogue_id}: {'created' if created else 'updated'} "
                    f"{index.pk} using {index.artifact_path} "
                    f"(retired {retired_count} previous active index(es))"
                )
            processed += 1

        if processed == 0:
            self.stdout.write("No eligible catalogue releases found.")
        elif skipped:
            self.stdout.write(
                f"Processed {processed} catalogue(s); skipped {skipped} without artifacts."
            )
