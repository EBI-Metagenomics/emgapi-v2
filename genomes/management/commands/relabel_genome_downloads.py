from django.core.management.base import BaseCommand, CommandError

from genomes.management.lib.genome_util import get_genome_download_labels
from genomes.models import CatalogueGenome, GenomeCatalogue


class Command(BaseCommand):
    help = "Relabel CatalogueGenome download groups and types without changing other metadata."

    def add_arguments(self, parser):
        parser.add_argument(
            "catalogue_ids",
            nargs="*",
            help="Catalogue release IDs to update. Defaults to all releases.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many catalogue genomes would change without writing updates.",
        )

    def handle(self, *args, **options):
        catalogue_ids = options["catalogue_ids"]
        genomes = CatalogueGenome.objects.exclude(downloads=[])
        if catalogue_ids:
            found_ids = set(
                GenomeCatalogue.objects.filter(pk__in=catalogue_ids).values_list(
                    "pk", flat=True
                )
            )
            missing_ids = sorted(set(catalogue_ids) - found_ids)
            if missing_ids:
                raise CommandError("Unknown catalogue IDs: " + ", ".join(missing_ids))
            genomes = genomes.filter(catalogue_id__in=catalogue_ids)

        count = 0
        for genome in genomes.only("pk", "downloads").iterator(chunk_size=1000):
            changed = False
            for download in genome.downloads:
                group, download_type = get_genome_download_labels(download["path"])
                if (
                    download.get("download_group") != group
                    or download.get("download_type") != download_type
                ):
                    download["download_group"] = group
                    download["download_type"] = download_type
                    changed = True
            if changed:
                if not options["dry_run"]:
                    # QuerySet.update avoids save hooks and timestamp changes.
                    CatalogueGenome.objects.filter(pk=genome.pk).update(
                        downloads=genome.downloads
                    )
                count += 1

        verb = "Would update" if options["dry_run"] else "Updated"
        self.stdout.write(f"{verb} downloads for {count} catalogue genomes.")
