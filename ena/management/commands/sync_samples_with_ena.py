from django.core.management import BaseCommand, CommandError

import ena.models
from workflows.ena_utils.ena_api_requests import sync_sample_metadata_from_ena


class Command(BaseCommand):
    help = "Sync sample metadata with ENA Portal"

    def add_arguments(self, parser):
        parser.add_argument(
            "-a",
            "--accessions",
            type=str,
            nargs="+",
            help="Individual sample accessions to sync, if not all.",
            required=False,
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Sync all samples.",
        )

    def handle(self, *args, **options):
        if options["accessions"] and options["all"]:
            raise CommandError("Cannot provide both --accessions and --all")
        if options["accessions"]:
            for accession in options["accessions"]:
                sample = ena.models.Sample.objects.get(accession=accession)
                sync_sample_metadata_from_ena(sample)
        elif options["all"]:
            for sample in ena.models.Sample.objects.all():
                sync_sample_metadata_from_ena(sample)
