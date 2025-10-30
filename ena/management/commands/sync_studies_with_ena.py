from django.core.management import BaseCommand, CommandError

import ena.models
from workflows.ena_utils.ena_api_requests import sync_study_metadata_from_ena


class Command(BaseCommand):
    help = "Sync study partial metadata with ENA Portal"

    def add_arguments(self, parser):
        parser.add_argument(
            "-a",
            "--accessions",
            type=str,
            nargs="+",
            help="Individual study accessions to sync, if not all.",
            required=False,
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Sync all studies.",
        )

    def handle(self, *args, **options):
        if options["accessions"] and options["all"]:
            raise CommandError("Cannot provide both --accessions and --all")
        if options["accessions"]:
            for accession in options["accessions"]:
                study = ena.models.Study.objects.get_ena_study(accession)
                sync_study_metadata_from_ena(study)
        elif options["all"]:
            for study in ena.models.Study.objects.all():
                sync_study_metadata_from_ena(study)

        # TODO: consider a prefect flow encompassing this as well as sync_privacy_state_of_ena_study_and_derived_objects flow
