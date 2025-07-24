import logging
from django.core.management.base import BaseCommand
from django.db.models import Count
from django.contrib.postgres.aggregates import ArrayAgg

from analyses.models import Assembly as MGAssembly
from analyses.models import Study as MGStudy
from ena.models import Study as ENAStudy


class Command(BaseCommand):
    help = "Identify duplicate MGnify studies with the same ENA study and reassign runs and assemblies."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )

    def handle(self, *args, **options):
        self.dry_run = options["dry_run"]
        self.deduplicate_mgys_studies()

    #   find ENA studies with two MGYS accessions
    def deduplicate_mgys_studies(self):
        dup_ena_study_ids = (
            MGStudy.objects.values("ena_accessions")
            .annotate(
                studies_with_these_accessions=Count("accession"),
                all_mgnify_accessions=ArrayAgg("accession"),
            )
            .filter(studies_with_these_accessions=2)
        )

        for ena_id in dup_ena_study_ids:
            logging.info(
                f"ENA accession {sorted(ena_id['ena_accessions'])} is linked to multiple MGnify Studies"
            )
            mgnify_studies = MGStudy.objects.filter(
                ena_accessions__overlap=ena_id["ena_accessions"]
            ).order_by("accession")

            for mgys in mgnify_studies:
                logging.info(f"{mgys.accession}")

            old_study, new_study = mgnify_studies[0], mgnify_studies[1]
            self.reassign_runs_and_assemblies(old_study, new_study)

    def reassign_runs_and_assemblies(self, old_study, new_study):
        """
        Check if duplicate runs exist
        Delete runs from old study
        Move runs and assemblies from new_study to old_study.
        Delete the new_study if it's empty of runs, assemblies, and analyses.
        """
        new_runs = new_study.runs.all()
        old_runs = old_study.runs.all()

        old_run_accessions = set(a for obj in old_runs for a in obj.ena_accessions)
        new_run_accessions = set(a for obj in new_runs for a in obj.ena_accessions)
        if old_run_accessions & new_run_accessions:
            overlapping_accessions = old_run_accessions & new_run_accessions
            logging.warning(
                f"DUPLICATE RUNS FOUND IN BOTH STUDIES: old {old_study.accession} and new {new_study.accession}"
            )
            if self.dry_run:
                logging.info(
                    "Dry run. Real would delete duplicate runs from old study. Exiting"
                )
                return
            # Convert set to list for database query
            overlapping_accessions_list = list(overlapping_accessions)
            old_study.runs.filter(
                ena_accessions__overlap=overlapping_accessions_list
            ).delete()
            logging.info(
                f"Deleted {len(overlapping_accessions)} duplicate runs from old study {old_study.accession}"
            )

        new_assemblies = MGAssembly.objects.filter(assembly_study=new_study)
        if self.dry_run:
            logging.info(
                f"Dry run. Real run would move {new_assemblies.count()} assemblies to {old_study}"
            )
            logging.info(
                f"Dry run. Real run would move {new_runs.count()} runs to {old_study}"
            )
        else:
            logging.info(
                f"Moving {new_assemblies.count()} assemblies from {new_study} to {old_study}"
            )
            new_assemblies.update(assembly_study=old_study)

            logging.info(
                f"Moving {new_runs.count()} runs from {new_study} to {old_study}"
            )
            new_runs.update(study=old_study)

        # Check if new_study is void of runs, assemblies, and analyses
        new_study.refresh_from_db()
        has_runs = new_study.runs.exists()
        has_assemblies = MGAssembly.objects.filter(assembly_study=new_study).exists()
        has_analyses = new_study.analyses.exists()

        # get ena_study for new_study
        old_ena_study_id = old_study.ena_study_id
        new_ena_study_id = new_study.ena_study_id

        if not has_runs and not has_assemblies and not has_analyses:
            if self.dry_run:
                logging.info(f"Dry run. Real run would delete study {new_study}")
            else:
                if old_ena_study_id != new_ena_study_id:
                    ena_study = ENAStudy.objects.get(accession=new_ena_study_id)
                    logging.info(
                        f"Deleting ENA study {ena_study.accession} as it is no longer linked to any MGnify studies."
                    )
                    ena_study.delete()
                new_study.delete()
                logging.info(f"Deleted study {new_study.accession}")
        else:
            logging.warning(
                f"Did not delete {new_study.accession} as study is not empty"
            )
