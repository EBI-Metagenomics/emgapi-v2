from django.conf import settings
from django.core.management.base import BaseCommand

from analyses.models import Run
from workflows.ena_utils.ena_api_requests import get_study_assemblies_from_ena


class Command(BaseCommand):
    help = "Repair Runs created from semicolon-delimited ENA co-assembly accessions."

    def handle(self, *args, **options):
        malformed_run_ids = list(
            Run.objects.filter(ena_accessions__icontains=";").values_list(
                "id", flat=True
            )
        )
        study_accessions = sorted(
            {
                accession
                for accession in Run.objects.filter(
                    id__in=malformed_run_ids
                ).values_list("assemblies__ena_study_id", flat=True)
                if accession
            }
        )

        refresh_assemblies = get_study_assemblies_from_ena.with_options(
            refresh_cache=True
        )
        for accession in study_accessions:
            refresh_assemblies(
                accession,
                limit=settings.EMG_CONFIG.ena.portal_max_readruns_to_fetch,
            )

        deleted = 0
        cleaned = 0
        unresolved = 0
        for run in Run.objects.filter(id__in=malformed_run_ids):
            valid_accessions = [
                accession for accession in run.ena_accessions if ";" not in accession
            ]
            if valid_accessions:
                run.ena_accessions = valid_accessions
                run.save(update_fields=["ena_accessions"])
                cleaned += 1
            elif not (
                run.assemblies.exists()
                or run.analyses.exists()
                or run.additional_contained_genomes.exists()
            ):
                run.delete()
                deleted += 1
            else:
                unresolved += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Checked {len(malformed_run_ids)} malformed Runs across "
                f"{len(study_accessions)} ENA studies: deleted {deleted}, "
                f"cleaned {cleaned}, unresolved {unresolved}."
            )
        )
