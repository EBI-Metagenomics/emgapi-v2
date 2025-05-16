import logging
from pathlib import Path

from django.core.management.base import BaseCommand

import analyses.models
import ena.models
import workflows.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Setup assembly analysis batches for a study.

    Fetches study and assemblies from ENA, creates analysis objects and batches.

    This is at this point used to fill the DB to test the ADMIN panel. In the future it will be used.
    """

    help = "Setup assembly analysis batches for a study"

    def add_arguments(self, parser):
        """
        Define command-line arguments.

        :param parser: Django argument parser
        :type parser: argparse.ArgumentParser
        """
        parser.add_argument("study_accession", type=str, help="Study accession")
        parser.add_argument("--biome", type=str, required=True, help="Biome path")
        parser.add_argument(
            "--workspace-dir", type=str, default=None, help="Workspace directory"
        )
        parser.add_argument(
            "--max-assemblies", type=int, default=500, help="Max assemblies from ENA"
        )
        parser.add_argument(
            "--pipeline-version", type=str, choices=["v5", "v6"], default="v6"
        )

    def handle(self, *args, **options):
        """
        Main command execution.

        :param args: Positional arguments
        :param options: Command options dictionary
        """
        study_accession = options["study_accession"]
        # TODO: implement the biome setting
        # biome_path = options["biome"]

        # Get or create ENA Study
        ena_study = ena.models.Study.objects.get_ena_study(study_accession)
        if not ena_study:
            ena_study = get_study_from_ena(study_accession)

        ena_study.refresh_from_db()
        self.stdout.write(f"ENA Study: {ena_study.accession} - {ena_study.title}")

        # Get or create MGnify Study
        mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
            study_accession
        )
        mgnify_study.refresh_from_db()
        self.stdout.write(f"MGnify Study: {mgnify_study.accession}")

        # Set biome
        # if not mgnify_study.biome:
        #     biome = analyses.models.Biome.objects.get(path=biome_path)
        #     mgnify_study.biome = biome
        #     mgnify_study.save()
        #     self.stdout.write(f"Biome set: {biome.path}")

        # Fetch assemblies from ENA
        assemblies_accessions = get_study_assemblies_from_ena(
            ena_study.accession,
            limit=options["max_assemblies"],
        )
        self.stdout.write(f"Found {len(assemblies_accessions)} assemblies from ENA")

        # Create analysis objects
        pipeline_version = (
            analyses.models.Analysis.PipelineVersions.v6
            if options["pipeline_version"] == "v6"
            else analyses.models.Analysis.PipelineVersions.v5
        )

        analyses_list = []
        for assembly in mgnify_study.assemblies_assembly.filter(
            ena_accessions__overlap=assemblies_accessions
        ).select_related("sample"):
            analysis, created = analyses.models.Analysis.objects.get_or_create(
                study=mgnify_study,
                sample=assembly.sample,
                assembly=assembly,
                ena_study=mgnify_study.ena_study,
                pipeline_version=pipeline_version,
            )
            if created:
                self.stdout.write(f"Created analysis for {assembly.first_accession}")
            analysis.inherit_experiment_type()
            analyses_list.append(analysis)

        self.stdout.write(f"Created {len(analyses_list)} analyses")

        # Create batches
        batches = workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=mgnify_study,
            pipeline=pipeline_version,
            workspace_dir=(
                Path(options["workspace_dir"]) if options["workspace_dir"] else None
            ),
        )

        self.stdout.write(f"\nCreated {len(batches)} batches:")
        for batch in batches:
            self.stdout.write(f"  {str(batch.id)[:8]}: {batch.total_analyses} analyses")
