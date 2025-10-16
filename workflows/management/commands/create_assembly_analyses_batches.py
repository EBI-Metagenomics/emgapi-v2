import logging
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
)
from workflows.flows.analyse_study_tasks.assembly.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.analyse_study_tasks.assembly.run_assembly_analysis_pipeline_batch import (
    run_assembly_analysis_pipeline_batch,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command to create assembly analysis batches for a study.

    This command provides a CLI interface for batch creation and testing without
    requiring the full Prefect flow orchestration. It can:
    - Fetch assemblies from ENA or use existing ones
    - Create Analysis objects
    - Create AssemblyAnalysisBatch objects with proper chunking
    - Optionally execute the analysis pipeline
    """

    help = "Create assembly analysis batches for a study (with optional pipeline execution)"

    def add_arguments(self, parser):
        """Define command-line arguments."""
        # Required arguments
        parser.add_argument(
            "study_accession",
            type=str,
            help="Study accession (e.g., PRJEB24849, ERP106708)",
        )

        # Optional arguments for data fetching
        parser.add_argument(
            "--fetch-from-ena",
            action="store_true",
            help="Fetch assemblies from ENA API (default: use existing assemblies in DB)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10000,
            help="Limit number of assemblies to fetch from ENA (default: 10000)",
        )

        # Batch configuration
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=None,
            help="Override default chunk size (default: from config)",
        )
        parser.add_argument(
            "--max-analyses",
            type=int,
            default=None,
            help="Safety cap on total analyses (default: from config)",
        )
        parser.add_argument(
            "--base-results-dir",
            type=str,
            default=None,
            help="Custom results directory",
        )

        # Analysis configuration
        parser.add_argument(
            "--pipeline-version",
            type=str,
            choices=["v5", "v6"],
            default="v6",
            help="Pipeline version (default: v6)",
        )
        parser.add_argument(
            "--skip-completed",
            action="store_true",
            default=True,
            help="Skip already-completed analyses (default: True)",
        )
        parser.add_argument(
            "--biome",
            type=str,
            default=None,
            help="Set biome (e.g., root.engineered) - required if study has no biome",
        )

        # Execution control
        parser.add_argument(
            "--run-pipeline",
            action="store_true",
            help="Execute the pipeline after creating batches (default: False)",
        )

    def handle(self, *args, **options):
        """Main command execution."""
        study_accession = options["study_accession"]
        fetch_from_ena = options["fetch_from_ena"]
        run_pipeline = options["run_pipeline"]

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*70}\nCreating Assembly Analysis Batches for {study_accession}\n{'='*70}\n"
            )
        )

        try:
            # Step 1: Fetch/Get Study
            self.stdout.write(self.style.WARNING("\n[1/6] Fetching study..."))
            mgnify_study = self._get_or_create_study(
                study_accession, options.get("biome")
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"  ✓ Study: {mgnify_study.accession} ({mgnify_study.title})"
                )
            )
            if mgnify_study.biome:
                self.stdout.write(
                    self.style.SUCCESS(f"  ✓ Biome: {mgnify_study.biome.path}")
                )

            # Step 2: Fetch assemblies from ENA (if requested)
            assembly_accessions = []
            if fetch_from_ena:
                self.stdout.write(
                    self.style.WARNING(
                        f"\n[2/6] Fetching assemblies from ENA (limit={options['limit']})..."
                    )
                )
                assembly_accessions = get_study_assemblies_from_ena(
                    study_accession, limit=options["limit"]
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ Fetched {len(assembly_accessions)} assemblies from ENA"
                    )
                )
            else:
                self.stdout.write(
                    self.style.WARNING(
                        "\n[2/6] Skipping ENA fetch (using existing assemblies)"
                    )
                )
                # Get existing assembly accessions
                assembly_accessions = list(
                    mgnify_study.assemblies_assembly.values_list(
                        "ena_accessions", flat=True
                    )
                )
                # Flatten JSONField arrays
                assembly_accessions = [
                    acc
                    for accession_list in assembly_accessions
                    for acc in accession_list
                ]
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ Found {len(assembly_accessions)} existing assemblies"
                    )
                )

            if not assembly_accessions:
                raise CommandError("No assemblies found for this study")

            # Step 3: Create Analyses
            self.stdout.write(
                self.style.WARNING("\n[3/6] Creating analysis objects...")
            )
            pipeline_version = (
                analyses.models.Analysis.PipelineVersions.v6
                if options["pipeline_version"] == "v6"
                else analyses.models.Analysis.PipelineVersions.v5
            )

            analyses_list = create_analyses_for_assemblies(
                mgnify_study, assembly_accessions, pipeline=pipeline_version
            )
            self.stdout.write(
                self.style.SUCCESS(f"  ✓ Created/found {len(analyses_list)} analyses")
            )

            # Step 4: Create Batches
            self.stdout.write(self.style.WARNING("\n[4/6] Creating batches..."))
            base_results_dir = (
                Path(options["base_results_dir"])
                if options["base_results_dir"]
                else None
            )

            batches = analyses.models.AssemblyAnalysisBatch.objects.create_batches_for_study(
                study=mgnify_study,
                pipeline=pipeline_version,
                chunk_size=options["chunk_size"],
                max_analyses=options["max_analyses"]
                or settings.EMG_CONFIG.assembly_analysis_pipeline.max_analyses_per_study,
                base_results_dir=base_results_dir,
                skip_completed=options["skip_completed"],
            )

            self.stdout.write(self.style.SUCCESS(f"  ✓ Created {len(batches)} batches"))
            for batch in batches:
                self.stdout.write(
                    f"    - {str(batch.id)[:8]}: {batch.total_analyses} analyses (state: {batch.asa_state})"
                )

            # Step 5: Run Pipeline (if requested)
            if run_pipeline:
                self.stdout.write(
                    self.style.WARNING(
                        f"\n[5/6] Running pipeline for {len(batches)} batches..."
                    )
                )
                for i, batch in enumerate(batches, 1):
                    self.stdout.write(
                        self.style.WARNING(
                            f"  [{i}/{len(batches)}] Running batch {str(batch.id)[:8]}..."
                        )
                    )
                    try:
                        run_assembly_analysis_pipeline_batch(batch)
                        batch.refresh_from_db()
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"    ✓ Batch {str(batch.id)[:8]} completed (state: {batch.asa_state})"
                            )
                        )
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(
                                f"    ✗ Batch {str(batch.id)[:8]} failed: {e}"
                            )
                        )
            else:
                self.stdout.write(
                    self.style.WARNING(
                        "\n[5/6] Skipping pipeline execution (use --run-pipeline to execute)"
                    )
                )

            # Step 6: Summary
            self.stdout.write(self.style.SUCCESS("\n[6/6] Summary"))
            self._print_summary(mgnify_study, batches)

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n{'='*70}\n✓ Command completed successfully\n{'='*70}\n"
                )
            )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n✗ Error: {e}"))
            raise CommandError(str(e))

    def _get_or_create_study(
        self, study_accession: str, biome_path: str = None
    ) -> analyses.models.Study:
        """
        Get or create study from ENA.

        :param study_accession: Study accession
        :param biome_path: Optional biome path (e.g., root.engineered)
        :return: MGnify Study object
        """
        # Get/create ENA study
        ena_study = ena.models.Study.objects.get_ena_study(study_accession)
        if not ena_study:
            ena_study = get_study_from_ena(study_accession)
            ena_study.refresh_from_db()

        # Get/create MGnify study
        mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
            study_accession
        )
        mgnify_study.refresh_from_db()

        # Set biome if provided and study doesn't have one
        if biome_path and not mgnify_study.biome:
            try:
                biome = analyses.models.Biome.objects.get(path=biome_path)
                mgnify_study.biome = biome
                mgnify_study.save()
            except analyses.models.Biome.DoesNotExist:
                raise CommandError(
                    f"Biome '{biome_path}' not found. Use --biome with a valid biome path."
                )

        # Check if biome is set
        if not mgnify_study.biome:
            raise CommandError(
                f"Study {mgnify_study.accession} has no biome. Use --biome to set one "
                f"(e.g., --biome root.engineered)"
            )

        return mgnify_study

    def _print_summary(
        self,
        study: analyses.models.Study,
        batches: list[analyses.models.AssemblyAnalysisBatch],
    ):
        """
        Print summary of created batches.

        :param study: MGnify Study
        :param batches: List of created batches
        """
        total_analyses = sum(batch.total_analyses for batch in batches)

        self.stdout.write(f"  Study: {study.accession}")
        self.stdout.write(f"  Total batches: {len(batches)}")
        self.stdout.write(f"  Total analyses: {total_analyses}")

        # Count analyses by state
        all_analyses = study.analyses.filter(assembly_analysis_batch__in=batches)
        completed_count = all_analyses.filter_by_statuses(
            [analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
        ).count()
        pending_count = total_analyses - completed_count

        self.stdout.write(f"    - Completed: {completed_count}")
        self.stdout.write(f"    - Pending: {pending_count}")

        # Batch states
        self.stdout.write("\n  Batch states:")
        for state in analyses.models.PipelineState:
            count = sum(1 for b in batches if b.asa_state == state.value)
            if count > 0:
                self.stdout.write(f"    - {state.label}: {count}")
