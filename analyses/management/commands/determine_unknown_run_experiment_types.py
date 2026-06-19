from __future__ import annotations

import csv
import logging
from pathlib import Path

from django.core.management.base import BaseCommand

from analyses.models import Analysis, Run
from workflows.ena_utils.ena_policies import (
    ENALibrarySourcePolicy,
    ENALibraryStrategyPolicy,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Determine UNKNOWN run experiment types from analyses, assemblies, or ENA "
        "metadata, and write a TSV report of changed and unresolved runs."
    )

    report_headers = [
        "run_accession",
        "study_accession",
        "analysis_accessions",
        "library_strategy",
        "library_source",
        "scientific_name",
        "previous_experiment_type",
        "new_experiment_type",
        "status",
        "reason",
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            "--output-tsv",
            default="unknown_run_experiment_type_report.tsv",
            help="Path to the TSV report to write.",
        )

    def handle(self, *args, **options):
        output_tsv = Path(options["output_tsv"])
        report_rows = []
        changed_count = 0
        unresolved_count = 0

        runs = (
            Run.objects.filter(experiment_type=Run.ExperimentTypes.UNKNOWN)
            .select_related("study", "study__ena_study")
            .prefetch_related("analyses", "assemblies")
        )
        total_count = runs.count()
        logger.info(f"Checking {total_count} UNKNOWN run experiment types")

        for run in runs:
            previous_experiment_type = run.experiment_type
            reason = self.determine_experiment_type(run)
            run.refresh_from_db()

            if run.experiment_type != previous_experiment_type:
                changed_count += 1
                self.inherit_changed_run_experiment_type(run)
                logger.info(
                    f"Changed run {run.first_accession} from {previous_experiment_type} to {run.experiment_type} using {reason}"
                )
                report_rows.append(
                    self.report_row(run, previous_experiment_type, "changed", reason)
                )
            elif run.experiment_type == Run.ExperimentTypes.UNKNOWN:
                unresolved_count += 1
                logger.warning(
                    "Could not determine experiment type for run %s",
                    run.first_accession,
                )
                report_rows.append(
                    self.report_row(run, previous_experiment_type, "unresolved", reason)
                )

        self.write_report(output_tsv, report_rows)
        logger.info(
            "Finished checking UNKNOWN run experiment types: %s changed, %s unresolved, %s reported",
            changed_count,
            unresolved_count,
            len(report_rows),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Checked {total_count} UNKNOWN runs; changed {changed_count}; "
                f"unresolved {unresolved_count}; wrote {output_tsv}"
            )
        )

    def determine_experiment_type(self, run: Run) -> str:
        # if the UNKNOWN type run has analyses with known (curated) types, use those
        analysis_experiment_types = set(
            run.analyses.exclude(
                experiment_type__in=[None, "", Analysis.ExperimentTypes.UNKNOWN]
            ).values_list("experiment_type", flat=True)
        )

        if len(analysis_experiment_types) == 1:
            run.experiment_type = next(iter(analysis_experiment_types))
            run.save()
            return "analysis_experiment_type"

        # ... unless they conflict with each other
        if len(analysis_experiment_types) > 1:
            logger.error(
                "Run %s has analyses with conflicting experiment types: %s",
                run.first_accession,
                ", ".join(sorted(analysis_experiment_types)),
            )
            return "conflicting_analysis_experiment_types"

        # if the run has an assembly, assume it was metagenomic
        if run.assemblies.exists():
            run.experiment_type = Run.ExperimentTypes.METAGENOMIC
            run.save()
            return "assembly_attached"

        # otherwise use the default handling of ENA policies...
        # this will still leave some UNKNOWN because we don't know the context in which runs were being fectched,
        # e.g. if we were in an "amplicon analysis flow" we'd have assumed/ unknown things are probably amplicon
        metadata = run.metadata_preferring_inferred
        run.set_experiment_type_by_metadata(
            ena_library_strategy=metadata.get(Run.CommonMetadataKeys.LIBRARY_STRATEGY)
            or "",
            ena_library_source=metadata.get(Run.CommonMetadataKeys.LIBRARY_SOURCE)
            or "",
            ena_scientific_name=metadata.get(Run.CommonMetadataKeys.SCIENTIFIC_NAME)
            or "",
            library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            library_source_policy=ENALibrarySourcePolicy.OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME,
        )
        return "ena_metadata"

    def inherit_changed_run_experiment_type(self, run: Run) -> None:
        for analysis in run.analyses.filter(
            experiment_type=Analysis.ExperimentTypes.UNKNOWN
        ):
            analysis.inherit_experiment_type()

    def report_row(
        self, run: Run, previous_experiment_type: str, status: str, reason: str
    ) -> dict[str, str]:
        metadata = run.metadata_preferring_inferred
        return {
            "run_accession": run.first_accession or "",
            "study_accession": (
                run.study.ena_study.accession if run.study.ena_study else ""
            ),
            "analysis_accessions": ",".join(
                run.analyses.order_by("accession").values_list("accession", flat=True)
            ),
            "library_strategy": metadata.get(
                Run.CommonMetadataKeys.LIBRARY_STRATEGY, ""
            ),
            "library_source": metadata.get(Run.CommonMetadataKeys.LIBRARY_SOURCE) or "",
            "scientific_name": metadata.get(Run.CommonMetadataKeys.SCIENTIFIC_NAME)
            or "",
            "previous_experiment_type": previous_experiment_type or "",
            "new_experiment_type": run.experiment_type or "",
            "status": status,
            "reason": reason,
        }

    def write_report(self, output_tsv: Path, report_rows: list[dict[str, str]]) -> None:
        output_tsv.parent.mkdir(parents=True, exist_ok=True)
        with output_tsv.open("w", newline="") as report_file:
            writer = csv.DictWriter(
                report_file, fieldnames=self.report_headers, delimiter="\t"
            )
            writer.writeheader()
            writer.writerows(report_rows)
