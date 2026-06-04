from pathlib import Path
from typing import Optional

from django.conf import settings
from django.core.management.base import BaseCommand

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.amplicon import (
    import_primer_identification,
)

V6_PRIMER_IDENTIFICATION_PIPELINE_VERSIONS = (
    analyses.models.Analysis.PipelineVersions.v6,
    analyses.models.Analysis.PipelineVersions.v6_1,
)


def import_primer_identification_for_analysis(
    analysis_id: int,
) -> tuple[str, bool, Optional[str]]:
    """
    Import primer-identification outputs for a single analysis, if possible.

    Returns a tuple of (analysis_accession, success, error_message_if_any).

    :param analysis_id: Analysis primary key to process.
    """
    try:
        analysis = analyses.models.Analysis.objects.select_related("run").get(
            id=analysis_id
        )
    except analyses.models.Analysis.DoesNotExist:  # type: ignore[attr-defined]
        return (f"<id:{analysis_id}>", False, "Analysis no longer exists")

    analysis.refresh_from_db()

    if analysis.experiment_type != analysis.ExperimentTypes.AMPLICON:
        return (analysis.accession, False, "Not an amplicon analysis")

    if analysis.pipeline_version not in V6_PRIMER_IDENTIFICATION_PIPELINE_VERSIONS:
        return (analysis.accession, False, "Pipeline version is older than v6")

    if not analysis.status.get(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED):
        return (analysis.accession, False, "Annotations not yet imported")

    if not analysis.results_dir:
        return (analysis.accession, False, "results_dir is not set for this analysis")

    dir_for_analysis = Path(analysis.results_dir)

    try:
        import_primer_identification(analysis, dir_for_analysis, allow_non_exist=True)
        return (analysis.accession, True, None)
    except Exception as e:
        return (analysis.accession, False, str(e))


class Command(BaseCommand):
    help = (
        "Import primer-identification outputs for v6+ amplicon analyses with "
        "ANALYSIS_ANNOTATIONS_IMPORTED set."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-count",
            type=int,
            default=None,
            help="Optional safety cap limiting how many analyses to process in one run.",
        )

    def handle(self, *args, **options):
        """
        Import available primer-identification outputs for eligible amplicon analyses.

        :param args: Positional command arguments, unused.
        :param options: Parsed command options.
        """
        assert Path(settings.EMG_CONFIG.slurm.default_workdir).exists()
        max_count = options["max_count"]
        q = (
            analyses.models.Analysis.objects.select_related("run")
            .filter(
                experiment_type=analyses.models.Analysis.ExperimentTypes.AMPLICON,
                pipeline_version__in=V6_PRIMER_IDENTIFICATION_PIPELINE_VERSIONS,
            )
            .filter_by_statuses(
                [analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            )
            .order_by("id")
        )

        if max_count is not None:
            q = q[:max_count]

        count = q.count()
        self.stdout.write(
            "Found "
            f"{count} v6+ amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED "
            "to check for primer-identification outputs"
        )

        successes = 0
        failures = 0

        for analysis in q:
            acc, ok, err = import_primer_identification_for_analysis(analysis.id)
            if ok:
                successes += 1
                self.stdout.write(
                    f"[{acc}] primer-identification import done (if files present)"
                )
            else:
                failures += 1
                self.stdout.write(f"[{acc}] skipped/failed: {err}")

        self.stdout.write(
            "Primer-identification import complete. "
            f"Successes={successes}, Failures/Skipped={failures}"
        )
