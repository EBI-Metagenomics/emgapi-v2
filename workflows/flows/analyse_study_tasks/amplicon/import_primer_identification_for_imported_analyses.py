from pathlib import Path
from typing import Optional

from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
    django_db_task as task,
)

from activate_django_first import EMG_CONFIG  # noqa: F401  # Ensure Django is setup

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.amplicon import (
    import_primer_identification,
)


@task()
def import_primer_identification_for_analysis(
    analysis_id: int,
) -> tuple[str, bool, Optional[str]]:
    """
    Import primer-identification outputs for a single analysis, if possible.

    Returns a tuple of (analysis_accession, success, error_message_if_any)
    """
    try:
        analysis = analyses.models.Analysis.objects.select_related("run").get(
            id=analysis_id
        )
    except analyses.models.Analysis.DoesNotExist:  # type: ignore[attr-defined]
        return (f"<id:{analysis_id}>", False, "Analysis no longer exists")

    analysis.refresh_from_db()

    # Only handle amplicon analyses which already have annotations imported
    if analysis.experiment_type != analysis.ExperimentTypes.AMPLICON:
        return (analysis.accession, False, "Not an amplicon analysis")

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


@flow(
    name="Import primer-identification for ANALYSIS_ANNOTATIONS_IMPORTED amplicon analyses",
    log_prints=True,
)
def import_primer_identification_for_imported_analyses(
    max_count: Optional[int] = None,
) -> None:
    """
    Find all amplicon analyses with status ANALYSIS_ANNOTATIONS_IMPORTED=True and
    import any available primer-identification outputs for each.

    Args:
        max_count: Optional safety cap limiting how many analyses to process in one run.
    """
    # Build queryset: amplicon analyses with annotations already imported
    q = (
        analyses.models.Analysis.objects.select_related("run")
        .filter(
            experiment_type=analyses.models.Analysis.ExperimentTypes.AMPLICON,
        )
        .filter_by_statuses(
            [analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
        )
    )

    if max_count is not None:
        q = q.order_by("id")[:max_count]

    count = q.count()
    print(
        f"Found {count} amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED to check for primer-identification outputs"
    )

    successes = 0
    failures = 0

    for analysis in q:
        acc, ok, err = import_primer_identification_for_analysis(analysis.id)
        if ok:
            successes += 1
            print(f"[{acc}] primer-identification import done (if files present)")
        else:
            failures += 1
            print(f"[{acc}] skipped/failed: {err}")

    print(
        f"Primer-identification import complete. Successes={successes}, Failures/Skipped={failures}"
    )
