from pathlib import Path
from typing import List

from prefect import flow, task

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.assembly import (
    import_taxonomy,
    import_qc,
    import_functions,
)
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


@task
def import_completed_assembly_analysis(analysis: analyses.models.Analysis):
    """
    Import results for a completed assembly analysis.
    :param analysis: The analysis to import results for
    """
    analysis.refresh_from_db()
    dir_for_analysis = Path(analysis.results_dir)

    import_qc(analysis, dir_for_analysis, allow_non_exist=False)

    # Import taxonomy
    import_taxonomy(analysis, dir_for_analysis)

    # Import functional annotations
    import_functions(analysis, dir_for_analysis)

    # TODO: Import pathways

    # Mark the analysis as having its annotations imported
    mark_analysis_status(
        analysis,
        analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
        unset_statuses=[
            analysis.AnalysisStates.ANALYSIS_QC_FAILED,
            analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
            analysis.AnalysisStates.ANALYSIS_QC_FAILED,
            analysis.AnalysisStates.ANALYSIS_BLOCKED,
        ],
    )


@flow(log_prints=True)
def import_completed_assembly_analyses(
    assembly_current_outdir: Path, assembly_analyses: List[analyses.models.Analysis]
):
    """
    Import results for completed assembly analyses.
    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param assembly_analyses: List of assembly analyses
    """
    for analysis in assembly_analyses:
        analysis.refresh_from_db()
        if not analysis.status.get(AnalysisStates.ANALYSIS_COMPLETED):
            print(f"{analysis} is not completed successfully. Skipping.")
            continue
        if analysis.status.get(AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED):
            print(f"{analysis} failed post-analysis sanity check. Skipping.")
            continue
        if analysis.annotations.get(analysis.TAXONOMIES):
            print(f"{analysis} already has taxonomic annotations. Skipping.")
            continue

        dir_for_analysis = assembly_current_outdir / analysis.assembly.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        try:
            import_completed_assembly_analysis(analysis)
        except Exception as e:
            print(f"{analysis} failed import! {e}")
            analysis.mark_status(
                analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                reason=f"Failed during import: {e}",
            )
