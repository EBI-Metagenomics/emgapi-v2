from pathlib import Path
from typing import List

from prefect import flow, task, get_run_logger

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.assembly import (
    create_assembly_v6_schema,
)
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.flows.analyse_study_tasks.copy_v6_pipeline_results import (
    copy_v6_pipeline_results,
)
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


@task
def import_completed_assembly_analysis(analysis: analyses.models.Analysis):
    """
    Import results for a completed assembly analysis using the unified schema.

    :param analysis: The analysis to import results for
    """
    analysis.refresh_from_db()
    dir_for_analysis = Path(analysis.results_dir)

    # Use the unified schema for both validation and import
    schema = create_assembly_v6_schema()

    # First validate the directory structure (optional but recommended)
    try:
        schema.validate_directory_structure(
            dir_for_analysis.parent,  # Parent because results_dir includes assembly_id
            analysis.assembly.first_accession,
        )
    except Exception as e:
        print(f"Validation warning for {analysis}: {e}. Proceeding with import anyway.")

    # Import all results using the unified schema
    schema.import_analysis_results(analysis, dir_for_analysis.parent)

    # Mark the analysis as having its annotations imported
    copy_v6_pipeline_results(analysis.accession)

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
    logger = get_run_logger()

    for analysis in assembly_analyses:
        analysis.refresh_from_db()
        if not analysis.status.get(AnalysisStates.ANALYSIS_COMPLETED):
            logger.info(f"{analysis} is not completed successfully. Skipping.")
            continue
        if analysis.status.get(AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED):
            logger.info(f"{analysis} failed post-analysis sanity check. Skipping.")
            continue

        dir_for_analysis = assembly_current_outdir / analysis.assembly.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        try:
            logger.info(f"Importing the results for {analysis}")
            import_completed_assembly_analysis(analysis)
        except Exception as e:
            logger.info(f"{analysis} failed import! {e}")
            analysis.mark_status(
                analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                reason=f"Failed during import: {e}",
            )
