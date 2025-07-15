from pathlib import Path
from typing import List

from prefect import flow, task, get_run_logger

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.rawreads import (
    import_qc,
    import_taxonomy,
    import_functional,
)
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.flows.analyse_study_tasks.copy_v6_pipeline_results import (
    copy_v6_pipeline_results,
)
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


@task
def import_completed_analysis(analysis: analyses.models.Analysis):
    analysis.refresh_from_db()
    dir_for_analysis = Path(analysis.results_dir)

    import_qc(analysis, dir_for_analysis, allow_non_exist=False)

    t = analyses.models.Analysis.TaxonomySources
    for source in [
        t.MOTUS,
        t.LSU,
        t.SSU,
    ]:
        import_taxonomy(analysis, dir_for_analysis, source=source, allow_non_exist=True)
    t = analyses.models.Analysis.FunctionalSources
    for source in [
        t.PFAM,
    ]:
        import_functional(
            analysis, dir_for_analysis, source=source, allow_non_exist=True
        )

    copy_v6_pipeline_results(analysis.accession)
    mark_analysis_status(
        analysis,
        analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
        unset_statuses=[
            analysis.AnalysisStates.ANALYSIS_QC_FAILED,
            analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
            analysis.AnalysisStates.ANALYSIS_BLOCKED,
        ],
    )


@flow
def import_completed_analyses(
    rawreads_current_outdir: Path, rawreads_analyses: List[analyses.models.Analysis]
):
    logger = get_run_logger()

    for analysis in rawreads_analyses:
        analysis.refresh_from_db()
        if not analysis.status.get(AnalysisStates.ANALYSIS_COMPLETED):
            logger.warning(f"{analysis} is not completed successfully. Skipping.")
            continue
        if analysis.status.get(AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED):
            logger.warning(f"{analysis} failed post-analysis sanity check. Skipping.")
            continue
        if analysis.annotations.get(analysis.TAXONOMIES):
            logger.warning(f"{analysis} already has taxonomic annotations. Skipping.")
            continue
        if analysis.annotations.get(analysis.FUNCTIONAL_ANNOTATION):
            logger.warning(f"{analysis} already has functional annotations. Skipping.")
            continue

        dir_for_analysis = rawreads_current_outdir / analysis.run.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        try:
            import_completed_analysis(analysis)
        except Exception as e:
            # TODO: there shouldn't really be cases where sanity passes but import fails... but currently there are.
            logger.warning(f"{analysis} failed import! {e}")
            analysis.mark_status(
                analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                reason=f"Failed during import: {e}",
            )
