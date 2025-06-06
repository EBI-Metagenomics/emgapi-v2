from pathlib import Path
from typing import List

from prefect import flow, task

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.amplicon import (
    import_qc,
    import_taxonomy,
    import_asv,
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
        t.UNITE,
        t.DADA2_PR2,
        t.DADA2_SILVA,
        t.ITS_ONE_DB,
        t.LSU,
        t.SSU,
        t.PR2,
    ]:
        import_taxonomy(analysis, dir_for_analysis, source=source, allow_non_exist=True)
    import_asv(analysis, dir_for_analysis)
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
def import_completed_analyses(
    amplicon_current_outdir: Path, amplicon_analyses: List[analyses.models.Analysis]
):
    for analysis in amplicon_analyses:
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

        dir_for_analysis = amplicon_current_outdir / analysis.run.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        try:
            import_completed_analysis(analysis)
        except Exception as e:
            # TODO: there shouldn't really be cases where sanity passes but import fails... but currently there are.
            print(f"{analysis} failed import! {e}")
            analysis.mark_status(
                analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                reason=f"Failed during import: {e}",
            )
