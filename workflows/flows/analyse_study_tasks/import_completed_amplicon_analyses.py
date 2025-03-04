from pathlib import Path
from typing import List

from prefect import flow

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.amplicon import (
    import_qc,
    import_taxonomy,
    import_asv,
)
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.flows.analyse_study_tasks.copy_amplicon_pipeline_results import (
    copy_amplicon_pipeline_results,
)


@flow(log_prints=True)
def import_completed_analysis(
    amplicon_current_outdir: Path, amplicon_analyses: List[analyses.models.Analysis]
):
    for analysis in amplicon_analyses:
        analysis.refresh_from_db()
        if not analysis.status.get(AnalysisStates.ANALYSIS_COMPLETED):
            print(f"{analysis} is not completed successfully. Skipping.")
            continue
        if analysis.annotations.get(analysis.TAXONOMIES):
            print(f"{analysis} already has taxonomic annotations. Skipping.")
            continue

        dir_for_analysis = amplicon_current_outdir / analysis.run.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        import_qc(analysis, dir_for_analysis)

        for source in analyses.models.Analysis.TaxonomySources:
            import_taxonomy(
                analysis, dir_for_analysis, source=source, allow_non_exist=True
            )
        import_asv(analysis, dir_for_analysis)
        copy_amplicon_pipeline_results(analysis.accession)
