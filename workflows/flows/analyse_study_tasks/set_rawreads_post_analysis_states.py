import csv
from pathlib import Path
from typing import List

from prefect import task

from activate_django_first import EMG_CONFIG
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates

from workflows.flows.analyse_study_tasks.sanity_check_rawreads_results import (
    sanity_check_rawreads_results,
)
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


@task()
def set_post_analysis_states(current_outdir: Path, rawreads_analyses: List):
    # The pipeline produces top level end of execution reports, which contain
    # the list of the runs that were completed, and those that were not.

    def parse_pipeline_report(fp: Path):
        runs = {}
        if fp.is_file():
            with fp.open(mode="r") as file_handle:
                for row in csv.reader(file_handle, delimiter=","):
                    run_accession, fail_reason = row
                    runs[run_accession] = fail_reason
        return runs

    qc_failed_csv = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.failed_runs_csv}"
    )
    qc_failed_runs = parse_pipeline_report(
        qc_failed_csv
    )  # Stores {run_accession, qc_fail_reason}

    # qc_passed_runs.csv: runID, info(all_results/no_asvs)
    qc_completed_csv = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.completed_runs_csv}"
    )
    qc_completed_runs = parse_pipeline_report(
        qc_completed_csv
    )  # Stores {run_accession, qc_info}

    for analysis in rawreads_analyses:
        if analysis.run.first_accession in qc_failed_runs:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_QC_FAILED,
                reason=qc_failed_runs[analysis.run.first_accession],
            )
        elif analysis.run.first_accession in qc_completed_runs:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_COMPLETED,
                reason=qc_completed_runs[analysis.run.first_accession],
                unset_statuses=[
                    AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                    AnalysisStates.ANALYSIS_QC_FAILED,
                    AnalysisStates.ANALYSIS_FAILED,
                    AnalysisStates.ANALYSIS_BLOCKED,
                ],
            )
            sanity_check_rawreads_results(
                Path(f"{current_outdir}/{analysis.run.first_accession}"),
                analysis,
            )
        else:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_FAILED,
                reason="Missing run in execution",
            )
