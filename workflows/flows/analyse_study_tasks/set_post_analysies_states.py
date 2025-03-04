import csv
from pathlib import Path
from typing import List

from prefect import task

from activate_django_first import EMG_CONFIG
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates

from workflows.flows.analyse_study_tasks.sanity_check_amplicon_results import (
    sanity_check_amplicon_results,
)
from workflows.prefect_utils.analyses_models_helpers import task_mark_analysis_status
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def set_post_analysis_states(amplicon_current_outdir: Path, amplicon_analyses: List):
    # The pipeline produces top level end of execution reports, which contain
    # the list of the runs that were completed, and those that were not.
    # For more information: https://github.com/EBI-Metagenomics/amplicon-pipeline?tab=readme-ov-file#top-level-reports

    # qc_failed_runs.csv: runID,reason(seqfu_fail/sfxhd_fail/libstrat_fail/no_reads)
    qc_failed_csv = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.failed_runs_csv}"
    )
    qc_failed_runs = {}  # Stores {run_accession, qc_fail_reason}

    if qc_failed_csv.is_file():
        with qc_failed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, fail_reason = row
                qc_failed_runs[run_accession] = fail_reason

    # qc_passed_runs.csv: runID, info(all_results/no_asvs)
    qc_completed_csv = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.completed_runs_csv}"
    )
    qc_completed_runs = {}  # Stores {run_accession, qc_fail_reason}

    if qc_completed_csv.is_file():
        with qc_completed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, info = row
                qc_completed_runs[run_accession] = info

    for analysis in amplicon_analyses:
        if analysis.run.first_accession in qc_failed_runs:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_QC_FAILED,
                reason=qc_failed_runs[analysis.run.first_accession],
            )
        elif analysis.run.first_accession in qc_completed_runs:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_COMPLETED,
                reason=qc_completed_runs[analysis.run.first_accession],
                unset_statuses=[
                    AnalysisStates.ANALYSIS_FAILED,
                    AnalysisStates.ANALYSIS_BLOCKED,
                ],
            )
            sanity_check_amplicon_results(
                Path(f"{amplicon_current_outdir}/{analysis.run.first_accession}"),
                analysis,
            )
        else:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_FAILED,
                reason="Missing run in execution",
            )
