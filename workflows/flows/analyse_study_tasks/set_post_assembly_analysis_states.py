import csv
from pathlib import Path
from typing import List

from prefect import task
from prefect.tasks import task_input_hash

from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates

from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


@task(
    cache_key_fn=task_input_hash,
)
def set_post_assembly_analysis_states(
    assembly_current_outdir: Path, assembly_analyses: List
):
    """
    Set post-analysis states for assembly analyses based on the output of the pipeline.
    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param assembly_analyses: List of assembly analyses
    """
    # The pipeline produces top level end of execution reports, which contain
    # the list of the assemblies that were completed, and those that were not.
    # Similar to the amplicon pipeline

    # qc_failed_assemblies.csv: assemblyID,reason
    qc_failed_csv = Path(f"{assembly_current_outdir}/qc_failed_assemblies.csv")
    qc_failed_assemblies = {}  # Stores {assembly_accession, qc_fail_reason}

    if qc_failed_csv.is_file():
        with qc_failed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                assembly_accession, fail_reason = row
                qc_failed_assemblies[assembly_accession] = fail_reason

    # qc_passed_assemblies.csv: assemblyID, info
    qc_completed_csv = Path(f"{assembly_current_outdir}/qc_passed_assemblies.csv")
    qc_completed_assemblies = {}  # Stores {assembly_accession, info}

    if qc_completed_csv.is_file():
        with qc_completed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                assembly_accession, info = row
                qc_completed_assemblies[assembly_accession] = info

    for analysis in assembly_analyses:
        if analysis.assembly.first_accession in qc_failed_assemblies:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_QC_FAILED,
                reason=qc_failed_assemblies[analysis.assembly.first_accession],
            )
        elif analysis.assembly.first_accession in qc_completed_assemblies:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_COMPLETED,
                reason=qc_completed_assemblies[analysis.assembly.first_accession],
                unset_statuses=[
                    AnalysisStates.ANALYSIS_FAILED,
                    AnalysisStates.ANALYSIS_BLOCKED,
                ],
            )
        else:
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_FAILED,
                reason="Missing assembly in execution",
            )
