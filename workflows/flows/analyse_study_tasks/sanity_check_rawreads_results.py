from pathlib import Path

from prefect import task, get_run_logger
from prefect.tasks import task_input_hash

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status


def validate_function_summary_folder(current_outdir, run_id, logger):
    function_summary_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.function_summary_folder}"
    )
    logger.info(
        f"Looking for {run_id} function summary folder in {function_summary_folder}"
    )

    if not function_summary_folder.exists():
        return f"No {EMG_CONFIG.rawreads_pipeline.function_summary_folder} folder"
    func_dbs = ["pfam"]
    for db in function_summary_folder.iterdir():
        if db.name in func_dbs:
            txt = Path(f"{db}/{run_id}_{db.name}.txt.gz")
            stats = Path(f"{db}/{run_id}_{db.name}.stats.json")
            if not (stats.exists()):
                return f"Missing stats.json file in {db}"
            if not (txt.exists()):
                return f"Missing txt.gz file in {db}"
        else:
            return f"unknown {db} in {EMG_CONFIG.rawreads_pipeline.function_summary_folder}"


def validate_taxonomy_summary_folder(current_outdir, run_id, logger):
    taxonomy_summary_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder}"
    )
    logger.info(
        f"Looking for {run_id} taxonomy summary folder in {taxonomy_summary_folder}"
    )

    if not taxonomy_summary_folder.exists():
        return f"No {EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder} folder"
    tax_dbs = ["silva-ssu", "silva-lsu", "motus"]
    for db in taxonomy_summary_folder.iterdir():
        if db.name in tax_dbs:
            html = Path(f"{db}/{run_id}_{db.name}.html")
            txt = Path(f"{db}/{run_id}_{db.name}.txt.gz")
            if not (html.exists()):
                return f"Missing html file in {db}"
            if not (txt.exists()):
                return f"Missing txt.gz file in {db}"
        else:
            return f"unknown {db} in {EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder}"


def validate_qc_folder(current_outdir, run_id, logger):
    qc_folder = Path(f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.qc_folder}")
    logger.info(f"Looking for {run_id} QC folder in {qc_folder}")

    if not qc_folder.exists():
        return f"No {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"

    if not Path(f"{qc_folder}/{run_id}_qc.fastp.json").exists():
        return f"No required QC fastp.json in {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"
    if not Path(f"{qc_folder}/{run_id}_decontamination.fastp.json").exists():
        return f"No required decontamination fastp.json in {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"
    if not Path(f"{qc_folder}/{run_id}_multiqc_report.html").exists():
        return f"No required multiqc report in {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"


@task(
    cache_key_fn=task_input_hash,
)
def sanity_check_rawreads_results(
    current_outdir: Path, analysis: analyses.models.Analysis
):
    """
    required:
     - qc
         - ${run_id}_qc.fastp.json
         - ${run_id}_decontamination.fastp.json
         - ${run_id}_multiqc_report.html
     - taxonomy-summary
         - silva-lsu
            - ${run_id}_silva-lsu.html
            - ${run_id}_silva-lsu.txt.gz
         - silva-ssu
            - ${run_id}_silva-ssu.html
            - ${run_id}_silva-ssu.txt.gz
         - motus
            - ${run_id}_motus.html
            - ${run_id}_motus.txt.gz
    optional:
     - function-summary
         - pfam
            - ${run_id}_pfam.txt.gz
            - ${run_id}_pfam.stats.json
    """
    logger = get_run_logger()
    run_id = analysis.run.first_accession

    validators = [
        # validate_function_summary_folder,
        validate_taxonomy_summary_folder,
        validate_qc_folder,
    ]

    for validator in validators:
        reason = validator(current_outdir, run_id, logger)
        if reason:
            logger.error(f"Validation failed: {reason}")
            mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                reason=reason,
            )

    logger.info(f"Post sanity check for {run_id} completed")
