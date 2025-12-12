from pathlib import Path

from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG


@flow
def generate_dwc_ready_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> list[Path] | None:
    logger = get_run_logger()
    logger.warning("Not implemented yet")
    # TODO: implement me :)
    return []


@flow
def merge_dwc_ready_summaries(
    mgnify_study_accession: str,
    cleanup_partials: bool = False,
    bludgeon: bool = True,
) -> list[Path] | None:
    logger = get_run_logger()
    logger.warning("Not implemented yet")
    # TODO: implement me :)
    return []
