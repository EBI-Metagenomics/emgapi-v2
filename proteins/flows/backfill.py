import os
from pathlib import Path

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from proteins.backfill import pending, submit
from workflows.prefect_utils.flows_utils import django_db_flow as flow


@flow(flow_run_name="Backfill the protein DB's accessions")
def proteindb_backfill(study: str | None = None):
    """Submits mgyp-accession for every completed v6 assembly analysis not yet registered, or only those of `study`.

    Does not wait for the tasks: running it again submits whatever is still unregistered.

    :param study: A study accession, to backfill only its analyses.
    """
    config = EMG_CONFIG.proteindb
    if not os.environ.get("PROTEINDB_DSN"):
        raise RuntimeError(
            "PROTEINDB_DSN, the proteindb_accession URI the tasks connect with, is not set"
        )
    if not config.accession_image:
        raise RuntimeError("EMG_PROTEINDB__ACCESSION_IMAGE is not set")
    logger = get_run_logger()
    pipeline = EMG_CONFIG.assembly_analysis_pipeline
    submitted = submit(
        Path(config.root) / "backfill",
        pending(pipeline.cds_folder, pipeline.qc_folder, study),
        config.accession_image,
        config.backfill_parallelism,
        config.backfill_array_size,
    )
    if submitted.missing:
        logger.warning(
            f"{len(submitted.missing)} analyses miss an input and were left out,"
            f" listed in {submitted.run / 'missing.tsv'}"
        )
    if submitted.jobs:
        logger.info(
            f"Submitted {submitted.ready} analyses, listed in {submitted.run / 'tasks.tsv'},"
            f" as SLURM jobs {', '.join(map(str, submitted.jobs))}"
        )
    else:
        logger.info("Nothing to submit")
    return submitted
