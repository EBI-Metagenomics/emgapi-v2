from prefect import task, get_run_logger

from analyses.models import Study, Run, Sample
from workflows.data_io_utils.legacy_emg_dbs import LegacyRun, LEGACY_EXPERIMENT_TYPE_MAP


@task
def make_run_from_legacy_emg_db(legacy_run: LegacyRun, study: Study) -> Run:
    assert (
        legacy_run.experiment_type_id in LEGACY_EXPERIMENT_TYPE_MAP.keys()
    ), f"Legacy run {legacy_run.run_id} is not known. Experiment type is {legacy_run.experiment_type_id}. Known experiment types are {LEGACY_EXPERIMENT_TYPE_MAP.keys()}"

    logger = get_run_logger()

    sample = Sample.objects.get(
        ena_accessions__contains=[legacy_run.sample.primary_accession]
    )

    run, created = Run.objects.get_or_create(
        ena_study=study.ena_study,
        study=study,
        sample=sample,
        experiment_type=LEGACY_EXPERIMENT_TYPE_MAP[legacy_run.experiment_type_id],
        ena_accessions=list(
            {legacy_run.accession, legacy_run.secondary_accession}
        ),  # dedupes
        metadata={
            Run.CommonMetadataKeys.INSTRUMENT_PLATFORM: legacy_run.instrument_platform,
            Run.CommonMetadataKeys.INSTRUMENT_MODEL: legacy_run.instrument_model,
        },
    )
    if created:
        logger.info(f"Created new run object {run}")
    return run
