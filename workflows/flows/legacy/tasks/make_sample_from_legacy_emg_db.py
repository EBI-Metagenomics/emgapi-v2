from prefect import task, get_run_logger

import ena.models
from analyses.models import Study, Sample
from workflows.data_io_utils.legacy_emg_dbs import LegacySample


@task
def make_sample_from_legacy_emg_db(legacy_sample: LegacySample, study: Study) -> Sample:
    logger = get_run_logger()

    ena_sample, created = ena.models.Sample.objects.get_or_create(
        accession__in=[legacy_sample.primary_accession, legacy_sample.ext_sample_id],
        defaults={
            "accession": legacy_sample.primary_accession,
            "additional_accessions": [legacy_sample.ext_sample_id],
            "study": study.ena_study,
        },
    )
    if created:
        logger.warning(f"Created new ENA sample object {ena_sample}")

    mg_sample, created = Sample.objects.get_or_create(
        ena_sample=ena_sample,
        ena_study=study.ena_study,
        defaults={
            "ena_accessions": [
                legacy_sample.primary_accession,
                legacy_sample.ext_sample_id,
            ],
        },
    )
    if created:
        logger.warning(f"Created new sample object {mg_sample}")
    return mg_sample
