from prefect import task, get_run_logger

import ena.models
from analyses.models import Study, Biome
from workflows.data_io_utils.legacy_emg_dbs import LegacyStudy, LegacyBiome


@task
def make_study_from_legacy_emg_db(
    legacy_study: LegacyStudy, legacy_biome: LegacyBiome, is_private: bool = False
) -> Study:
    logger = get_run_logger()

    ena_study, created = ena.models.Study.objects.update_or_create(
        accession__in=[legacy_study.ext_study_id, legacy_study.project_id],
        defaults={
            "is_private": is_private,
            "webin_submitter": legacy_study.submission_account_id,
        },
        creating_fields={
            "accession": legacy_study.ext_study_id,
            "additional_accessions": [legacy_study.project_id],
            "title": legacy_study.study_name,
        },
    )
    if created:
        logger.warning(f"Created new ENA study object {ena_study}")

    biome, created = Biome.objects.get_or_create(
        path=Biome.lineage_to_path(legacy_biome.lineage),
        defaults={"biome_name": legacy_biome.biome_name},
    )
    if created:
        logger.warning(f"Created new Biome object {biome}")

    mg_study, created = Study.objects.get_or_create(
        id=legacy_study.id,
        defaults={
            "ena_study": ena_study,
            "title": legacy_study.study_name,
            "ena_accessions": [legacy_study.ext_study_id, legacy_study.project_id],
            "biome": biome,
        },
    )
    if created:
        logger.warning(f"Created new study object {mg_study}")
    return mg_study
