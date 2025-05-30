from pathlib import Path

import django
from prefect import flow, get_run_logger, task
from sqlalchemy import select

django.setup()

import ena.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis, Biome, Run, Sample, Study
from workflows.data_io_utils.legacy_emg_dbs import (
    LEGACY_DOWNLOAD_TYPE_MAP,
    LEGACY_FILE_FORMATS_MAP,
    LegacyBiome,
    LegacyRun,
    LegacySample,
    LegacyStudy,
    get_taxonomy_from_api_v1_mongo,
    legacy_emg_db_session,
)


@task
def make_study_from_legacy_emg_db(
    legacy_study: LegacyStudy, legacy_biome: LegacyBiome
) -> Study:
    logger = get_run_logger()

    ena_study, created = ena.models.Study.objects.get_or_create(
        accession=legacy_study.ext_study_id,
        defaults={
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


@task
def make_sample_from_legacy_emg_db(legacy_sample: LegacySample, study: Study) -> Sample:
    logger = get_run_logger()

    ena_sample, created = ena.models.Sample.objects.get_or_create(
        accession=legacy_sample.primary_accession,
        defaults={
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


@task
def make_run_from_legacy_emg_db(legacy_run: LegacyRun, study: Study) -> Run:
    assert (
        legacy_run.experiment_type_id == 3
    ), f"Legacy run {legacy_run.run_id} is not amplicon. Experiment type is {legacy_run.experiment_type_id}"

    logger = get_run_logger()

    sample = Sample.objects.get(
        ena_accessions__contains=[legacy_run.sample.primary_accession]
    )

    run, created = Run.objects.get_or_create(
        ena_study=study.ena_study,
        study=study,
        sample=sample,
        experiment_type=Run.ExperimentTypes.AMPLICON,
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


@flow(
    name="Import V5 Amplicon Analyses",
    flow_run_name="Import V5 amplicon analyses from study: {mgys}",
)
def import_v5_amplicon_analyses(mgys: str):
    """
    This flow will iteratively import amplicon analyses (made with MGnify V5 pipeline)
    into the EMG DB.

    It connects to the legacy Mongo database server directly to copy data (it is big),
    but uses a TSV dump file of the legacy MySQL db (it is quite small).
    """

    logger = get_run_logger()

    study_id = int(mgys.upper().lstrip("MGYS"))

    with legacy_emg_db_session() as session:
        study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == study_id)
        legacy_study: LegacyStudy = session.scalar(study_select_stmt)
        logger.info(f"Got legacy study {legacy_study}")
        legacy_biome = legacy_study.biome

        study = make_study_from_legacy_emg_db(legacy_study, legacy_biome)

        for legacy_analysis in legacy_study.analysis_jobs:
            legacy_sample = legacy_analysis.sample
            sample = make_sample_from_legacy_emg_db(legacy_sample, study)
            run = make_run_from_legacy_emg_db(legacy_analysis.run, study)

            analysis, created = Analysis.objects.update_or_create(
                id=legacy_analysis.job_id,
                defaults={
                    "study": study,
                    "sample": sample,
                    "results_dir": legacy_analysis.result_directory,
                    "ena_study": study.ena_study,
                    "pipeline_version": Analysis.PipelineVersions.v5,
                    "run": run,
                },
            )
            analysis.inherit_experiment_type()

            if created:
                logger.info(f"Created analysis {analysis}")
            else:
                logger.warning(f"Updated analysis {analysis}")

            for legacy_download in legacy_analysis.downloads:
                basename = Path(legacy_download.real_name)

                if legacy_download.subdir:
                    path = Path(legacy_download.subdir.subdir) / basename
                else:
                    path = basename

                analysis.add_download(
                    DownloadFile(
                        path=str(path),
                        alias=legacy_download.alias,
                        long_description=legacy_download.description.description,
                        short_description=legacy_download.description.description_label,
                        download_type=LEGACY_DOWNLOAD_TYPE_MAP.get(
                            legacy_download.group_id, DownloadType.OTHER
                        ),
                        download_group="all",
                        file_type=LEGACY_FILE_FORMATS_MAP.get(
                            legacy_download.format_id, DownloadFileType.OTHER
                        ),
                    )
                )

            taxonomy = get_taxonomy_from_api_v1_mongo(analysis.accession)
            analysis.annotations[Analysis.TAXONOMIES] = taxonomy
            analysis.mark_status(analysis.AnalysisStates.ANALYSIS_STARTED)
            analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)
            analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
            analysis.save()
