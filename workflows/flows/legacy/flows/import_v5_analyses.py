from pathlib import Path

import django
from prefect import flow, get_run_logger
from sqlalchemy import select

from workflows.ena_utils.ena_api_requests import (
    sync_privacy_state_of_ena_study_and_derived_objects,
    sync_study_metadata_from_ena,
    sync_sample_metadata_from_ena,
)
from workflows.flows.legacy.tasks.make_run_from_legacy_emg_db import (
    make_run_from_legacy_emg_db,
)
from workflows.flows.legacy.tasks.make_sample_from_legacy_emg_db import (
    make_sample_from_legacy_emg_db,
)
from workflows.flows.legacy.tasks.make_study_from_legacy_emg_db import (
    make_study_from_legacy_emg_db,
)

django.setup()

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis
from workflows.data_io_utils.legacy_emg_dbs import (
    LEGACY_DOWNLOAD_TYPE_MAP,
    LEGACY_FILE_FORMATS_MAP,
    LegacyStudy,
    get_taxonomy_from_api_v1_mongo,
    legacy_emg_db_session,
    get_functions_from_api_v1_mongo,
)


@flow(
    name="Import V5 Analyses",
    flow_run_name="Import V5 analyses from study: {mgys}",
)
def import_v5_analyses(mgys: str):
    """
    This flow will iteratively import analyses (made with MGnify V5 pipeline)
    into the EMG DB.

    It connects to the legacy EMG MySQL and Mongo DBs, but not through Django.
    """

    logger = get_run_logger()

    study_id = int(mgys.upper().lstrip("MGYS"))

    with legacy_emg_db_session() as session:
        study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == study_id)
        legacy_study: LegacyStudy = session.scalar(study_select_stmt)
        logger.info(f"Got legacy study {legacy_study}")
        legacy_biome = legacy_study.biome

        study = make_study_from_legacy_emg_db(
            legacy_study,
            legacy_biome,
            is_private=legacy_study.is_private or False,  # null is_private -> false
        )

        for legacy_analysis in legacy_study.analysis_jobs:
            legacy_sample = legacy_analysis.sample
            sample = make_sample_from_legacy_emg_db(legacy_sample, study)
            sync_sample_metadata_from_ena(sample.ena_sample)
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
            analysis.refresh_from_db()
            analysis.annotations[Analysis.TAXONOMIES] = taxonomy
            if not analysis.experiment_type == analysis.ExperimentTypes.AMPLICON:
                functions = get_functions_from_api_v1_mongo(analysis.accession)
                analysis.annotations.update(functions)
            analysis.mark_status(analysis.AnalysisStates.ANALYSIS_STARTED, save=False)
            analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED, save=False)
            analysis.mark_status(
                analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED, save=False
            )
            analysis.save()

        sync_privacy_state_of_ena_study_and_derived_objects(study.ena_study)
        sync_study_metadata_from_ena(study.ena_study)
