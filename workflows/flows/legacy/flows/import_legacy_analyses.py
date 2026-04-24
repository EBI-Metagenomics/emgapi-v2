from pathlib import Path

from prefect import get_run_logger
from sqlalchemy import select

import activate_django_first  # noqa

from workflows.ena_utils.ena_api_requests import (
    sync_privacy_state_of_ena_study_and_derived_objects,
    sync_study_metadata_from_ena,
    sync_sample_metadata_from_ena,
)
from workflows.flows.legacy.tasks.make_assembly_from_legacy_emg_db import (
    make_assembly_from_legacy_emg_db,
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
    LEGACY_PIPELINE_ID_MAP,
)

from workflows.prefect_utils.flows_utils import django_db_flow as flow


@flow(
    name="Import Legacy (pre-V6) Analyses",
    flow_run_name="Import legacy analyses from study: {mgys}",
)
def import_legacy_analyses(mgys: str, fetch_functions: bool = False):
    """
    This flow will iteratively import analyses (made with MGnify V1-V5 pipelines)
    into the EMG DB.

    It connects to the legacy EMG MySQL and Mongo DBs, but not through Django.

    Functions (functional annotations) are not fetched by default due to this being a very slow
    read operation. Set fetch_functions=True to include them in the import.
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
            run, assembly = None, None
            if legacy_analysis.run:
                run = make_run_from_legacy_emg_db(legacy_analysis.run, study)
            elif legacy_analysis.assembly:
                assembly = make_assembly_from_legacy_emg_db(
                    legacy_analysis.assembly,
                    study,
                    sample,
                )
            else:
                logger.warning(
                    f"Analysis {legacy_analysis.job_id} has no linked run or assembly to import"
                )

            analysis, created = Analysis.objects.update_or_create(
                id=legacy_analysis.job_id,
                defaults={
                    "study": study,
                    "sample": sample,
                    "results_dir": legacy_analysis.result_directory,
                    "ena_study": study.ena_study,
                    "pipeline_version": LEGACY_PIPELINE_ID_MAP.get(
                        legacy_analysis.pipeline_id
                    ),
                    "run": run,
                    "assembly": assembly,
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
            if (
                fetch_functions
                and not analysis.experiment_type == analysis.ExperimentTypes.AMPLICON
            ):
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


@flow(
    name="Import all legacy analyses",
    flow_run_name="Import legacy analyses from all known studies",
)
def import_all_legacy_analyses(fetch_functions: bool = False):
    """
    Imports all legacy analyses from all known studies.

    This function retrieves a list of all studies and processes each study to
    import legacy analyses. It optionally fetches associated functions during
    the import process.

    :param fetch_functions: Boolean indicating whether to fetch associated
        functions during the import process.
    :type fetch_functions: bool
    :return: None
    """
    logger = get_run_logger()
    with legacy_emg_db_session() as session:
        study_ids_stmt = select(LegacyStudy.id)
        study_ids = session.scalars(study_ids_stmt).all()

    logger.info(f"Found {len(study_ids)} legacy studies to import")

    for study_id in study_ids:
        mgys = f"MGYS{study_id:08d}"
        logger.info(f"Importing study {mgys}")
        import_legacy_analyses(mgys=mgys, fetch_functions=fetch_functions)
