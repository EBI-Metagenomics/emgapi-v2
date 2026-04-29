from prefect import get_run_logger

import analyses.models
from workflows.ena_utils.ena_api_requests import get_study_readruns_from_ena
from workflows.prefect_utils.flows_utils import django_db_task as task


@task
def get_or_import_mgnify_run(
    run_accession: str,
    mgnify_study_id: int,
    read_run_limit: int,
) -> int:
    """
    Get an MGnify Run by accession, fetching the study's ENA read runs first if needed.
    """
    logger = get_run_logger()
    mgnify_study = analyses.models.Study.objects.get(id=mgnify_study_id)

    try:
        run = analyses.models.Run.objects.get(ena_accessions__contains=[run_accession])
    except analyses.models.Run.DoesNotExist:
        logger.info(
            f"Run {run_accession} is not in the local database. "
            f"Fetching read runs for study {mgnify_study.first_accession} from ENA."
        )
        get_study_readruns_from_ena(
            mgnify_study.first_accession,
            limit=read_run_limit,
            raise_on_empty=False,
        )
        try:
            run = analyses.models.Run.objects.get(
                ena_accessions__contains=[run_accession]
            )
        except analyses.models.Run.DoesNotExist:
            logger.error(
                f"Could not import Run {run_accession} from ENA. "
                "Run import from ENA may be needed before importing assemblies."
            )
            raise
    except analyses.models.Run.MultipleObjectsReturned:
        logger.error(f"Could not find exactly one Run for {run_accession}.")
        raise

    return run.id
