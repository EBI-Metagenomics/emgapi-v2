from prefect import get_run_logger

from analyses.models import Study
import ena.models
from workflows.ena_utils.ena_api_requests import get_study_from_ena
from workflows.prefect_utils.flows_utils import django_db_task as task


@task
def get_or_create_mgnify_study(study_accession: str) -> int:
    """
    Get an MGnify Study by ENA accession, fetching the ENA Study first if needed.
    """
    logger = get_run_logger()
    try:
        mgnify_study = Study.objects.get_by_accession(study_accession)
    except Study.DoesNotExist:
        ena_study = ena.models.Study.objects.get_ena_study(study_accession)
        if not ena_study:
            get_study_from_ena(study_accession)
        mgnify_study = Study.objects.get_or_create_for_ena_study(study_accession)

    logger.info(
        f"Using MGnify study {mgnify_study.accession} for ENA study "
        f"{mgnify_study.ena_study.accession}"
    )
    return mgnify_study.id
