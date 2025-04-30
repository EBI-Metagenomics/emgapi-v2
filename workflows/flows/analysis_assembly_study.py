from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
)


@flow(
    name="Run assembly analysis v6 pipeline on a study",
    flow_run_name="Analyse assembly: {study_accession}",
)
def analysis_assembly_study(study_accession: str):
    """
    Get a study from ENA (or MGnify), and run assembly-v6 pipeline on its read-runs.
    :param study_accession: e.g. PRJ or ERP accession
    """
    logger = get_run_logger()
    EMG_CONFIG

    # Study fetching and creation
    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession)

    ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    mgnify_study.refresh_from_db()
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    # Get assemble-able runs
    assemblies = get_study_assemblies_from_ena(
        ena_study.accession,
        limit=10000,
    )
    logger.info(f"Returned {len(assemblies)} assemblies from ENA portal API")
