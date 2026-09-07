import operator
from functools import reduce

from prefect import get_run_logger, task

from workflows.ena_utils.abstract import ENAPortalResultType
from workflows.ena_utils.ena_api_requests import (
    get_available_study_assembly_accessions,
)
from workflows.ena_utils.requestors import (
    ENAAccessException,
    ENAAPIRequest,
    ENAAvailabilityException,
)
from workflows.ena_utils.study import ENAStudyFields, ENAStudyQuery


@task(task_run_name="Check for an existing assembly TPA: {reads_study_accession}")
def study_has_existing_tpa(reads_study_accession: str) -> bool:
    logger = get_run_logger()
    try:
        reads_studies = ENAAPIRequest(
            result=ENAPortalResultType.STUDY,
            query=ENAStudyQuery(study_accession=reads_study_accession)
            | ENAStudyQuery(secondary_study_accession=reads_study_accession),
            fields=[
                ENAStudyFields.STUDY_ACCESSION,
                ENAStudyFields.SECONDARY_STUDY_ACCESSION,
            ],
        ).get()
        reads_study = reads_studies[0]
        reads_accessions = [
            accession
            for accession in (
                reads_study.get(ENAStudyFields.STUDY_ACCESSION),
                reads_study.get(ENAStudyFields.SECONDARY_STUDY_ACCESSION),
            )
            if accession
        ]
        assembly_studies = ENAAPIRequest(
            result=ENAPortalResultType.STUDY,
            query=reduce(
                operator.or_,
                [
                    ENAStudyQuery(study_title=accession)
                    for accession in reads_accessions
                ],
            ),
            fields=[
                ENAStudyFields.STUDY_ACCESSION,
                ENAStudyFields.SECONDARY_STUDY_ACCESSION,
                ENAStudyFields.STUDY_TITLE,
            ],
        ).get(raise_on_empty=False)
        for study in assembly_studies:
            accessions = filter(
                None,
                (
                    study.get(ENAStudyFields.STUDY_ACCESSION),
                    study.get(ENAStudyFields.SECONDARY_STUDY_ACCESSION),
                ),
            )
            logger.info(
                f"Plausible assembly TPA study {', '.join(accessions)}: "
                f"{study.get(ENAStudyFields.STUDY_TITLE, '')}"
            )
        assemblies = (
            get_available_study_assembly_accessions(
                [study[ENAStudyFields.STUDY_ACCESSION] for study in assembly_studies]
            )
            if assembly_studies
            else set()
        )
    except (ENAAccessException, ENAAvailabilityException, IndexError) as error:
        logger.warning(
            f"Could not check ENA for existing assemblies of {reads_study_accession}: {error}"
        )
        return False

    return bool(assemblies)
