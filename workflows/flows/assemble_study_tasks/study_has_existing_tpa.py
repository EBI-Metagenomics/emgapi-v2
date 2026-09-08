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
    """
    Check if a study seems to have an existing TPA (Third Party Assembly) study.

    This queries the ENA portal API twice: once to fetch all known study accessions for the reads study,
    and then again to check if any of those accessions are mentioned in the title of another study.

    This relies on a convention to entitle TPA studies things like "Third Party Assembly of PRJxxx",
    or "Metagenome assembly of ERPxxx", which is not enforced but is followed e.g. by MGnify and SPIRE
    (e.g. https://github.com/EBI-Metagenomics/assembly_uploader/ does this).

    N.B. this suppresses (with a warning) ENA fetch failures since these are expected for private data etc.

    :param reads_study_accession: The accession of the reads study.
    :return: True if the study has a plausible existing TPA study, False otherwise.
    """
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
        reads_study_accessions = [
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
                    for accession in reads_study_accessions
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
