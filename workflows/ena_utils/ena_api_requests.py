import logging
from datetime import date
from enum import Enum
from typing import List, Literal, Optional, Type, TypeVar, Union

import httpx
from django.conf import settings
from ena_portal_api.constants import ENAPortalResultType, PAIRED_END_LIBRARY_LAYOUT, SINGLE_END_LIBRARY_LAYOUT, \
    METAGENOME_SCIENTIFIC_NAME, ALLOWED_LIBRARY_SOURCE
from ena_portal_api.ena_api_requests import ENAAPIRequest
# from ena_portal_api.ena_api_requests.ENAAPIRequest  import create_ena_api_request
from ena_portal_api.models.study import StudyFields as ENAStudyFields, StudyQuery as ENAStudyQuery
from httpx import Auth, Response
from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field, computed_field, field_serializer, model_validator
from typing_extensions import Self

import analyses.models
import ena.models
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash

EMG_CONFIG = settings.EMG_CONFIG

@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
async def get_study_from_ena(accession: str, limit: int = 10) -> ena.models.Study:
    logger = get_run_logger()

    logger.info(f"Will fetch from ENA Portal API Study {accession}")

    portal = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        fields=[
            ENAStudyFields[f.upper()] for f in EMG_CONFIG.ena.study_metadata_fields
        ],
        limit=limit,
        query=ENAStudyQuery(study_accession=accession)
        | ENAStudyQuery(secondary_study_accession=accession),
    ).get()

    if portal.status_code == httpx.codes.OK:
        response_json = portal.json()
        # Check if the response is empty
        if not response_json:
            raise Exception(f"No study found for accession {accession}")
        s = response_json[0]

        # Check secondary accession
        secondary_accession: str = s["secondary_study_accession"]
        additional_accessions: list = []
        if not secondary_accession:
            logger.warning(f"Study {accession} secondary_accession is not available")
        else:
            if len(secondary_accession.split(";")) > 1:
                logger.warning(
                    f"Study {accession} has more than one secondary_accession"
                )
                additional_accessions = secondary_accession.split(";")
            else:
                additional_accessions = [secondary_accession]

        # Check primary accession
        if not s["study_accession"]:
            logger.warning(
                f"Study {accession} primary_accession is not available. "
                f"Use first secondary accession as primary_accession"
            )
            if additional_accessions:
                primary_accession = additional_accessions[0]
            else:
                raise Exception(
                    f"Neither primary nor secondary accessions found for study {accession}"
                )
        else:
            primary_accession: str = s["study_accession"]

        study, created = await ena.models.Study.objects.aget_or_create(
            accession=primary_accession,
            defaults={
                "title": portal.json()[0]["study_title"],
                "additional_accessions": additional_accessions,
                # TODO: more metadata
            },
        )
        return study
    else:
        raise Exception(f"Bad status! {portal.status_code} {portal}")


def check_reads_fastq(fastq: list, run_accession: str, library_layout: str):
    logger = get_run_logger()
    sorted_fastq = sorted(fastq)  # to keep order [_1, _2, _3(?)]
    if not len(sorted_fastq):
        logger.warning(f"No fastq files for run {run_accession}")
        return False
    # potential single end
    elif len(sorted_fastq) == 1:
        if library_layout == PAIRED_END_LIBRARY_LAYOUT:
            logger.warning(
                f"Incorrect library_layout for {run_accession} having one fastq file"
            )
            return False
        if "_1.f" in sorted_fastq[0] or "_2.f" in sorted_fastq[0]:
            logger.warning(
                f"Single fastq file contains _1 or _2 for run {run_accession}"
            )
            return False
        else:
            logger.info(f"One fastq for {run_accession}: {sorted_fastq}")
            return sorted_fastq
    # potential paired end
    elif len(sorted_fastq) == 2:
        if library_layout == SINGLE_END_LIBRARY_LAYOUT:
            logger.warning(
                f"Incorrect library_layout for {run_accession} having two fastq files"
            )
            return False
        if "_1.f" in sorted_fastq[0] and "_2.f" in sorted_fastq[1]:
            logger.info(f"Two fastqs for {run_accession}: {sorted_fastq}")
            return sorted_fastq
        else:
            logger.warning(
                f"Incorrect names of fastq files for run {run_accession} (${sorted_fastq})"
            )
            return False
    elif len(fastq) > 2:
        logger.info(f"More than 2 fastq files provided for run {run_accession}")
        return sorted_fastq[:2]


@task(
    retries=10,
    cache_key_fn=context_agnostic_task_input_hash,
    retry_delay_seconds=60,
    task_run_name="Get study readruns from ENA: {accession}",
    log_prints=True,
)
def get_study_readruns_from_ena(
    accession: str,
    limit: int = 20,
    filter_library_strategy: str = None,
    extra_cache_hash: str = None,
) -> List[str]:
    """
    Retrieve a list of read_runs from the ENA Portal API, for a given study.
    Only read_runs with the matching library strategy metadata will be fetched.

    :param accession: Study accession on ENA
    :param limit: Maximum number of read_runs to fetch
    :param filter_library_strategy: E.g. AMPLICON, to only fetch library-strategy: amplicon reads
    :param extra_cache_hash: A string/hash that, when changed, will cause the cache to invalidate and so the task will run again.
    :return: A list of run accessions that have been fetched and matched the specified library strategy. Study may also contain other non-matching runs.
    """
    logger = get_run_logger()
    if extra_cache_hash:
        logger.info(f"Cache has additional hash of {extra_cache_hash}")

    # api call arguments
    query = f'"(study_accession={accession} OR secondary_study_accession={accession})"'
    if filter_library_strategy:
        query = f'{query[:-1]} AND library_strategy={filter_library_strategy}"'
    query = query.replace('"', "%22")

    fields = ",".join(EMG_CONFIG.ena.readrun_metadata_fields)
    result_type = "read_run"

    logger.info(f"Will fetch study {accession} read-runs from ENA portal API")

    mgys_study = analyses.models.Study.objects.get(ena_study__accession=accession)

    portal = httpx.get(
        ENAAPIRequest.create_ena_api_request(
            "read_run", query=query, limit=limit, fields=fields
        )
        + "&dataPortal=metagenome"
    )
    if not portal.status_code == httpx.codes.OK:
        raise Exception(f"Bad status! {portal.status_code} {portal}")

    logger.info("ENA portal responded ok.")

    run_accessions = []
    for read_run in portal.json():
        # check scientific name and metagenome source
        if (
            METAGENOME_SCIENTIFIC_NAME not in read_run["scientific_name"]
            and read_run["library_source"] not in ALLOWED_LIBRARY_SOURCE
        ):
            logger.warning(
                f"Run {read_run['run_accession']} is not in metagenome taxa and not in allowed library_sources. "
                f"No further processing for that run."
            )
            continue

        # check fastq files order/presence
        fastq_ftp_reads = check_reads_fastq(
            fastq=read_run["fastq_ftp"].split(";"),
            run_accession=read_run["run_accession"],
            library_layout=read_run["library_layout"],
        )
        if not fastq_ftp_reads:
            logger.warning(
                "Incorrect structure of fastq files provided. No further processing for that run."
            )
            continue

        logger.info(f"Creating objects for {read_run['run_accession']}")
        ena_sample, _ = ena.models.Sample.objects.get_or_create(
            accession=read_run["sample_accession"],
            defaults={
                "metadata": {"sample_title": read_run["sample_title"]},
                "study": mgys_study.ena_study,
            },
        )

        mgnify_sample, _ = analyses.models.Sample.objects.update_or_create(
            ena_sample=ena_sample,
            defaults={
                "ena_accessions": [
                    read_run["sample_accession"],
                    read_run["secondary_sample_accession"],
                ],
                "ena_study": mgys_study.ena_study,
            },
        )

        run, _ = analyses.models.Run.objects.update_or_create(
            ena_accessions=[read_run["run_accession"]],
            study=mgys_study,
            ena_study=mgys_study.ena_study,
            sample=mgnify_sample,
            defaults={
                analyses.models.Run.metadata.field.name: {
                    analyses.models.Run.CommonMetadataKeys.LIBRARY_STRATEGY: read_run[
                        analyses.models.Run.CommonMetadataKeys.LIBRARY_STRATEGY
                    ],
                    analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT: read_run[
                        analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                    ],
                    analyses.models.Run.CommonMetadataKeys.LIBRARY_SOURCE: read_run[
                        analyses.models.Run.CommonMetadataKeys.LIBRARY_SOURCE
                    ],
                    analyses.models.Run.CommonMetadataKeys.SCIENTIFIC_NAME: read_run[
                        analyses.models.Run.CommonMetadataKeys.SCIENTIFIC_NAME
                    ],
                    analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS: fastq_ftp_reads,
                }
            },
        )
        run.set_experiment_type_by_ena_library_strategy(
            read_run[analyses.models.Run.CommonMetadataKeys.LIBRARY_STRATEGY]
        )
        run_accessions.append(run.first_accession)

    return run_accessions


@task(
    task_run_name="Determine if {accession} is public in ENA",
    retries=4,
    retry_delay_seconds=15,
)
def is_ena_study_public(accession: str):
    logger = get_run_logger()

    fields = "study_accession"
    result_type = "study"
    query = (
        f"study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}"
    )

    logger.info(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        ENAAPIRequest.create_ena_api_request(
            result_type=result_type, query=query, limit=1, fields=fields
        )
    )
    if portal.status_code == httpx.codes.OK:
        response_json = portal.json()
        return not not response_json
    else:
        raise Exception(
            f"Bad status talking to portal API: {portal.status_code} {portal}"
        )


@task
def is_ena_study_private(accession: str):
    logger = get_run_logger()
    fields = "study_accession"


@flow
def sync_privacy_state_of_ena_study_and_derived_objects(
    ena_study: Union[ena.models.Study, str]
):
    if isinstance(ena_study, str):
        ena_study = ena.models.Study.objects.get_ena_study(ena_study)

    # call portal api to check visibility
    public = is_ena_study_public(ena_study.accession)

    # call portal api logged in to check if it is private
    # if not public:

    # otherwise it must be suppressed. pause and wait for user confirmation? slack notification?    q
