import operator
from datetime import timedelta
from functools import reduce
from typing import List, Optional, Type, Union, Literal

from django.conf import settings
from httpx import Auth
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

import analyses.models
import ena.models
from emgapiv2.dict_utils import some
from emgapiv2.enum_utils import FutureStrEnum
from workflows.ena_utils.abstract import ENAPortalResultType
from workflows.ena_utils.analysis import ENAAnalysisFields, ENAAnalysisQuery
from workflows.ena_utils.ena_accession_matching import (
    extract_all_accessions,
    extract_study_accession_from_study_title,
)
from workflows.ena_utils.ena_auth import dcc_auth
from workflows.ena_utils.read_run import ENAReadRunFields, ENAReadRunQuery
from workflows.ena_utils.requestors import ENAAPIRequest, ENAAvailabilityException
from workflows.ena_utils.study import ENAStudyQuery, ENAStudyFields

ALLOWED_LIBRARY_SOURCE: list = ["METAGENOMIC", "METATRANSCRIPTOMIC"]
SINGLE_END_LIBRARY_LAYOUT: str = "SINGLE"
PAIRED_END_LIBRARY_LAYOUT: str = "PAIRED"
METAGENOME_SCIENTIFIC_NAME: str = "metagenome"

EMG_CONFIG = settings.EMG_CONFIG

RETRIES = EMG_CONFIG.ena.portal_search_api_max_retries
RETRY_DELAY = EMG_CONFIG.ena.portal_search_api_retry_delay_seconds


class ENALibraryStrategyPolicy(FutureStrEnum):
    """
    Each policy determines a trust vs. override level for the library strategy metadata in ENA.
    """

    ONLY_IF_CORRECT_IN_ENA = "only_if_correct_in_ena"
    ASSUME_OTHER_ALSO_MATCHES = "assume_other_also_matches"
    OVERRIDE_ALL = "override_all"


def library_strategy_policy_to_filter(
    primary_library_strategy: str,
    other_library_strategies: list[str] = None,
    policy: ENALibraryStrategyPolicy = ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
) -> list[str]:
    if other_library_strategies is None:
        other_library_strategies = []

    if policy == ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA:
        return [primary_library_strategy] + other_library_strategies
    elif policy == ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES:
        return [primary_library_strategy] + other_library_strategies + ["OTHER"]
    return []


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    cache_key_fn=task_input_hash,
    task_run_name="Get study from ENA: {accession}",
)
def get_study_from_ena(accession: str, limit: int = 10) -> ena.models.Study:
    logger = get_run_logger()

    logger.info(f"Will fetch from ENA Portal API Study {accession}")

    is_public = is_ena_study_public(accession)
    is_private = not is_public and is_ena_study_available_privately(accession)

    if not (is_private or is_public):
        raise ENAAvailabilityException(
            f"Study {accession} is not available publicly or privately on ENA"
        )

    # TODO: verify webin ownership

    ena_auth = dcc_auth if is_private else None

    if ena_auth:
        logger.info("Fetching study with authentication.")

    portal = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        fields=[
            ENAStudyFields[f.upper()] for f in EMG_CONFIG.ena.study_metadata_fields
        ],
        limit=limit,
        query=ENAStudyQuery(study_accession=accession)
        | ENAStudyQuery(secondary_study_accession=accession),
    ).get(auth=ena_auth)

    s = portal[0]

    # Check secondary accession
    additional_accessions = extract_all_accessions(s["secondary_study_accession"])
    if len(additional_accessions) > 1:
        logger.warning(f"Study {accession} has more than one secondary_accession")
    if not additional_accessions:
        logger.warning(f"Study {accession} secondary_accession is not available")

    # Check primary accession
    if not s[ENAStudyFields.STUDY_ACCESSION]:
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
        primary_accession: str = s[ENAStudyFields.STUDY_ACCESSION]

    study, created = ena.models.Study.objects.get_or_create(
        accession=primary_accession,
        defaults={
            "title": s[ENAStudyFields.STUDY_TITLE],
            "additional_accessions": additional_accessions,
            "is_private": is_private,
            # TODO: more metadata
        },
    )
    return study


def check_reads_fastq(
    fastq: list[str], run_accession: str, library_layout: str
) -> tuple[List[str] | None, Literal["SINGLE", "PAIRED", None]]:
    """
    Analyses and validates a list of FASTQ files for a given run accession and
    specified library layout. Ensures the FASTQ files align with the expected
    library layout, such as single-end or paired-end sequencing data.

    :param fastq: A list of FASTQ file paths associated with the sequencing run.
    :param run_accession: The unique identifier for the sequencing run.
    :param library_layout: The specified library layout for the data. This should
        indicate whether the data is single-end ("SINGLE") or paired-end ("PAIRED").
    :return: A tuple containing the sorted FASTQ file paths and the inferred library
        layout as "SINGLE" or "PAIRED", or None if validation fails or no valid FASTQ
        files are provided.
    """
    logger = get_run_logger()  # TODO: make this method okay to use outside of prefect
    sorted_fastq = sorted(fastq)  # to keep order [_1, _2, _3(?)]
    # potential single end
    if len(sorted_fastq) == 1:
        if not sorted_fastq[0]:
            # if it's an empty string
            logger.warning(f"No fastq files for run {run_accession}")
            return None, None
        if "_2.f" in sorted_fastq[0]:
            # we accept _1 be in SE fastq path
            logger.warning(f"Single fastq file contains _2 for run {run_accession}")
            return None, None
        if library_layout == PAIRED_END_LIBRARY_LAYOUT:
            logger.warning(
                f"Incorrect library_layout for {run_accession} having one fastq file"
            )
            return sorted_fastq, "SINGLE"
        else:
            logger.info(f"One fastq for {run_accession}: {sorted_fastq}")
            return sorted_fastq, "SINGLE"
    # potential paired end
    elif len(sorted_fastq) >= 2:
        match_1 = next((f for f in sorted_fastq if "_1" in f), None)
        match_2 = next((f for f in sorted_fastq if "_2" in f), None)
        if match_1 and match_2:
            logger.info(f"Two fastqs for {run_accession}: {sorted_fastq}")
            return [match_1, match_2], "PAIRED"
        else:
            if len(sorted_fastq) > 2:
                logger.warning(
                    f"More than 2 fastq files provided for run {run_accession}"
                )
            else:
                logger.warning(
                    f"Incorrect library_layout for {run_accession} having two fastq files"
                )
            logger.warning(
                f"Incorrect names of fastq files for run {run_accession} (${sorted_fastq})"
            )
            return None, None
    return None, None


def _make_samples_and_run(
    run_or_assembly_response: dict, study: analyses.models.Study
) -> (ena.models.Sample, analyses.models.Sample, analyses.models.Run):
    _ = ENAReadRunFields  # fields used here are also present on ENAAnalysisFields

    ena_sample, __ = ena.models.Sample.objects.update_or_create(
        accession__in=[
            run_or_assembly_response[_.SAMPLE_ACCESSION],
            run_or_assembly_response[_.SECONDARY_SAMPLE_ACCESSION],
        ],
        defaults={
            "metadata": some(run_or_assembly_response, {_.SAMPLE_TITLE, _.LAT, _.LON}),
        },
        create_defaults={
            "accession": run_or_assembly_response[_.SAMPLE_ACCESSION],
            "additional_accessions": [
                run_or_assembly_response[_.SECONDARY_SAMPLE_ACCESSION]
            ],
            "study": study.ena_study,  # TODO could be more than one...
        },
    )

    mgnify_sample, __ = analyses.models.Sample.objects.update_or_create_by_accession(
        known_accessions=[
            run_or_assembly_response[_.SAMPLE_ACCESSION],
            run_or_assembly_response[_.SECONDARY_SAMPLE_ACCESSION],
        ],
        defaults={
            "is_private": study.is_private,
            "metadata": some(run_or_assembly_response, {_.LAT, _.LON}),
        },
        create_defaults={
            "ena_sample": ena_sample,
            "ena_study": study.ena_study,
        },
    )
    mgnify_sample.studies.add(study)

    run, __ = analyses.models.Run.objects.update_or_create_by_accession(
        known_accessions=[run_or_assembly_response[_.RUN_ACCESSION]],
        defaults={
            "metadata": some(
                run_or_assembly_response,
                {
                    _.LIBRARY_STRATEGY,
                    _.LIBRARY_LAYOUT,
                    _.LIBRARY_SOURCE,
                    _.SCIENTIFIC_NAME,
                    _.HOST_TAX_ID,
                    _.HOST_SCIENTIFIC_NAME,
                    _.INSTRUMENT_PLATFORM,
                    _.INSTRUMENT_MODEL,
                },
            ),
            "is_private": study.is_private,
        },
        create_defaults={
            "study": study,
            "ena_study": study.ena_study,
            "sample": mgnify_sample,
        },
    )
    run.set_experiment_type_by_metadata(
        run_or_assembly_response[_.LIBRARY_STRATEGY],
        run_or_assembly_response[_.LIBRARY_SOURCE],
    )
    return ena_sample, mgnify_sample, run


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    cache_expiration=timedelta(days=1),
    cache_key_fn=task_input_hash,
    task_run_name="Get study readruns from ENA: {accession}",
)
def get_study_readruns_from_ena(
    accession: str,
    limit: int = 20,
    filter_library_strategy: list[str] = None,
    raise_on_empty: bool = True,
) -> List[str]:
    """
    Retrieve a list of read_runs from the ENA Portal API, for a given study.
    Only read_runs with the matching library strategy metadata will be fetched.

    :param accession: Study accession on ENA
    :param limit: Maximum number of read_runs to fetch
    :param filter_library_strategy: E.g. ["AMPLICON"], to only fetch library-strategy: amplicon reads
    :param raise_on_empty: Raise an exception if no read_runs are found for the given study (default True, as some ENA failure modes may result in no read_runs being returned)
    :return: A list of run accessions that have been fetched and matched the specified library strategy. Study may also contain other non-matching runs.
    """

    logger = get_run_logger()
    logger.info(f"Will fetch Read Runs from ENA Portal API for Study {accession}")

    mgys_study = analyses.models.Study.objects.get(
        ena_study__accession__contains=accession
    )

    ena_auth = dcc_auth if mgys_study.is_private else None

    query = ENAReadRunQuery(study_accession=accession) | ENAReadRunQuery(
        secondary_study_accession=accession
    )
    if filter_library_strategy:
        strategy_queries = reduce(
            operator.or_,
            [ENAReadRunQuery(library_strategy=ls) for ls in filter_library_strategy],
        )
        query &= strategy_queries

    _ = ENAReadRunFields

    logger.info(f"Will fetch study {accession} read-runs from ENA portal API")

    portal_read_runs = ENAAPIRequest(
        result=ENAPortalResultType.READ_RUN,
        fields=[
            _.RUN_ACCESSION,
            _.SAMPLE_ACCESSION,
            _.SAMPLE_TITLE,
            _.SECONDARY_SAMPLE_ACCESSION,
            _.FASTQ_MD5,
            _.FASTQ_FTP,
            _.LIBRARY_LAYOUT,
            _.LIBRARY_STRATEGY,
            _.LIBRARY_SOURCE,
            _.SCIENTIFIC_NAME,
            _.HOST_TAX_ID,
            _.HOST_SCIENTIFIC_NAME,
            _.INSTRUMENT_PLATFORM,
            _.INSTRUMENT_MODEL,
            _.LOCATION,
            _.LAT,
            _.LON,
        ],
        limit=limit,
        query=query,
    ).get(auth=ena_auth, raise_on_empty=raise_on_empty)

    run_accessions = []
    for read_run in portal_read_runs:
        # check scientific name and metagenome source
        if (
            METAGENOME_SCIENTIFIC_NAME not in read_run[_.SCIENTIFIC_NAME]
            and read_run[_.LIBRARY_SOURCE] not in ALLOWED_LIBRARY_SOURCE
        ):
            logger.warning(
                f"Run {read_run['run_accession']} is not in metagenome taxa and not in allowed library_sources. "
                f"No further processing for that run."
            )
            continue

        # check fastq files order/presence
        fastq_ftp_reads, inferred_library_layout = check_reads_fastq(
            fastq=read_run[_.FASTQ_FTP].split(";"),
            run_accession=read_run[_.RUN_ACCESSION],
            library_layout=read_run[_.LIBRARY_LAYOUT],
        )
        if not fastq_ftp_reads:
            logger.warning(
                "Incorrect structure of fastq files provided. No further processing for that run."
            )
            continue

        logger.info(f"Creating objects for {read_run[_.RUN_ACCESSION]}")

        __, __, run = _make_samples_and_run(read_run, mgys_study)
        run.metadata[analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS] = (
            fastq_ftp_reads
        )
        if not inferred_library_layout == read_run[_.LIBRARY_LAYOUT]:
            logger.warning(
                f"Using inferred library layout of {inferred_library_layout} for {read_run[_.RUN_ACCESSION]} instead of metadata-provided {read_run[_.LIBRARY_LAYOUT]}."
            )
            run.metadata[
                analyses.models.Run.CommonMetadataKeys.INFERRED_LIBRARY_LAYOUT
            ] = inferred_library_layout
        run.save()

        run_accessions.append(run.first_accession)

    return run_accessions


def is_study_available(accession: str, auth: Optional[Type[Auth]] = None) -> bool:
    logger = get_run_logger()
    logger.info(f"Checking ENA Portal for {accession}")
    if auth is None:
        logger.info("Checking publicly, without auth")
    else:
        logger.info("Checking privately, with auth")

    try:
        portal = ENAAPIRequest(
            result=ENAPortalResultType.STUDY,
            query=(
                ENAStudyQuery(study_accession=accession)
                | ENAStudyQuery(secondary_study_accession=accession)
            ),
            format="json",
            fields=[ENAStudyFields.STUDY_ACCESSION],
        ).get(auth=auth)
    except ENAAvailabilityException as e:
        logger.info(f"Looks like an error-free empty response from ENA: {e}")
        return False
    return len(portal) > 0


@task(
    task_run_name="Determine if {accession} is public in ENA",
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
)
def is_ena_study_public(accession: str):
    logger = get_run_logger()
    is_public = is_study_available(accession=accession)
    logger.info(f"Is {accession} public? {is_public}")
    return is_public


@task(
    task_run_name="Determine if {accession} is public in ENA",
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
)
def is_ena_study_available_privately(accession: str):
    logger = get_run_logger()
    is_available_privately = is_study_available(accession=accession, auth=dcc_auth)
    logger.info(
        f"Is {accession} available privately to {EMG_CONFIG.webin.dcc_account}? {is_available_privately}"
    )
    return is_available_privately


@flow
def sync_privacy_state_of_ena_study_and_derived_objects(
    ena_study: Union[ena.models.Study, str],
):
    logger = get_run_logger()

    if isinstance(ena_study, str):
        ena_study = ena.models.Study.objects.get_ena_study(ena_study)

    # call portal api to check visibility
    public = is_ena_study_public(ena_study.accession)
    if public:
        logger.info(f"Study {ena_study} is available publicly in ENA Portal")
    private = None

    # call portal api logged in to check if it is private
    if not public:
        # Use authentication, where the Webin account must be a member of the ENA Data Hub (dcc).
        private = is_ena_study_available_privately(ena_study.accession)
        if private:
            logger.info(f"Study {ena_study} is available privately in ENA Portal")

    suppressed = not (public or private)
    if suppressed:
        logger.warning(
            f"ENA Study {ena_study} is not available via portal API, either publicly or privately. Assuming it has been suppressed."
        )
        ena_study.is_suppressed = True
    else:
        ena_study.is_private = private
    ena_study.save()


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    cache_expiration=timedelta(days=1),
    cache_key_fn=task_input_hash,
    task_run_name="Get study assemblies from ENA: {accession}",
)
def get_study_assemblies_from_ena(accession: str, limit: int = 10) -> list[str]:
    """
    Fetches a list of assemblies from the European Nucleotide Archive (ENA) for a given study accession.

    This function queries the ENA Portal API to retrieve assemblies associated with the given study
    accession. The query returns a limited number of results, which can be configured via the `limit`
    parameter. Authenticated requests may be sent to fetch private data if the `dcc_auth` is configured
    correctly.

    :param accession: The ENA accession identifier of the study for which to fetch assemblies.
    An analysis.study must already exist for this accession.
    :type accession: str
    :param limit: The maximum number of assemblies to retrieve. Default is 10.
    :type limit: int
    :return: A list of assembly accession strings fetched from ENA.
    :rtype: List[str]
    """
    logger = get_run_logger()
    logger.info(f"Will fetch Assemblies from ENA Portal API for Study {accession}")

    study = analyses.models.Study.objects.get(ena_study__accession__contains=accession)

    _ = ENAAnalysisFields

    ena_auth = dcc_auth if study.is_private else None

    # fetch all assemblies in the "assembly study"
    portal_assemblies = ENAAPIRequest(
        result=ENAPortalResultType.ANALYSIS,
        fields=[
            _.SAMPLE_ACCESSION,
            _.SAMPLE_TITLE,
            _.SECONDARY_SAMPLE_ACCESSION,
            _.RUN_ACCESSION,
            _.ANALYSIS_ACCESSION,
            _.COMPLETENESS_SCORE,
            _.CONTAMINATION_SCORE,
            _.SCIENTIFIC_NAME,
            _.LOCATION,
            _.LAT,
            _.LON,
            _.GENERATED_FTP,
        ],
        limit=limit,
        query=ENAAnalysisQuery(study_accession=accession)
        | ENAAnalysisQuery(secondary_study_accession=accession),
    ).get(auth=ena_auth)

    # read-runs may exist in same study as the assemblies
    portal_runs = get_study_readruns_from_ena(
        accession=accession, limit=limit, raise_on_empty=False
    )

    if study.assemblies_assembly.exists():
        # Looks like a study we assembled / already know the connection to a reads study for these assemblies
        # (assumption here: reads study is singular for all assemblies)
        reads_study = study.assemblies_assembly.first().reads_study
    elif (
        reads_study_accession := extract_study_accession_from_study_title(study.title)
        and not portal_runs
    ):
        # Looks like a TPA study – no read-runs within it, and an accession in the study title
        # e.g. "This is a TPA Study of PRJ123"
        ena_reads_study = ena.models.Study.objects.get_ena_study(reads_study_accession)
        if not ena_reads_study:
            ena_reads_study = get_study_from_ena(reads_study_accession)
            ena_reads_study.refresh_from_db()
        reads_study: analyses.models.Study = (
            analyses.models.Study.objects.get_or_create_for_ena_study(
                reads_study_accession
            )
        )
    else:
        # No easily determined reads-study, so the assemblies may be missing links to samples/reads etc.
        # This is the case for assembly-only studies where raw reads were not uploaded to ENA.
        logger.warning(
            f"No reads study could be found for the assemblies of {accession}"
        )
        reads_study = None

    if reads_study:
        # Reads-study is different from assembly-study, so fetch runs+samples from it
        get_study_readruns_from_ena(
            accession=reads_study.first_accession,
            limit=limit,
        )

    # Build assembly objects
    assemblies = []
    for assembly_data in portal_assemblies:
        try:
            # sample may have been made previously, e.g. prior to assembly or by read-run fetchers above
            mgnify_sample = analyses.models.Sample.objects.get_by_accession(
                assembly_data[_.SAMPLE_ACCESSION]
            )
        except analyses.models.Sample.DoesNotExist:
            # make sample based on metadata available from ENA assembly
            __, mgnify_sample, run = _make_samples_and_run(assembly_data, study)
        else:
            run = mgnify_sample.runs.first()

        assembly, __ = analyses.models.Assembly.objects.update_or_create_by_accession(
            known_accessions=[assembly_data[_.ANALYSIS_ACCESSION]],
            defaults={
                "sample": mgnify_sample,
                "is_private": study.is_private,
                "run": run,
            },
            create_defaults={
                "assembly_study": study,
                "reads_study": reads_study,
                "ena_study": study.ena_study,
            },
            include_update_defaults_in_create_defaults=True,
        )
        assembly.metadata[_.GENERATED_FTP] = assembly_data[_.GENERATED_FTP]
        assembly.save()
        assemblies.append(assembly)
    return assemblies
