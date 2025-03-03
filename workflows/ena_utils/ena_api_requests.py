from datetime import date
from enum import Enum
from json import JSONDecodeError
from typing import List, Literal, Optional, Type, TypeVar, Union, Dict, Any

import httpx
from django.conf import settings
from httpx import Auth
from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field, computed_field, field_serializer, model_validator
from typing_extensions import Self

import analyses.models
import ena.models
from emgapiv2.enum_utils import FutureStrEnum
from workflows.ena_utils.ena_auth import dcc_auth
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash

ALLOWED_LIBRARY_SOURCE: list = ["METAGENOMIC", "METATRANSCRIPTOMIC"]
SINGLE_END_LIBRARY_LAYOUT: str = "SINGLE"
PAIRED_END_LIBRARY_LAYOUT: str = "PAIRED"
METAGENOME_SCIENTIFIC_NAME: str = "metagenome"

EMG_CONFIG = settings.EMG_CONFIG

RETRIES = EMG_CONFIG.ena.portal_search_api_max_retries
RETRY_DELAY = EMG_CONFIG.ena.portal_search_api_retry_delay_seconds


class ENAPortalResultType(str, Enum):
    ANALYSIS = "analysis"  # Nucelotide sequence analyses from reads
    ANALYSIS_STUDY = (
        "analysis_study"  # Studies used for nucleotide sequence analyses from reads
    )
    ASSEMBLY = "assembly"  # Genome assemblies
    CODING = "coding"  # Coding sequences
    NONCODING = "noncoding"  # Non-coding sequences
    READ_EXPERIMENT = "read_experiment"  # Experiments used for raw reads
    READ_RUN = "read_run"  # Raw reads
    READ_STUDY = "read_study"  # Studies used for raw reads
    SAMPLE = "sample"
    STUDY = "study"
    TAXON = "taxon"  # Taxonomic classification
    TLS_SET = "tls_set"  # Targeted locus study contig sets (TLS)
    TSA_SET = "tsa_set"  # Transcriptome assembly contig sets (TSA)
    WGS_SET = "wgs_set"  # Genome assembly contig set (WGS)


class ENAQueryOperators(str, Enum):
    OR = "OR"
    AND = "AND"
    NOT = "NOT"


class ENAQueryClause(BaseModel):
    search_field: str
    value: Union[str, int, date]
    is_not: bool = Field(default=False)

    def __str__(self):
        value = self.value
        if isinstance(value, date):
            value = value.strftime("%Y-%m-%d")
        return f"{ENAQueryOperators.NOT if self.is_not else ''} {self.search_field}={value}".strip()

    def __or__(self, other: Union[Self, "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: Union[Self, "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> Self:
        return ENAQueryClause(
            search_field=self.search_field, value=self.value, is_not=not self.is_not
        )


ENAQuerySetType = TypeVar("ENAQuerySetType", bound="_ENAQueryConditions")


class ENAQueryPair(BaseModel):
    operator: ENAQueryOperators = Field(ENAQueryOperators.AND)
    left: Union[ENAQueryClause, Self, ENAQuerySetType]
    right: Union[ENAQueryClause, Self, ENAQuerySetType]
    is_not: bool = Field(default=False)

    def __str__(self):
        return f"{ENAQueryOperators.NOT + ' ' if self.is_not else ''}({str(self.left)} {self.operator.value} {str(self.right)})"

    def __or__(self, other: Union[ENAQueryClause, Self, ENAQuerySetType]) -> Self:
        return self.__class__(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: Union[ENAQueryClause, Self, ENAQuerySetType]) -> Self:
        return self.__class__(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> Self:
        return self.__class__(
            left=self.left,
            operator=self.operator,
            right=self.right,
            is_not=not self.is_not,
        )


class _ENAQueryConditions(BaseModel):
    is_not: bool = Field(default=False)

    # Define specific fields on inheriting models e.g.:
    # class ENAStudyQuery(ENAQueryConditions):
    #     description: Optional[str] = Field(None, description="brief sequence description")

    @computed_field
    @property
    def queries(self) -> Union[ENAQueryPair, ENAQueryClause]:
        # combine all set fields with an AND.
        # technically this makes nested pairs, which is not optimal, but is reasonable for most use cases.
        # e.g. ((study_description=hello AND tax_id=1) AND broker_name=EMG)
        # could be simplified to (study_description=hello AND tax_id=1 AND broker_name=EMG)
        clauses = None
        for search_field, value in self.model_dump(
            exclude={"is_not", "queries"}
        ).items():
            if value is None:
                continue
            if clauses:
                clauses &= ENAQueryClause(search_field=search_field, value=value)
            else:
                clauses = ENAQueryClause(search_field=search_field, value=value)

        return clauses

    def __str__(self):
        return str(self.queries)

    def __or__(self, other: ENAQuerySetType) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: ENAQuerySetType) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> Self:
        already_set = self.model_dump(exclude={"is_not"})
        return self.__class__(**already_set, is_not=not self.is_not)


class ENAStudyQuery(_ENAQueryConditions):
    # From: https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result=study 2025/01/23
    # Some are controlled values not yet controlled here (e.g. datahub)
    breed: Optional[str] = Field(None, description="Breed")
    broker_name: Optional[str] = Field(None, description="broker name")
    center_name: Optional[str] = Field(None, description="Submitting center")
    cultivar: Optional[str] = Field(
        None,
        description="cultivar (cultivated variety) of plant from which sample was obtained",
    )
    datahub: Optional[str] = Field(None, description="DCC datahub name")
    description: Optional[str] = Field(None, description="brief sequence description")
    first_public: Optional[date] = Field(None, description="date when made public")
    geo_accession: Optional[str] = Field(None, description="GEO accession")
    isolate: Optional[str] = Field(
        None, description="individual isolate from which sample was obtained"
    )
    keywords: Optional[str] = Field(
        None, description="keywords associated with sequence"
    )
    last_updated: Optional[date] = Field(None, description="date when last updated")
    parent_study_accession: Optional[str] = Field(
        None, description="parent study accession"
    )
    project_name: Optional[str] = Field(
        None,
        description="name of the project within which the sequencing was organized",
    )
    scientific_name: Optional[str] = Field(
        None, description="scientific name of an organism"
    )
    secondary_study_accession: Optional[str] = Field(
        None, description="secondary study accession number"
    )
    secondary_study_alias: Optional[str] = Field(None, description="Submitting center")
    secondary_study_center_name: Optional[str] = Field(
        None, description="Submitting center"
    )
    status: Optional[int] = Field(None, description="Status")
    strain: Optional[str] = Field(
        None, description="strain from which sample was obtained"
    )
    study_accession: Optional[str] = Field(None, description="study accession number")
    study_alias: Optional[str] = Field(
        None, description="Submitter's name for the study"
    )
    study_description: Optional[str] = Field(
        None, description="detailed sequencing study description"
    )
    study_name: Optional[str] = Field(None, description="sequencing study name")
    study_title: Optional[str] = Field(
        None, description="brief sequencing study description"
    )
    submission_tool: Optional[str] = Field(None, description="Submission tool")
    tag: Optional[str] = Field(None, description="Classification Tags")
    tax_division: Optional[str] = Field(None, description="taxonomic division")
    tax_id: Optional[str] = Field(None, description="NCBI taxonomic classification")


class ENAStudyFields(FutureStrEnum):
    # from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=study 2025-01-23
    BREED = "breed"  # breed
    BROKER_NAME = "broker_name"  # broker name
    CENTER_NAME = "center_name"  # Submitting center
    CULTIVAR = "cultivar"  # cultivar (cultivated variety) of plant from which sample was obtained
    DATAHUB = "datahub"  # DCC datahub name
    DESCRIPTION = "description"  # brief sequence description
    FIRST_PUBLIC = "first_public"  # date when made public
    GEO_ACCESSION = "geo_accession"  # GEO accession
    ISOLATE = "isolate"  # individual isolate from which sample was obtained
    KEYWORDS = "keywords"  # keywords associated with sequence
    LAST_UPDATED = "last_updated"  # date when last updated
    PARENT_STUDY_ACCESSION = "parent_study_accession"  # parent study accession
    PROJECT_NAME = (
        "project_name"  # name of the project within which the sequencing was organized
    )
    SCIENTIFIC_NAME = "scientific_name"  # scientific name of an organism
    SECONDARY_STUDY_ACCESSION = (
        "secondary_study_accession"  # secondary study accession number
    )
    SECONDARY_STUDY_ALIAS = "secondary_study_alias"  # Submitting center
    SECONDARY_STUDY_CENTER_NAME = "secondary_study_center_name"  # Submitting center
    STATUS = "status"  # Status
    STRAIN = "strain"  # strain from which sample was obtained
    STUDY_ACCESSION = "study_accession"  # study accession number
    STUDY_ALIAS = "study_alias"  # submitter's name for the study
    STUDY_DESCRIPTION = "study_description"  # detailed sequencing study description
    STUDY_NAME = "study_name"  # sequencing study name
    STUDY_TITLE = "study_title"  # brief sequencing study description
    SUBMISSION_TOOL = "submission_tool"  # Submission tool
    TAG = "tag"  # Classification Tags
    TAX_DIVISION = "tax_division"  # taxonomic division
    TAX_ID = "tax_id"  # NCBI taxonomic classification
    TAX_LINEAGE = "tax_lineage"  # Complete taxonomic lineage for an organism


class ENAAPIRequest(BaseModel):
    result: ENAPortalResultType
    query: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair]
    fields: Union[List[ENAStudyFields]]
    limit: Optional[int] = Field(None, description="Max number of results to return")
    format: Literal["tsv", "json"] = Field("json")

    @model_validator(mode="after")
    def result_and_query_and_return_fields_align(self):
        if self.result == ENAPortalResultType.STUDY:
            assert isinstance(self.fields, List)
            assert isinstance(self.fields[0], ENAStudyFields)
            self._assert_query_conditions_are_of_type(self.query, self.result)
        return self

    def _assert_query_conditions_are_of_type(
        self,
        query_part: Union[ENAQuerySetType, ENAQueryClause, ENAQueryPair],
        result_type: ENAPortalResultType,
    ):
        if type(query_part) is ENAQuerySetType:
            if result_type == ENAPortalResultType.STUDY:
                assert isinstance(query_part, ENAStudyQuery)
            elif isinstance(query_part, ENAQueryPair):
                self._assert_query_conditions_are_of_type(query_part.left, result_type)
                self._assert_query_conditions_are_of_type(query_part.right, result_type)

    @field_serializer("query")
    def serialize_query(self, query: Type[_ENAQueryConditions], _info):
        return f'"{query}"'

    @field_serializer("fields")
    def serialize_fields(self, fields: Union[List[ENAStudyFields]], _info):
        return ",".join(fields)

    @field_serializer("result")
    def serialize_result_type(self, result: ENAPortalResultType):
        return result.value

    def _parse_response(self, response: httpx.Response):
        if self.format == "json":
            try:
                j = response.json()
            except JSONDecodeError:
                raise ENAAccessException("Bad JSON response.")
            if isinstance(j, dict) and "message" in j:
                raise ENAAccessException(f"Error response: {j['message']}")
            elif isinstance(j, list) and len(j) == 0:
                raise ENAAvailabilityException("Empty response.")
            return j
        return response.text  # TODO: tsv

    def get(self, auth: Type[Auth] = None) -> Union[List[Dict[str, Any]], str]:
        url = EMG_CONFIG.ena.portal_search_api
        params = self.model_dump()
        r = httpx.get(
            url=url,
            params=params,
            auth=auth,
        )
        if httpx.codes.is_error(r.status_code):
            raise ENAAccessException(r.text)
        return self._parse_response(r)


def create_ena_api_request(result_type, query, limit, fields, result_format="json"):
    return (
        f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result={result_type}&"
        f"query={query}&"
        f"limit={limit}&"
        f"format={result_format}&"
        f"fields={fields}"
    )


class ENAAccessException(Exception): ...


class ENAAvailabilityException(Exception): ...


@task(
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    cache_key_fn=context_agnostic_task_input_hash,
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
    secondary_accession: str = s["secondary_study_accession"]
    additional_accessions: list = []
    if not secondary_accession:
        logger.warning(f"Study {accession} secondary_accession is not available")
    else:
        if len(secondary_accession.split(";")) > 1:
            logger.warning(f"Study {accession} has more than one secondary_accession")
            additional_accessions = secondary_accession.split(";")
        else:
            additional_accessions = [secondary_accession]

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
        if "_2.f" in sorted_fastq[0]:
            # we accept _1 be in SE fastq path
            logger.warning(f"Single fastq file contains _2 for run {run_accession}")
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
    retries=RETRIES,
    retry_delay_seconds=RETRY_DELAY,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study readruns from ENA: {accession}",
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
    # TODO: rewrite using ena-api-handler once available

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

    if mgys_study.is_private:
        auth = dcc_auth
        logger.info("Using dcc authentication as study is private")
    else:
        auth = None

    portal = httpx.get(
        create_ena_api_request(
            result_type=result_type, query=query, limit=limit, fields=fields
        )
        + "&dataPortal=metagenome",
        auth=auth,
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
                "is_private": mgys_study.is_private,
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
                    analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID: read_run[
                        analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID
                    ],
                    analyses.models.Run.CommonMetadataKeys.HOST_SCIENTIFIC_NAME: read_run[
                        analyses.models.Run.CommonMetadataKeys.HOST_SCIENTIFIC_NAME
                    ],
                    analyses.models.Run.CommonMetadataKeys.INSTRUMENT_MODEL: read_run[
                        analyses.models.Run.CommonMetadataKeys.INSTRUMENT_MODEL
                    ],
                    analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM: read_run[
                        analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM
                    ],
                },
                "is_private": mgys_study.is_private,
            },
        )
        run.set_experiment_type_by_metadata(
            read_run[analyses.models.Run.CommonMetadataKeys.LIBRARY_STRATEGY],
            read_run[analyses.models.Run.CommonMetadataKeys.LIBRARY_SOURCE],
        )
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
