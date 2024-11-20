import re
from typing import List, Pattern

from pydantic import AnyHttpUrl, BaseModel, Field
from pydantic.networks import MongoDsn, MySQLDsn
from pydantic_settings import BaseSettings


class SlurmConfig(BaseModel):
    default_job_status_checks_limit: int = 10
    default_workdir: str = "/nfs/production/dev-slurm-work-dir"
    pipelines_root_dir: str = "/app/workflows/pipelines"
    user: str = "root"

    incomplete_job_limit: int = 100
    # if this many jobs are RUNNING or PENDING, no more are submitted

    default_seconds_between_job_checks: int = 10
    # when a job is running, we wait this long between status checks

    default_seconds_between_submission_attempts: int = 10
    default_submission_attempts_limit: int = 100
    # if the cluster is "full", we wait this long before checking again for space,
    #   and only attempt submission a limited number of times before giving up.

    wait_seconds_between_slurm_flow_resumptions: int = 2

    job_log_tail_lines: int = 10
    # how many lines of slurm log to send to prefect each time we check it

    use_nextflow_tower: bool = False
    nextflow_tower_org: str = "EMBL-EBI"
    nextflow_tower_workspace: str = "ebi-spws-dev-microbiome-info"

    datamover_paritition: str = "datamover"

    assembly_uploader_python_executable: str = "python3"
    assembly_uploader_root_dir: str = ""
    webin_cli_executor: str = "/usr/bin/webin-cli/webin-cli.jar"

    amplicon_nextflow_master_job_memory: int = 1  # Gb

    shared_filesystem_root_on_slurm: str = "/nfs/public"
    shared_filesystem_root_on_server: str = "/app/data"

    samplesheet_editing_allowed_inside: str = default_workdir
    samplesheet_editing_path_from_shared_filesystem: str = "temporary_samplesheet_edits"
    # allow django-admin access to edit csv/tsv files inside this dir


class AssemblerConfig(BaseModel):
    assembly_pipeline_repo: str = "ebi-metagenomics/miassembler"
    assembler_default: str = "metaspades"
    assembler_version_default: str = "3.15.5"
    miassemebler_git_revision: str = (
        "main"  # branch or commit of ebi-metagenomics/miassembler
    )
    miassembler_nf_profile: str = "codon_slurm"


class AmpliconPipelineConfig(BaseModel):
    amplicon_pipeline_repo: str = "ebi-metagenomics/amplicon-pipeline"
    amplicon_pipeline_git_revision: str = (
        "main"  # branch or commit of ebi-metagenomics/amplicon-pipeline
    )
    amplicon_pipeline_nf_profile: str = "codon_slurm"
    samplesheet_chunk_size: int = 20
    amplicon_library_strategy: str = "AMPLICON"
    # results stats
    completed_runs_csv: str = "qc_passed_runs.csv"
    failed_runs_csv: str = "qc_failed_runs.csv"
    # results folders
    qc_folder: str = "qc"
    sequence_categorisation_folder: str = "sequence-categorisation"
    amplified_region_inference_folder: str = "amplified-region-inference"
    asv_folder: str = "asv"
    primer_identification_folder: str = "primer-identification"
    taxonomy_summary_folder: str = "taxonomy-summary"


class WebinConfig(BaseModel):
    emg_webin_account: str = None
    emg_webin_password: str = None
    submitting_center_name: str = "EMG"
    webin_cli_executor: str = "/usr/bin/webin-cli/webin-cli.jar"


class ENAConfig(BaseModel):
    primary_study_accession_re: str = "(PRJ[EDN][A-Z][0-9]+)"
    assembly_accession_re: str = "([EDS]RZ[0-9]{6,})"
    portal_search_api: AnyHttpUrl = "https://www.ebi.ac.uk/ena/portal/api/search"
    # TODO: migrate to the ENA Handler
    study_metadata_fields: list = [
        "study_title",
        "secondary_study_accession"
    ]
    # TODO: migrate to the ENA Handler
    readrun_metadata_fields: list = [
        "sample_accession",
        "sample_title",
        "secondary_sample_accession",
        "fastq_md5",
        "fastq_ftp",
        "library_layout",
        "library_strategy",
        "library_source",
        "scientific_name"
    ]

    ftp_prefix: str = "ftp.sra.ebi.ac.uk/vol1/"
    fire_prefix: str = "s3://era-public/"


class LegacyServiceConfig(BaseModel):
    emg_mongo_dsn: MongoDsn = "mongodb://mongo.not.here/db"
    emg_mongo_db: str = "emgapi"

    emg_mysql_dsn: MySQLDsn = "mysql+mysqlconnector://mysql.not.here/emg"

    emg_analysis_download_url_pattern: str = (
        "https://www.ebi.ac.uk/metagenomics/api/v1/analyses/{id}/file/{alias}"
    )


class ServiceURLsConfig(BaseModel):
    app_root: str = "http://localhost:8000"


class SlackConfig(BaseModel):
    slack_webhook_prefect_block_name: str = "slack-webhook"


class MaskReplacement(BaseModel):
    match: Pattern = Field(
        ..., description="A compiled regex pattern which, when matched, will be masked"
    )
    replacement: str = Field(
        default="***", description="A string to replace occurences of match with"
    )


class LogMaskingConfig(BaseModel):
    patterns: List[MaskReplacement] = [
        MaskReplacement(
            match=re.compile(r"(?i)(-password(?:=|\s))(['\"]?)(.*?)(\2)(?=\s|$)"),
            replacement=r"\1\2*****\2",
        )
    ]


class EMGConfig(BaseSettings):
    amplicon_pipeline: AmpliconPipelineConfig = AmpliconPipelineConfig()
    assembler: AssemblerConfig = AssemblerConfig()
    ena: ENAConfig = ENAConfig()
    environment: str = "development"
    legacy_service: LegacyServiceConfig = LegacyServiceConfig()
    service_urls: ServiceURLsConfig = ServiceURLsConfig()
    slack: SlackConfig = SlackConfig()
    slurm: SlurmConfig = SlurmConfig()
    webin: WebinConfig = WebinConfig()
    log_masking: LogMaskingConfig = LogMaskingConfig()

    model_config = {
        "env_prefix": "emg_",
        "env_nested_delimiter": "__",
    }
