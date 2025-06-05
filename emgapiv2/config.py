import re
from datetime import timedelta
from typing import List, Pattern

from pydantic import AnyHttpUrl, BaseModel, Field
from pydantic.networks import MongoDsn, MySQLDsn
from pydantic_settings import BaseSettings


class SlurmConfig(BaseModel):
    default_job_status_checks_limit: int = 10
    default_workdir: str = "/nfs/production/dev-slurm-work-dir"
    pipelines_root_dir: str = "/app/workflows/pipelines"
    ftp_results_dir: str = "/nfs/ftp/public/databases/metagenomics/mgnify_results"
    user: str = "root"

    incomplete_job_limit: int = 100
    # if this many jobs are RUNNING or PENDING, no more are submitted

    default_seconds_between_job_checks: int = 10
    # when a job is running, we wait this long between status checks

    default_seconds_between_submission_attempts: int = 10
    default_submission_attempts_limit: int = 100
    # if the cluster is "full", we wait this long before checking again for space,
    #   and only attempt submission a limited number of times before giving up.

    cluster_job_flow_timeout_seconds: int = timedelta(days=14).total_seconds()
    # if a cluster job flow is still running after this long, it is timed out to clear
    #   up cases where the prefect worker has been ended whilst the slurm job ran.

    job_log_tail_lines: int = 10
    # how many lines of slurm log to send to prefect each time we check it

    job_log_failure_tail_lines: int = 100
    # how many lines of final slurm log to send to prefect if a job fails

    use_nextflow_tower: bool = False
    nextflow_tower_org: str = "EMBL-EBI"
    nextflow_tower_workspace: str = "ebi-spws-dev-microbiome-info"

    datamover_paritition: str = "datamover"

    shared_filesystem_root_on_slurm: str = "/nfs/public"
    shared_filesystem_root_on_server: str = "/app/data"

    samplesheet_editing_allowed_inside: str = default_workdir
    samplesheet_editing_path_from_shared_filesystem: str = "temporary_samplesheet_edits"
    # allow django-admin access to edit csv/tsv files inside this dir

    preparation_command_job_memory_gb: int = 2
    # memory for jobs like `nextflow clean ...` or `rm -r ./work` that are run before bigger jobs


class AssemblerConfig(BaseModel):
    assembly_pipeline_repo: str = "ebi-metagenomics/miassembler"
    assembler_default: str = "metaspades"
    assembler_version_default: str = "3.15.5"
    miassemebler_git_revision: str = (
        "main"  # branch or commit of ebi-metagenomics/miassembler
    )
    miassembler_nf_profile: str = "codon_slurm"
    assembly_pipeline_time_limit_days: int = 5
    assembly_nextflow_master_job_memory_gb: int = 8

    assembly_uploader_mem_gb: int = 4
    assembly_uploader_time_limit_hrs: int = 2


class AmpliconPipelineConfig(BaseModel):
    amplicon_pipeline_repo: str = "ebi-metagenomics/amplicon-pipeline"
    amplicon_pipeline_git_revision: str = (
        "main"  # branch or commit of ebi-metagenomics/amplicon-pipeline
    )
    amplicon_pipeline_nf_profile: str = "codon_slurm"
    samplesheet_chunk_size: int = 50
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

    amplicon_nextflow_master_job_memory_gb: int = 1
    amplicon_pipeline_time_limit_days: int = 5

    allow_non_insdc_run_names: bool = False
    keep_study_summary_partials: bool = False


class AssemblyAnalysisPipelineConfig(BaseModel):
    pipeline_repo: str = "ebi-metagenomics/assembly-analysis-pipeline"
    pipeline_git_revision: str = "dev"
    pipeline_nf_config: str = "test.config"
    pipeline_nf_profile: str = "debug"
    pipeline_time_limit_days: int = 5
    samplesheet_chunk_size: int = 10
    nextflow_master_job_memory_gb: int = 1
    completed_assemblies_csv: str = "qc_passed_assemblies.csv"
    failed_assemblies_csv: str = "qc_failed_assemblies.csv"
    taxonomy_folder: str = "taxonomy"
    functional_folder: str = "functional-annotation"


class WebinConfig(BaseModel):
    emg_webin_account: str = None
    emg_webin_password: str = None
    dcc_account: str = "dcc_metagenome"
    dcc_password: str = None
    submitting_center_name: str = "EMG"
    webin_cli_executor: str = "/usr/bin/webin-cli/webin-cli.jar"
    broker_prefix: str = "mg-"
    broker_password: str = None
    webin_cli_retries: int = 6
    webin_cli_retry_delay_seconds: int = 60
    auth_endpoint: AnyHttpUrl = "https://www.ebi.ac.uk/ena/submit/webin/auth"
    jwt_secret_key: str = None
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 60


class ENAConfig(BaseModel):
    portal_search_api: AnyHttpUrl = "https://www.ebi.ac.uk/ena/portal/api/search"
    portal_search_api_max_retries: int = 4
    portal_search_api_retry_delay_seconds: int = 15
    browser_view_url_prefix: AnyHttpUrl = "https://www.ebi.ac.uk/ena/browser/view"
    # TODO: migrate to the ENA Handler
    study_metadata_fields: list[str] = ["study_title", "secondary_study_accession"]

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
    base_url: str = ""
    transfer_services_url_root: str = (
        "http://localhost:8080/pub/databases/metagenomics/mgnify_results/"
    )
    private_data_url_root: str = "http://localhost:8081/private-data/"


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
    assembly_analysis_pipeline: AssemblyAnalysisPipelineConfig = (
        AssemblyAnalysisPipelineConfig()
    )
    assembler: AssemblerConfig = AssemblerConfig()
    ena: ENAConfig = ENAConfig()
    environment: str = "development"
    legacy_service: LegacyServiceConfig = LegacyServiceConfig()
    service_urls: ServiceURLsConfig = ServiceURLsConfig()
    slurm: SlurmConfig = SlurmConfig()
    webin: WebinConfig = WebinConfig()
    log_masking: LogMaskingConfig = LogMaskingConfig()

    model_config = {
        "env_prefix": "emg_",
        "env_nested_delimiter": "__",
    }
