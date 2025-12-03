import re
from datetime import timedelta
from typing import List, Pattern

from pydantic import AnyHttpUrl, BaseModel, Field
from pydantic.networks import MongoDsn, MySQLDsn
from pydantic_settings import BaseSettings

from workflows.ena_utils.abstract import ENAPortalDataPortal


class SlurmConfig(BaseModel):
    default_job_status_checks_limit: int = 10
    default_workdir: str = "/nfs/production/dev-slurm-work-dir"
    pipelines_root_dir: str = "/app/workflows/pipelines"
    ftp_results_dir: str = "/nfs/ftp/public/databases/metagenomics/mgnify_results"
    private_results_dir: str = "/nfs/public/services/private-data"
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


class MGnifyPipelineConfig(BaseModel):
    """
    Base configuration for MGnify Nextflow pipelines.

    Provides common default values for pipeline repository, config file, and profile.
    Subclasses must override pipeline_repo and pipeline_git_revision.
    """

    pipeline_repo: str = ...  # Required
    pipeline_git_revision: str = ...  # Required
    pipeline_config_file: str = "/nfs/public/donco.config"
    pipeline_nf_profile: str = "codon"
    has_fire_access: bool = True  # Only available on-prem @ EBI

    # Basic resources
    pipeline_time_limit_days: int = 1
    samplesheet_chunk_size: int = 50
    nextflow_master_job_memory_gb: int = 8


class AssemblerConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/miassembler"
    pipeline_git_revision: str = "v3.0.3"

    # Resources
    pipeline_time_limit_days: int = 5
    assembly_uploader_mem_gb: int = 4
    assembly_uploader_time_limit_hrs: int = 2

    # Settings
    assembler_default: str = "metaspades"
    assembler_version_default: str = "3.15.5"
    suspend_timeout_for_editing_samplesheets_secs: int = 28800  # 8 hrs


class AmpliconPipelineConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/amplicon-analysis-pipeline"
    pipeline_git_revision: str = "v6.0.5"

    # Resources
    pipeline_time_limit_days: int = 5
    samplesheet_chunk_size: int = 50
    nextflow_master_job_memory_gb: int = 1

    # Settings
    allow_non_insdc_run_names: bool = False
    keep_study_summary_partials: bool = False

    # End-of-run reports
    completed_runs_csv: str = "qc_passed_runs.csv"
    failed_runs_csv: str = "qc_failed_runs.csv"

    # Results folders
    qc_folder: str = "qc"
    sequence_categorisation_folder: str = "sequence-categorisation"
    amplified_region_inference_folder: str = "amplified-region-inference"
    asv_folder: str = "asv"
    primer_identification_folder: str = "primer-identification"
    taxonomy_summary_folder: str = "taxonomy-summary"


class RawReadsPipelineConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/raw-reads-analysis-pipeline"
    pipeline_git_revision: str = "master"

    # Resources
    pipeline_time_limit_days: int = 5
    samplesheet_chunk_size: int = 50
    # TODO: remove this one, it is part of the default pipelines config
    base_workdir: str = (
        "/hps/nobackup/rdf/metagenomics/service-team/nextflow-workdir/rawreads-pipeline"
    )

    # Settings
    allow_non_insdc_run_names: bool = False
    keep_study_summary_partials: bool = False

    # End-of-run reports
    completed_runs_csv: str = "qc_passed_runs.csv"
    failed_runs_csv: str = "qc_failed_runs.csv"

    # Results folders
    qc_folder: str = "qc"
    taxonomy_summary_folder: str = "taxonomy-summary"
    function_summary_folder: str = "function-summary"

    # Analysis sources
    taxonomy_analysis_sources: set = {"silva-ssu", "silva-lsu", "motus"}
    function_analysis_sources: set = {"pfam"}


class AssemblyAnalysisPipelineConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/assembly-analysis-pipeline"
    pipeline_git_revision: str = "dev"
    pipeline_time_limit_days: int = 5

    # Resources
    samplesheet_chunk_size: int = 10
    max_analyses_per_study: int = None  # Safety cap, None = unlimited
    nextflow_master_job_memory_gb: int = 1

    # Workdir
    workdir_root: str = "/nfs/public/wd"

    # End-of-run reports
    completed_assemblies_csv: str = "analysed_assemblies.csv"
    qc_failed_assemblies: str = "qc_failed_assemblies.csv"

    # Results folders
    # TODO: there is some repetition with the Pipeline Schemas used to validate and import results
    qc_folder: str = "qc"
    cds_folder: str = "cds"
    taxonomy_folder: str = "taxonomy"
    functional_annotation_folder: str = "functional-annotation"
    pathways_systems_folder: str = "pathways-and-systems"
    annotation_summary_folder: str = "annotation-summary"

    # Downstream samplesheets
    downstream_samplesheets_folder: str = "downstream_samplesheets"
    virify_samplesheet: str = "virify_samplesheet.csv"


class VirifyPipelineConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/emg-viral-pipeline"
    pipeline_git_revision: str = "v3.0.0"
    pipeline_time_limit_days: int = 1

    # Resources
    nextflow_master_job_memory_gb: int = 8

    # Results folders
    final_gff_folder: str = "08-final/gff"


class MapPipelineConfig(MGnifyPipelineConfig):
    pipeline_repo: str = "ebi-metagenomics/mobilome-annotation-pipeline"
    pipeline_git_revision: str = "v4.1.0"
    pipeline_time_limit_days: int = 1

    # Resources
    nextflow_master_job_memory_gb: int = 8

    # Results folders
    final_gff_folder: str = "gff"


class WebinConfig(BaseModel):
    emg_webin_account: str = None
    emg_webin_password: str = None
    dcc_account: str = "dcc_metagenome"
    dcc_password: str = None
    submitting_center_name: str = "EMG"
    webin_cli_executor: str = "/usr/bin/webin-cli/webin-cli.jar"
    aspera_ascp_executor: str = None
    broker_prefix: str = "mg-"
    broker_password: str = None
    webin_cli_retries: int = 6
    webin_cli_retry_delay_seconds: int = 60
    auth_endpoint: AnyHttpUrl = "https://www.ebi.ac.uk/ena/submit/webin/auth"
    jwt_secret_key: str = None
    jwt_expiration_minutes: int = (
        1440  # TODO: shorten once https://github.com/eadwinCode/django-ninja-jwt/issues/33 is fixed
    )
    jwt_refresh_expiration_hours: int = 24


class ENAConfig(BaseModel):
    portal_search_api: AnyHttpUrl = "https://www.ebi.ac.uk/ena/portal/api/search"
    portal_search_api_default_data_portals: list[ENAPortalDataPortal] = [
        ENAPortalDataPortal.METAGENOME,
        ENAPortalDataPortal.ENA,
    ]
    portal_search_api_max_retries: int = 4
    portal_search_api_retry_delay_seconds: int = 15
    browser_view_url_prefix: AnyHttpUrl = "https://www.ebi.ac.uk/ena/browser/view"
    # TODO: migrate to the ENA Handler
    study_metadata_fields: list[str] = [
        "study_title",
        "study_description",
        "center_name",
        "secondary_study_accession",
        "study_name",
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


class GenomeConfig(BaseModel):
    mags_ftp_site: str = (
        "http://ftp.ebi.ac.uk/pub/databases/metagenomics/mgnify_genomes/"
    )
    latest_mags_pipeline_tag: str = "v1.2.1"
    results_directory_root: str = "/nfs/donco/results"


class EuropePMCConfig(BaseModel):
    annotations_endpoint: str = Field(
        "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"
    )
    annotations_provider: str = Field("Metagenomics")


class EMGConfig(BaseSettings):
    amplicon_pipeline: AmpliconPipelineConfig = AmpliconPipelineConfig()
    rawreads_pipeline: RawReadsPipelineConfig = RawReadsPipelineConfig()
    assembly_analysis_pipeline: AssemblyAnalysisPipelineConfig = (
        AssemblyAnalysisPipelineConfig()
    )
    assembler: AssemblerConfig = AssemblerConfig()
    virify_pipeline: VirifyPipelineConfig = VirifyPipelineConfig()
    map_pipeline: MapPipelineConfig = MapPipelineConfig()
    ena: ENAConfig = ENAConfig()
    environment: str = "development"
    legacy_service: LegacyServiceConfig = LegacyServiceConfig()
    service_urls: ServiceURLsConfig = ServiceURLsConfig()
    slurm: SlurmConfig = SlurmConfig()
    webin: WebinConfig = WebinConfig()
    log_masking: LogMaskingConfig = LogMaskingConfig()
    europe_pmc: EuropePMCConfig = EuropePMCConfig()
    genomes: GenomeConfig = GenomeConfig()

    model_config = {
        "env_prefix": "emg_",
        "env_nested_delimiter": "__",
    }
