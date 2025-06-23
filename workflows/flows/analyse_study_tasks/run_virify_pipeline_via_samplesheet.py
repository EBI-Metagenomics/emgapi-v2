from datetime import timedelta
from pathlib import Path
from typing import List

from django.conf import settings
from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run

import analyses.models
from activate_django_first import EMG_CONFIG

AnalysisStates = analyses.models.Analysis.AnalysisStates

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.flows.analyse_study_tasks.analysis_states import (
    mark_analysis_as_failed,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy
from workflows.data_io_utils.filenames import next_enumerated_subdir


@task(log_prints=True)
def add_virify_gff_to_analysis_downloads(
    analysis: analyses.models.Analysis,
    virify_outdir: Path,
):
    """
    Add the virify GFF file to the analysis downloads.

    :param analysis: The analysis to add the download to
    :param virify_outdir: The output directory of the virify pipeline
    """
    logger = get_run_logger()

    # Path to the GFF file
    gff_dir = virify_outdir / EMG_CONFIG.virify_pipeline.final_gff_folder

    # Check if the GFF directory exists
    if not gff_dir.exists():
        logger.warning(
            f"GFF directory {gff_dir} does not exist. No GFF file to add to downloads."
        )
        return

    # Find GFF files in the directory
    gff_files = list(gff_dir.glob("*.gff"))

    if not gff_files:
        logger.warning(
            f"No GFF files found in {gff_dir}. No GFF file to add to downloads."
        )
        return

    # Use the first GFF file found
    gff_file = gff_files[0]

    # Create a relative path from the analysis results directory
    rel_path = f"virify/{gff_file.name}"

    # Create a download file object
    download = DownloadFile(
        path=rel_path,
        alias=f"virify_{gff_file.name}",
        download_type=DownloadType.GENOME_ANALYSIS,
        file_type=DownloadFileType.GFF,
        long_description="Viral genome annotation from VIRify pipeline",
        short_description="Viral annotations",
        download_group="virify",
    )

    # Add the download to the analysis
    try:
        analysis.add_download(download)
        logger.info(
            f"Added virify GFF file {gff_file.name} to analysis {analysis.accession} downloads"
        )
    except FileExistsError:
        logger.warning(
            f"Download with alias {download.alias} already exists for analysis {analysis.accession}"
        )


@flow(name="Run VIRIfy pipeline via samplesheet", log_prints=True)
def run_virify_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_analyses: List[analyses.models.Analysis],
    assembly_pipeline_outdir: Path,
):
    """
    Run the VIRIfy pipeline for a set of assemblies using the downstream samplesheet.

    :param mgnify_study: The MGnify study
    :param assembly_analyses: List of assembly analyses
    :param assembly_pipeline_outdir: The output directory of the assembly pipeline
    """
    logger = get_run_logger()

    # Path to the virify samplesheet
    virify_samplesheet_path = (
        assembly_pipeline_outdir
        / EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
        / EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )

    # Check if the virify samplesheet exists
    if not virify_samplesheet_path.exists():
        logger.warning(
            f"Virify samplesheet {virify_samplesheet_path} does not exist. Skipping virify pipeline."
        )
        # TODO: update the status of the job
        # for assembly_analysis in assembly_analyses:
        #     assembly_analysis ..
        return

    # Mark analyses as started
    # Add the MAP GFF file to each analysis's downloads
    # TODO: I feel we need and intermediate status  here, otherwise the analysis will cycle through running/started a
    # a few times
    for analysis in assembly_analyses:
        analysis.mark_status(AnalysisStates.ANALYSIS_STARTED)

    # Create a new output directory for the virify pipeline
    virify_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_virify/{flow_run.root_flow_run_id}"
    )

    virify_outdir = next_enumerated_subdir(virify_outdir_parent, mkdirs=True)
    logger.info(f"Using output dir {virify_outdir} for virify pipeline")

    # Build the command to run the virify pipeline
    command = cli_command(
        [
            (
                "nextflow",
                "run",
                EMG_CONFIG.virify_pipeline.pipeline_repo,
            ),
            (
                "-r",
                EMG_CONFIG.virify_pipeline.pipeline_git_revision,
            ),
            "-latest",
            (
                "-profile",
                EMG_CONFIG.virify_pipeline.pipeline_nf_profile,
            ),
            "-resume",
            ("--samplesheet", virify_samplesheet_path),
            ("--outdir", virify_outdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-ansi-log", "false"),
        ]
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Virify pipeline for study {mgnify_study.ena_study.accession}",
            command=command,
            expected_time=timedelta(
                days=settings.EMG_CONFIG.virify_pipeline.pipeline_time_limit_days
            ),
            memory=f"{settings.EMG_CONFIG.virify_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[virify_samplesheet_path],
            working_dir=virify_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        logger.error(
            f"Virify pipeline failed for study {mgnify_study.ena_study.accession}"
        )
        for analysis in assembly_analyses:
            mark_analysis_as_failed(analysis, reason="Virify pipeline failed")
        return

    # Add the virify GFF file to each analysis's downloads
    for analysis in assembly_analyses:
        add_virify_gff_to_analysis_downloads(analysis, virify_outdir)

    # TODO: update status
    # # Generate study summary
    # This is not needed - we won't add anything from VIRfy to the study summary
    # generate_study_summary_for_pipeline_run(
    #     pipeline_outdir=virify_outdir,
    #     mgnify_study_accession=mgnify_study.accession,
    #     analysis_type="virify",
    #     completed_runs_filename="virify_completed_runs.csv",
    # )
