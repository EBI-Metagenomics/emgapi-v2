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
from workflows.flows.analyse_study_tasks.make_samplesheet_assembly import (
    make_samplesheet_for_map,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy
from workflows.data_io_utils.filenames import next_enumerated_subdir


@task
def add_map_gff_to_analysis_downloads(
    analysis: analyses.models.Analysis,
    map_outdir: Path,
):
    """
    Add the MAP (Mobilome Annotation Pipeline) GFF file to the analysis downloads.

    :param analysis: The analysis to add the download to
    :param map_outdir: The output directory of the MAP pipeline
    """
    logger = get_run_logger()

    # Check if the GFF directory exists
    if not map_outdir.exists():
        logger.warning(
            f"MAP output directory {map_outdir} does not exist. No GFF file to add to downloads."
        )
        return

    # Find GFF files in the directory
    # mobilome_prokka.gff contains the mobilome annotation plus any other feature
    # annotated by PROKKA, mobileOG, or ViPhOG.
    gff_file = map_outdir / "mobilome_prokka.gff"

    if not gff_file.exists():
        logger.warning(
            f"No GFF files found in {gff_file}. No GFF file to add to downloads."
        )
        return

    # Create a download file object
    download = DownloadFile(
        path=gff_file,
        alias=f"map_{gff_file.name}",
        download_type=DownloadType.GENOME_ANALYSIS,
        file_type=DownloadFileType.GFF,
        long_description="Mobilome annotation from MAP (Mobilome Annotation Pipeline)",
        short_description="Mobilome annotations",
        download_group="map",
    )

    # Add the download to the analysis
    try:
        analysis.add_download(download)
        logger.info(
            f"Added MAP GFF file {gff_file.name} to analysis {analysis.accession} downloads"
        )
    except FileExistsError:
        logger.warning(
            f"Download with alias {download.alias} already exists for analysis {analysis.accession}"
        )


@flow(name="Run MAP pipeline via samplesheet")
def run_map_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_analyses: List[analyses.models.Analysis],
    assembly_pipeline_outdir: Path,
):
    """
    Run the MAP (Mobilome Annotation Pipeline) for a set of assemblies using the downstream samplesheet.
    This pipeline takes a samplesheet produced by the combination of assembly-annotation-pipeline + virify.
    It processes a fasta file with proteins from the assembly-annotation-pipeline and the final GFF from virify.

    :param mgnify_study: The MGnify study
    :param assembly_analyses: List of assembly analyses
    :param assembly_pipeline_outdir: The output directory of the assembly pipeline
    """
    logger = get_run_logger()

    # Path to the MAP samplesheet
    # Create the MAP samplesheet using the make_samplesheet_for_map function
    map_samplesheet_path, _ = make_samplesheet_for_map(mgnify_study, assembly_analyses)

    # Check if the MAP samplesheet exists
    if not map_samplesheet_path.exists():
        logger.warning(
            f"MAP samplesheet {map_samplesheet_path} does not exist. Skipping MAP pipeline."
        )
        # TODO: update the status of the job
        # for assembly_analysis in assembly_analyses:
        #     assembly_analysis ..
        return

    # Mark analyses as started
    # Add the MAP GFF file to each analysis's downloads
    # TODO: I feel we need intermediate states here, otherwise the analysis goes from
    for analysis in assembly_analyses:
        analysis.mark_status(AnalysisStates.ANALYSIS_STARTED)

    # Create a new output directory for the MAP pipeline
    map_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_map/{flow_run.root_flow_run_id}"
    )

    map_outdir = next_enumerated_subdir(map_outdir_parent, mkdirs=True)
    logger.info(f"Using output dir {map_outdir} for MAP pipeline")

    # Build the command to run the MAP pipeline
    # TODO: Update this once MAP pipeline configuration is available in EMG_CONFIG
    command = cli_command(
        [
            (
                "nextflow",
                "run",
                EMG_CONFIG.virify_pipeline.pipeline_repo,  # Replace with MAP pipeline repo when available
            ),
            (
                "-r",
                EMG_CONFIG.virify_pipeline.pipeline_git_revision,  # Replace with MAP pipeline revision when available
            ),
            "-latest",
            (
                "-profile",
                EMG_CONFIG.virify_pipeline.pipeline_nf_profile,  # Replace with MAP pipeline profile when available
            ),
            "-resume",
            ("--samplesheet", map_samplesheet_path),
            ("--outdir", map_outdir),
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
            name=f"MAP pipeline for study {mgnify_study.ena_study.accession}",
            command=command,
            expected_time=timedelta(
                days=settings.EMG_CONFIG.virify_pipeline.pipeline_time_limit_days  # Replace with MAP pipeline time limit when available
            ),
            memory=f"{settings.EMG_CONFIG.virify_pipeline.nextflow_master_job_memory_gb}G",  # Replace with MAP pipeline memory when available
            environment=env_variables,
            input_files_to_hash=[map_samplesheet_path],
            working_dir=map_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        logger.error(
            f"MAP pipeline failed for study {mgnify_study.ena_study.accession}"
        )
        for analysis in assembly_analyses:
            mark_analysis_as_failed(analysis, reason="MAP pipeline failed")
        return

    for analysis in assembly_analyses:
        add_map_gff_to_analysis_downloads(analysis, map_outdir)

    # TODO: update status

    # # Generate study summary
    # This is not needed - we won't add anything from MAP to the study summary
    # generate_study_summary_for_pipeline_run(
    #     pipeline_outdir=map_outdir,
    #     mgnify_study_accession=mgnify_study.accession,
    #     analysis_type="map",
    #     completed_runs_filename="map_completed_runs.csv",
    # )
