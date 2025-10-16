import uuid
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run

import analyses.models
from activate_django_first import EMG_CONFIG
from analyses.models import PipelineState
from workflows.data_io_utils.schemas.virify import VirifyResultSchema
from workflows.data_io_utils.schemas.validation import sanity_check_pipeline_results

from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task
def add_virify_gff_to_analysis_downloads(
    analysis: analyses.models.Analysis,
    virify_outdir: Path,
):
    """
    Add the virify GFF file to the analysis downloads using unified schema.

    :param analysis: The analysis to add the download to
    :param virify_outdir: The output directory of the virify pipeline
    """
    logger = get_run_logger()

    try:
        # Validate results first
        schema = VirifyResultSchema()

        sanity_check_pipeline_results(
            schema, virify_outdir, analysis.assembly.first_accession, "VIRify"
        )

        # Use schema to generate downloads
        downloads = schema.generate_downloads(analysis, virify_outdir)

        # Add downloads to analysis
        for download in downloads:
            try:
                analysis.add_download(download)
                logger.info(
                    f"Added VIRify file {download.alias} to analysis {analysis.accession} downloads"
                )
            except FileExistsError:
                logger.warning(
                    f"Download with alias {download.alias} already exists for analysis {analysis.accession}"
                )
    except Exception as e:
        logger.error(f"Failed to add VIRify downloads for {analysis.accession}: {e}")
        raise


@flow(name="Run VIRify pipeline batch")
def run_virify_batch(
    assembly_batch_id: uuid.UUID,
):
    """
    Run the VIRIfy pipeline for a batch of assemblies.
    Updates business state at milestones - Prefect tracks execution details.

    :param assembly_batch_id: The AssemblyAnalysisBatch to process
    """
    logger = get_run_logger()

    assembly_batch = analyses.models.AssemblyAnalysisBatch.objects.get(
        id=assembly_batch_id
    )

    # Store Prefect flow run ID
    assembly_batch.virify_flow_run_id = flow_run.id
    assembly_batch.save()

    # Record pipeline version
    assembly_batch.set_pipeline_version(
        analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
        EMG_CONFIG.virify_pipeline.pipeline_git_revision,
    )

    # Mark the batch as virify-in-progress
    assembly_batch.set_pipeline_state(
        analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
        PipelineState.IN_PROGRESS,
    )

    # Get ASA output directory to find samplesheet
    asa_outdir = assembly_batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA.value
    )

    # Path to the virify samplesheet
    virify_samplesheet_path = (
        asa_outdir
        / EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
        / EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )

    # Check if the virify samplesheet exists
    if not virify_samplesheet_path.exists():
        logger.warning(
            f"Virify samplesheet {virify_samplesheet_path} does not exist. Skipping virify pipeline."
        )
        assembly_batch.last_error = (
            f"VIRify samplesheet not found at {virify_samplesheet_path}"
        )
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
            PipelineState.FAILED,
        )
        return

    assembly_batch.virify_samplesheet_path = str(virify_samplesheet_path)
    assembly_batch.save()

    mgnify_study = assembly_batch.study

    # Create VIRify workspace
    # TODO: should we store this in an "enumerated" dir?
    virify_outdir = assembly_batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY.value
    )

    logger.info(f"Using output dir {virify_outdir} for VIRify pipeline")

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
    except ClusterJobFailedException as cluster_exception:
        logger.error(
            f"Virify pipeline failed for study {mgnify_study.ena_study.accession}: {cluster_exception}"
        )
        # Mark batch as failed (VIRify failure doesn't propagate to analyses)
        assembly_batch.last_error = str(cluster_exception)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
            PipelineState.FAILED,
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error in VIRify pipeline: {e}")
        # Mark batch as failed (VIRify failure doesn't propagate to analyses)
        assembly_batch.last_error = str(e)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
            PipelineState.FAILED,
        )
        raise
    else:
        # Add the virify GFF file to each analysis's downloads
        for analysis in assembly_batch.analyses:
            add_virify_gff_to_analysis_downloads(analysis, virify_outdir)

        # Mark VIRify pipeline as ready
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY,
            PipelineState.READY,
        )
        logger.info("VIRify pipeline completed successfully and marked as READY")
