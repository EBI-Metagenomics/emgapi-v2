import uuid
from datetime import timedelta

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy


@flow(name="Run the VIRify pipeline batch")
def run_virify_batch(assembly_analyses_batch_id: uuid.UUID):
    """
    Runs the VIRify pipeline for a batch of assemblies.

    It expects the analyses to have been completed by ASA at this point. It will select those
    analyses and uses the ASA end-of-run virify samplesheet to pick up the assemblies to execute.

    It doesn't validate the results or import them, but the results will be stored in the batch workspace.

    :param assembly_analyses_batch_id: Unique identifier for the assembly batch to be
        processed by the VIRify pipeline.
    :type assembly_analyses_batch_id: uuid.UUID
    :raises ClusterJobFailedException: If the VIRify pipeline execution job fails.
    :raises Exception: For any unexpected errors during pipeline execution.
    """
    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    # Store Prefect flow run ID
    assembly_analysis_batch.virify_flow_run_id = flow_run.id
    assembly_analysis_batch.save()

    # Record pipeline version
    assembly_analysis_batch.set_pipeline_version(
        AssemblyAnalysisPipeline.VIRIFY,
        EMG_CONFIG.virify_pipeline.pipeline_git_revision,
    )

    # Mark the batch as virify-in-progress
    assembly_analysis_batch.set_pipeline_status(
        AssemblyAnalysisPipeline.VIRIFY,
        AssemblyAnalysisPipelineStatus.RUNNING,
    )

    # Get ASA output directory to find samplesheet
    assembly_analysis_pipeline_outdir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.ASA.value
    )

    # Path to the virify samplesheet
    virify_samplesheet_path = (
        assembly_analysis_pipeline_outdir
        / EMG_CONFIG.assembly_analysis_pipeline.downstream_samplesheets_folder
        / EMG_CONFIG.assembly_analysis_pipeline.virify_samplesheet
    )

    # Check if the virify samplesheet exists
    if not virify_samplesheet_path.exists():
        logger.warning(
            f"Virify samplesheet {virify_samplesheet_path} does not exist. Skipping virify pipeline."
        )
        assembly_analysis_batch.last_error = (
            f"VIRify samplesheet not found at {virify_samplesheet_path}"
        )
        # Mark all batch relations as failed for VIRify
        assembly_analysis_batch.batch_analyses.update(
            virify_status=AssemblyAnalysisPipelineStatus.FAILED
        )

        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.VIRIFY,
            AssemblyAnalysisPipelineStatus.FAILED,
        )
        return

    assembly_analysis_batch.virify_samplesheet_path = str(virify_samplesheet_path)
    assembly_analysis_batch.save()

    mgnify_study = assembly_analysis_batch.study

    # Create VIRify workspace
    virify_outdir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.VIRIFY.value
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
            ("-config", EMG_CONFIG.virify_pipeline.pipeline_config_file),
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
            + f"{',TOWER_ACCESS_TOKEN' if EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Virify pipeline for study {mgnify_study.ena_study.accession}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.virify_pipeline.pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.virify_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[virify_samplesheet_path],
            working_dir=virify_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except Exception as e:
        error_type = (
            "VIRIfy pipeline failed"
            if isinstance(e, ClusterJobFailedException)
            else "Unexpected error in VIRIfy pipeline"
        )
        logger.error(f"{error_type} for study {mgnify_study.ena_study.accession}: {e}")

        # Mark batch as failed (VIRify failure doesn't propagate to analyses)
        assembly_analysis_batch.last_error = str(e)
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.VIRIFY,
            AssemblyAnalysisPipelineStatus.FAILED,
        )
        assembly_analysis_batch.batch_analyses.filter(
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).update(virify_status=AssemblyAnalysisPipelineStatus.FAILED)
    else:
        logger.info("VIRify pipeline completed successfully")

        # Mark analyses that completed ASA as VIRify completed
        completed_count = assembly_analysis_batch.batch_analyses.filter(
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).update(virify_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        logger.info(
            f"Marked {completed_count} ASA completed analyses as VIRify completed "
            f"(out of {assembly_analysis_batch.total_analyses} total)"
        )

        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.VIRIFY,
            AssemblyAnalysisPipelineStatus.COMPLETED,
        )
        logger.info("VIRify pipeline completed successfully and marked as COMPLETED")
