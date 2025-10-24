import uuid
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG
from analyses.models import Study
from workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly import (
    make_samplesheet_for_map,
)
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


@flow(name="Run MAP pipeline batch")
def run_map_batch(assembly_analyses_batch_id: uuid.UUID):
    """
    Run the MAP (Mobilome Annotation Pipeline) for a batch of assemblies.

    It expects the analyses to have been completed with ASA and VIRify at this point. It will select those
    analyses and generate a samplesheet for the MAP pipeline.

    It doesn't validate the results or import them, but the results will be stored in the batch workspace.

    This flow won't raise an exception if the MAP pipeline fails unless raise_on_failure is set to True.
    This behavior is useful for batch processing, where we want to continue processing batches even if one fails.

    :param assembly_analyses_batch_id: The AssemblyAnalysisBatch to process
    :param raise_on_failure: Raise an exception if the MAP pipeline fails
    """

    # REFACTOR: There is a bit of repetition on this method:
    # `make_samplesheet_for_map` will also pull the analyses-batches from the DB
    # The status update process is done in a very "manual" way.. a few more generalistic methods in
    # Managers should help with this

    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    # Store Prefect flow run ID
    assembly_analysis_batch.map_flow_run_id = flow_run.id
    assembly_analysis_batch.save()

    # Record pipeline version
    assembly_analysis_batch.set_pipeline_version(
        AssemblyAnalysisPipeline.MAP,
        EMG_CONFIG.map_pipeline.pipeline_git_revision,
    )

    assembly_analysis_batch.set_pipeline_status(
        AssemblyAnalysisPipeline.MAP,
        AssemblyAnalysisPipelineStatus.RUNNING,
    )

    assembly_analyes_for_map = assembly_analysis_batch.batch_analyses.filter(
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
    )

    if assembly_analyes_for_map.count() == 0:
        logger.info("No analyses to run MAP on")
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.MAP,
            AssemblyAnalysisPipelineStatus.PENDING,
        )
        return

    assembly_analyes_for_map.update(map_status=AssemblyAnalysisPipelineStatus.RUNNING)

    # Create samplesheets directory in the batch workspace
    mgnify_study: Study = assembly_analysis_batch.study

    # Create the MAP samplesheet using the make_samplesheet_for_map function
    # The function uses the filesystem to find CDS and VIRify GFF files
    map_samplesheet_path, _ = make_samplesheet_for_map(
        assembly_analysis_batch.id,
        output_dir=Path(assembly_analysis_batch.workspace_dir) / "samplesheets",
    )

    # Check if the MAP samplesheet exists
    if not map_samplesheet_path.exists():
        logger.warning(
            f"MAP samplesheet {map_samplesheet_path} does not exist. Skipping MAP pipeline."
        )
        assembly_analysis_batch.last_error = (
            f"MAP samplesheet not found at {map_samplesheet_path}"
        )
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.MAP,
            AssemblyAnalysisPipelineStatus.FAILED,
        )
        return

    # Store samplesheet path
    assembly_analysis_batch.map_samplesheet_path = str(map_samplesheet_path)
    assembly_analysis_batch.save()

    map_outdir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.MAP.value
    )

    logger.info(f"Using output dir {map_outdir} for MAP pipeline")

    # Build the command to run the MAP pipeline
    command = cli_command(
        [
            (
                "nextflow",
                "run",
                EMG_CONFIG.map_pipeline.pipeline_repo,
            ),
            (
                "-r",
                EMG_CONFIG.map_pipeline.pipeline_git_revision,
            ),
            "-latest",
            (
                "-profile",
                EMG_CONFIG.map_pipeline.pipeline_nf_profile,
            ),
            ("-config", EMG_CONFIG.map_pipeline.pipeline_config_file),
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
            + f"{',TOWER_ACCESS_TOKEN' if EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"MAP pipeline for study {mgnify_study.ena_study.accession}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.map_pipeline.pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.map_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[map_samplesheet_path],
            working_dir=map_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except Exception as e:
        error_type = (
            "MAP pipeline failed"
            if isinstance(e, ClusterJobFailedException)
            else "Unexpected error in MAP pipeline"
        )
        logger.error(f"{error_type} for study {mgnify_study.ena_study.accession}: {e}")

        # Mark batch as failed (MAP failure doesn't propagate to analyses)
        assembly_analysis_batch.last_error = str(e)
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.MAP,
            AssemblyAnalysisPipelineStatus.FAILED,
        )
        assembly_analysis_batch.batch_analyses.filter(
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).update(map_status=AssemblyAnalysisPipelineStatus.FAILED)
    else:
        logger.info("MAP pipeline completed successfully")

        # Mark analyses that completed both ASA and VIRify as MAP completed
        completed_count = assembly_analysis_batch.batch_analyses.filter(
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).update(map_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        logger.info(
            f"Marked {completed_count} ASA and VIRify completed analyses as MAP completed "
            f"(out of {assembly_analysis_batch.total_analyses} total)"
        )

        # No need to propagate to the batch-anlaysis, otherwise it will overwrite the status,
        # For example, those were VIRIfy failed will be marked as MAP completed (which is not possible)
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.MAP, AssemblyAnalysisPipelineStatus.COMPLETED
        )
        logger.info("MAP pipeline completed successfully and marked as COMPLETED")
