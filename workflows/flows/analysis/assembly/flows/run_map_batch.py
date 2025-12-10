import uuid
from datetime import timedelta
from pathlib import Path

from django.db import close_old_connections
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
from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy


@flow(name="Run MAP pipeline batch")
def run_map_batch(assembly_analyses_batch_id: uuid.UUID):
    """
    Run the MAP (Mobilome Annotation Pipeline) for a batch of assemblies.

    It expects the analyses to have been completed with ASA and VIRify at this point. It will select those
    analyses and generate a samplesheet for the MAP pipeline.

    The flow is idempotent: analyses that are already COMPLETED for MAP will be skipped.
    Only analyses with virify_status=COMPLETED and map_status!=COMPLETED will be processed.

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

    # Check if all analyses are already completed for MAP
    # Only process analyses that completed VIRify but haven't completed MAP yet
    analyses_to_process = assembly_analysis_batch.batch_analyses.filter(
        virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
    ).exclude(map_status=AssemblyAnalysisPipelineStatus.COMPLETED)

    if not analyses_to_process.exists():
        logger.warning(
            "All VIRify-completed analyses already have MAP COMPLETED status, skipping MAP execution"
        )
        # Update counts to reflect current state
        assembly_analysis_batch.update_pipeline_status_counts(
            AssemblyAnalysisPipeline.MAP
        )
        return

    logger.info(
        f"Processing {analyses_to_process.count()} analyses for MAP (skipping already-completed)"
    )

    # Mark only the analyses being processed as RUNNING (not the already-completed ones)
    analyses_to_process.update(map_status=AssemblyAnalysisPipelineStatus.RUNNING)

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
        # TODO: this chunk of code needs to be simplified, it is repeated downstream and in the makesamplesheet task
        error = f"MAP samplesheet {map_samplesheet_path} does not exist."
        logger.error(error)
        assembly_analysis_batch.last_error = error
        # Only mark RUNNING analyses as FAILED (don't overwrite already-COMPLETED ones)
        assembly_analysis_batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.RUNNING
        ).update(map_status=AssemblyAnalysisPipelineStatus.FAILED)

        # Update counts to reflect the issue
        assembly_analysis_batch.update_pipeline_status_counts(
            AssemblyAnalysisPipeline.MAP
        )
        return

    # Store samplesheet path
    assembly_analysis_batch.map_samplesheet_path = str(map_samplesheet_path)
    assembly_analysis_batch.save()

    map_outdir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.MAP.value
    )

    logger.info(f"Using output dir {map_outdir} for MAP pipeline")

    # TODO: we need to standardize pipelines params
    #       i.e. VIRIfy output => outdir
    #       i.e. MAP samplesheet => input

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
            (
                "-work-dir",
                Path(EMG_CONFIG.assembly_analysis_pipeline.workdir_root)
                / mgnify_study.first_accession
                / "map",
            ),
            ("--input", map_samplesheet_path),
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
            resubmit_policy=ResubmitAlwaysPolicy,  # We let Nextflow handle resubmissions
        )
        close_old_connections()
    except Exception as e:
        close_old_connections()
        error_type = (
            "MAP pipeline failed"
            if isinstance(e, ClusterJobFailedException)
            else "Unexpected error in MAP pipeline"
        )
        logger.error(f"{error_type} for study {mgnify_study.ena_study.accession}: {e}")

        # Mark batch as failed
        assembly_analysis_batch.last_error = str(e)
        # Only mark RUNNING analyses as FAILED (don't overwrite already-COMPLETED ones)
        assembly_analysis_batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.RUNNING
        ).update(map_status=AssemblyAnalysisPipelineStatus.FAILED)
        # Update counts to reflect failures
        assembly_analysis_batch.update_pipeline_status_counts(
            AssemblyAnalysisPipeline.MAP
        )
    else:
        logger.info("MAP pipeline completed successfully")

        # Mark only RUNNING analyses as MAP completed (don't overwrite already-COMPLETED ones)
        completed_count = assembly_analysis_batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.RUNNING
        ).update(map_status=AssemblyAnalysisPipelineStatus.COMPLETED)

        logger.info(
            f"Marked {completed_count} analyses as MAP completed "
            f"(out of {assembly_analysis_batch.total_analyses} total)"
        )

        # Update counts to reflect completions
        assembly_analysis_batch.update_pipeline_status_counts(
            AssemblyAnalysisPipeline.MAP
        )
        logger.info("MAP pipeline completed successfully, counts updated")
