import uuid
from datetime import timedelta
from pathlib import Path

from django.db import close_old_connections
from django.db.models import Q
from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG

from analyses.models import Study, Analysis
from workflows.flows.analysis.assembly.flows.import_map_batch import import_map_batch
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
from workflows.flows.analysis.assembly.utils.status_update_hooks import (
    update_batch_status_counts,
)
from workflows.flows.analyse_study_tasks.cleanup_pipeline_directories import (
    remove_dir,
)


@flow(
    flow_run_name="Run MAP Batch: {assembly_analyses_batch_id}",
    on_running=[update_batch_status_counts],
    on_completion=[update_batch_status_counts],
    on_failure=[update_batch_status_counts],
    on_crashed=[update_batch_status_counts],
    on_cancellation=[update_batch_status_counts],
    retries=2,
    retry_delay_seconds=60,
)
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

    # TODO: REFACTOR: There is a bit of repetition on this method:
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

    # Selection filters for analyses ready for MAP:
    # - VIRify must have finished (either COMPLETED or FAILED)
    # - If FAILED, it must be because of ANALYSIS_QC_FAILED
    # - MAP must not be COMPLETED yet
    analyses_to_process = assembly_analysis_batch.batch_analyses.filter(
        Q(virify_status=AssemblyAnalysisPipelineStatus.COMPLETED)
        | Q(
            virify_status=AssemblyAnalysisPipelineStatus.FAILED,
            analysis__status__contains={
                Analysis.AnalysisStates.ANALYSIS_QC_FAILED: True
            },
        )
    ).exclude(map_status=AssemblyAnalysisPipelineStatus.COMPLETED)

    if not analyses_to_process.exists():
        logger.warning(
            "No analyses ready for MAP execution (all already COMPLETED or none finished VIRify), skipping"
        )
        return

    logger.info(
        f"Processing {analyses_to_process.count()} analyses for MAP (skipping already-completed)"
    )

    # Create the MAP samplesheet using the updated task
    # Note: we pass the IDs of the analyses to process
    analyses_to_process_ids = list(analyses_to_process.values_list("id", flat=True))

    map_samplesheet_path, _ = make_samplesheet_for_map(
        assembly_analysis_batch.id,
        analysis_batch_job_ids=analyses_to_process_ids,
        output_dir=Path(assembly_analysis_batch.workspace_dir) / "samplesheets",
    )

    # Mark selected analyses as RUNNING
    if not map_samplesheet_path:
        logger.warning("No MAP samplesheet generated, skipping MAP execution")
        return

    # Check if the MAP samplesheet exists
    if not map_samplesheet_path.exists():
        # TODO: this chunk of code needs to be simplified, it is repeated downstream and in the makesamplesheet task
        error = f"MAP samplesheet {map_samplesheet_path} does not exist."
        logger.error(error)
        assembly_analysis_batch.last_error = error
        assembly_analysis_batch.save()
        # Only mark RUNNING analyses as FAILED (don't overwrite already-COMPLETED ones)
        assembly_analysis_batch.batch_analyses.filter(
            id__in=analyses_to_process_ids
        ).update(map_status=AssemblyAnalysisPipelineStatus.FAILED)
        return

    analyses_to_process.update(map_status=AssemblyAnalysisPipelineStatus.RUNNING)

    mgnify_study: Study = assembly_analysis_batch.study

    # Store samplesheet path
    assembly_analysis_batch.map_samplesheet_path = str(map_samplesheet_path)
    assembly_analysis_batch.save()

    map_outdir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.MAP.value
    )

    logger.info(f"Using output dir {map_outdir} for MAP pipeline")

    nextflow_workdir = (
        Path(f"{EMG_CONFIG.slurm.default_nextflow_workdir}")
        / Path(f"{mgnify_study.ena_study.accession}")
        / f"{EMG_CONFIG.map_pipeline.pipeline_name}_{EMG_CONFIG.map_pipeline.pipeline_version}"
        / f"{assembly_analysis_batch.id}"
    )
    nextflow_workdir.mkdir(parents=True, exist_ok=True)

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
            # "-latest", this was causing issues - Cannot lock pack in assembly-analysis-pipeline/.git/objects/pack/pack-e....pack
            (
                "-profile",
                EMG_CONFIG.map_pipeline.pipeline_nf_profile,
            ),
            ("-config", EMG_CONFIG.map_pipeline.pipeline_config_file),
            "-resume",
            ("-work-dir", nextflow_workdir),
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
        assembly_analysis_batch.save()
        # Only mark RUNNING analyses as FAILED (don't overwrite already-COMPLETED ones)
        assembly_analysis_batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.RUNNING
        ).update(map_status=AssemblyAnalysisPipelineStatus.FAILED)

        return
    else:
        remove_dir(nextflow_workdir)  # will also delete past "abandoned" nextflow files

        logger.info("MAP pipeline completed successfully")

    logger.info("MAP pipeline completed successfully")

    # Mark only RUNNING analyses as MAP completed (don't overwrite already-COMPLETED ones)
    completed_count = assembly_analysis_batch.batch_analyses.filter(
        map_status=AssemblyAnalysisPipelineStatus.RUNNING
    ).update(map_status=AssemblyAnalysisPipelineStatus.COMPLETED)

    logger.info(
        f"Marked {completed_count} analyses as MAP completed "
        f"(out of {assembly_analysis_batch.total_analyses} total)"
    )

    ##################
    # Import results #
    ##################
    import_map_batch(assembly_analyses_batch_id=assembly_analysis_batch.id)
