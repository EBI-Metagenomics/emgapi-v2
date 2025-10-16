import uuid
from datetime import timedelta

from django.conf import settings
from django.utils.text import slugify
from prefect import flow, get_run_logger
from prefect.runtime import flow_run

import analyses.models
from analyses.models import PipelineState
from workflows.flows.analyse_study_tasks.assembly.import_completed_assembly_analyses import (
    import_completed_assembly_analyses,
)
from workflows.flows.analyse_study_tasks.assembly.run_map_batch import run_map_batch
from workflows.flows.analyse_study_tasks.assembly.run_virify_batch import (
    run_virify_batch,
)
from workflows.flows.analyse_study_tasks.assembly.set_post_assembly_analysis_states import (
    set_post_assembly_analysis_states,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    generate_assembly_batch_summary,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy

AnalysisStates = analyses.models.Analysis.AnalysisStates


@flow(name="Run assembly analysis pipeline-v6 for an assembly analysis batch")
def run_assembly_analysis_pipeline_batch(
    assembly_batch_id: uuid.UUID,
):
    """
    Run the assembly analysis pipeline for a batch of assembly analysis.
    Updates the state at milestones (ASA, MAP and VIRify).

    :param assembly_batch_id: The AssemblyAnalysisBatch to process
    """
    logger = get_run_logger()

    assembly_batch = analyses.models.AssemblyAnalysisBatch.objects.get(
        id=assembly_batch_id
    )

    # Store Prefect flow run ID
    assembly_batch.asa_flow_run_id = flow_run.id
    assembly_batch.save()

    # Record pipeline version
    assembly_batch.set_pipeline_version(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA,
        settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
    )

    # Mark the batch as in progress
    assembly_batch.set_pipeline_state(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA,
        PipelineState.IN_PROGRESS,
    )

    # Get analyses from batch
    assembly_analyses: list[analyses.models.Analysis] = list(
        assembly_batch.assembly_analysis_set.select_related("assembly").all()
    )
    mgnify_study: analyses.models.Study = assembly_batch.study

    # Generate samplesheet
    samplesheet = assembly_batch.generate_asa_samplesheet()

    # Create ASA workspace
    assembly_current_outdir = assembly_batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA
    )

    logger.info(f"Using output dir {assembly_current_outdir} for ASA pipeline")

    command = cli_command(
        [
            (
                "nextflow",
                "run",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_repo,
            ),
            (
                "-r",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
            ),
            "-latest",
            (
                "-c",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_nf_config,
            ),
            (
                "-profile",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_nf_profile,
            ),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", assembly_current_outdir),
            ("-name", f"asa-v6-sheet-{slugify(str(samplesheet))[-10:]}"),
            settings.EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-ansi-log", "false"),
        ]
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Analyse assembly study {mgnify_study.ena_study.accession} via samplesheet {slugify(str(samplesheet))}",
            command=command,
            expected_time=timedelta(
                days=settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_time_limit_days
            ),
            memory=f"{settings.EMG_CONFIG.assembly_analysis_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=assembly_current_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException as cluster_exception:
        logger.info(f"Cluster job failed: {cluster_exception}")
        assembly_batch.last_error = str(cluster_exception)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.ASA, PipelineState.FAILED
        )
        raise
    except Exception as e:
        logger.info(f"Unexpected error: {e}")
        assembly_batch.last_error = str(e)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.ASA, PipelineState.FAILED
        )
        raise
    else:
        logger.info("Running the import assembly analysis results process")
        # Set post-assembly analysis states based on pipeline output CSVs
        set_post_assembly_analysis_states(assembly_current_outdir, assembly_analyses)

        import_completed_assembly_analyses(assembly_current_outdir, assembly_analyses)

        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.ASA, PipelineState.READY
        )
        logger.info("ASA pipeline completed successfully and marked as READY")

        assembly_batch.refresh_from_db()
        if assembly_batch.is_ready_for_virify:
            logger.info("Starting VIRify pipeline")
            run_virify_batch(
                assembly_batch_id=assembly_batch.id,
            )
        else:
            # TODO: this should be considered an error, how do we handle this?
            pass

        assembly_batch.refresh_from_db()
        if assembly_batch.is_ready_for_map:
            logger.info("Starting MAP pipeline")
            run_map_batch(
                assembly_batch_id=assembly_batch.id,
            )
        else:
            # TODO: this should be considered an error, how do we handle this?
            pass

        # Generate batch-specific study summary
        generate_assembly_batch_summary(assembly_batch.id)
