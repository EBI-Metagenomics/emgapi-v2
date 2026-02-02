import uuid
from datetime import timedelta
from pathlib import Path

from django.utils.text import slugify
from django.db import close_old_connections
from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG

from analyses.models import Study
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_assembly_batch_results,
)
from workflows.flows.analysis.assembly.flows.import_asa_batch import import_asa_batch
from workflows.flows.analysis.assembly.flows.run_map_batch import run_map_batch
from workflows.flows.analysis.assembly.flows.run_virify_batch import (
    run_virify_batch,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator import (
    generate_assembly_analysis_pipeline_batch_summary,
)
from workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly import (
    make_samplesheet_assembly,
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


@flow(
    name="Run assembly analysis pipeline-v6 for an assembly analysis batch (ASA, VIRIfy and MAP)",
    flow_run_name="Run Assembly Analysis Batch: {assembly_analyses_batch_id}",
    on_completion=[update_batch_status_counts],
    on_failure=[update_batch_status_counts],
    on_crashed=[update_batch_status_counts],
    on_cancellation=[update_batch_status_counts],
    retries=2,
    retry_delay_seconds=60,
)
def run_assembly_analysis_pipeline_batch(
    assembly_analyses_batch_id: uuid.UUID,
):
    """
    Run the assembly analysis pipeline-v6 for a given batch, including ASA, VIRify, and MAP.

    This function orchestrates the execution of the ASA pipeline, VIRify, and MAP. It handles tasks such as generating
    samplesheets, managing pipeline states, and storing relevant metadata into the database for further processing.

    Additionally, it imports analysis download files for each pipeline, validating results, and generates
    the study summary files for the batch.

    The flow is idempotent and can be restarted from any point:
    - ASA: Only processes analyses without asa_status=COMPLETED
    - VIRify: Only processes analyses with asa_status=COMPLETED and virify_status!=COMPLETED
    - MAP: Only processes analyses with virify_status=COMPLETED and map_status!=COMPLETED

    :param assembly_analyses_batch_id: Unique identifier for the assembly analysis batch to process.
    :type assembly_analyses_batch_id: uuid.UUID
    :raises ClusterJobFailedException: If the cluster job fails during the ASA pipeline execution.
    :raises Exception: For any unexpected errors encountered during the pipeline execution.
    """
    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    # Store Prefect flow run ID, this will be persisted in the db later in the code
    assembly_analysis_batch.asa_flow_run_id = flow_run.id

    # Each batch is used to keep track of the pipelines executed.
    assembly_analysis_batch.set_pipeline_version(
        AssemblyAnalysisPipeline.ASA,
        EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
    )

    mgnify_study: Study = assembly_analysis_batch.study

    # Check if all analyses are already completed for ASA
    # Only process analyses that haven't completed ASA yet
    analyses_to_process = assembly_analysis_batch.batch_analyses.exclude(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
    )

    if not analyses_to_process.exists():
        logger.info(
            "All analyses already have ASA COMPLETED status, skipping ASA execution"
        )
    else:
        logger.info(
            f"Processing {analyses_to_process.count()} analyses for ASA (skipping already-completed)"
        )

        # Mark analyses being processed as RUNNING
        analyses_to_process.update(asa_status=AssemblyAnalysisPipelineStatus.RUNNING)

        analyses_to_process_objs = assembly_analysis_batch.analyses.filter(
            id__in=analyses_to_process.values_list("analysis_id", flat=True)
        )

        # TODO: we are working out what is the best way to handle this
        #       mbc added this one so users know that the jobs are running
        #       running this on_running won't do the trick as refreshing the counts
        #       when this flow starts won't work
        #       The discussion in this PR: https://github.com/EBI-Metagenomics/emgapi-v2/pull/216
        assembly_analysis_batch.update_pipeline_status_counts()

        # Generate ASA samplesheet using the task - only for analyses that need processing
        samplesheet, _ = make_samplesheet_assembly(
            assembly_analysis_batch.study,
            analyses_to_process_objs,
            output_dir=Path(assembly_analysis_batch.workspace_dir) / "samplesheets",
        )

        # Store samplesheet path
        assembly_analysis_batch.asa_samplesheet_path = str(samplesheet)
        assembly_analysis_batch.save()

        assembly_analyses_workspace_dir = (
            assembly_analysis_batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
        )

        logger.info(
            f"Using {assembly_analyses_workspace_dir} as the batch nextflow outdir for ASA pipeline"
        )
        nextflow_workdir = (
            Path(assembly_analysis_batch.workspace_dir)
            / f"asa-sheet-{slugify(samplesheet)}"
        )
        nextflow_workdir.mkdir(exist_ok=True)

        command = cli_command(
            [
                (
                    "nextflow",
                    "run",
                    EMG_CONFIG.assembly_analysis_pipeline.pipeline_repo,
                ),
                (
                    "-r",
                    EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
                ),
                # "-latest", this was causing issues - Cannot lock pack in assembly-analysis-pipeline/.git/objects/pack/pack-e....pack
                (
                    "-c",
                    EMG_CONFIG.assembly_analysis_pipeline.pipeline_config_file,
                ),
                (
                    "-profile",
                    EMG_CONFIG.assembly_analysis_pipeline.pipeline_nf_profile,
                ),
                "-resume",
                (
                    "-work-dir",
                    Path(EMG_CONFIG.assembly_analysis_pipeline.workdir_root)
                    / mgnify_study.first_accession
                    / "asa",
                ),
                EMG_CONFIG.assembly_analysis_pipeline.has_fire_access
                and "--use_fire_download",
                ("--input", samplesheet),
                ("--outdir", assembly_analyses_workspace_dir),
                ("-work-dir", nextflow_workdir),
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
                name=f"Analyse assembly study {mgnify_study.ena_study.accession} via samplesheet {slugify(str(samplesheet))}",
                command=command,
                expected_time=timedelta(
                    days=EMG_CONFIG.assembly_analysis_pipeline.pipeline_time_limit_days
                ),
                memory=f"{EMG_CONFIG.assembly_analysis_pipeline.nextflow_master_job_memory_gb}G",
                environment=env_variables,
                input_files_to_hash=[samplesheet],
                working_dir=assembly_analyses_workspace_dir,
                resubmit_policy=ResubmitAlwaysPolicy,  # Let Nextflow handle resubmissions
            )
            # This is required because a flow may need a few days to run, and when that is done, the connection to
            # psql is going to be closed or dead at least
            close_old_connections()
        except Exception as e:

            close_old_connections()

            error_type = (
                "ASA pipeline failed"
                if isinstance(e, ClusterJobFailedException)
                else "Unexpected error in ASA pipeline"
            )
            logger.error(
                f"{error_type} for study {mgnify_study.ena_study.accession}: {e}"
            )
            assembly_analysis_batch.last_error = str(e)
            assembly_analysis_batch.save()
            # Only mark RUNNING analyses as FAILED (don't overwrite already-COMPLETED ones)
            assembly_analysis_batch.batch_analyses.filter(
                asa_status=AssemblyAnalysisPipelineStatus.RUNNING
            ).update(asa_status=AssemblyAnalysisPipelineStatus.FAILED)

            return

    ######################
    # Import the results #
    ######################
    import_asa_batch(assembly_analyses_batch_id)

    ##################
    # === VIRify === #
    ##################
    logger.info("Starting VIRify pipeline")
    run_virify_batch(assembly_analyses_batch_id=assembly_analysis_batch.id)
    close_old_connections()

    ###############
    # === MAP === #
    ###############
    logger.info("Starting MAP pipeline")
    run_map_batch(
        assembly_analyses_batch_id=assembly_analysis_batch.id,
    )
    # Just in case the connection was closed because previous steps took a long time
    close_old_connections()

    # At this point, the batch is completed and all analyses are in the DB. #

    #######################################
    # === Study summary for the batch === #
    #######################################
    # The study summary is generated from the ASA results, VIRIfy and MAP results are layered on top of the GFF
    # Only generate a summary if there are successfully imported ASA analyses, which mbc thing is a good idea
    # as it will allow us to have some results on the website...
    # TODO: review this decision
    assembly_analysis_batch.refresh_from_db()
    successfully_imported_asa_count = assembly_analysis_batch.batch_analyses.filter(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
    ).count()

    if successfully_imported_asa_count > 0:
        logger.info(
            f"Generating study summary for {successfully_imported_asa_count} successfully imported ASA analyses"
        )
        generate_assembly_analysis_pipeline_batch_summary(assembly_analyses_batch_id)
    else:
        logger.warning(
            "No successfully imported ASA analyses, skipping study summary generation"
        )

    #######################################
    # === Sync results for the batch === #
    #######################################
    copy_assembly_batch_results(assembly_analyses_batch_id)
