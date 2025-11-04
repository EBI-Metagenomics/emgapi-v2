import uuid
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.utils.text import slugify
from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from analyses.models import Study, Analysis
from workflows.data_io_utils.schemas import PipelineValidationError
from workflows.flows.analysis.assembly.flows.run_map_batch import run_map_batch
from workflows.flows.analysis.assembly.flows.run_virify_batch import (
    run_virify_batch,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    assembly_analysis_batch_results_importer,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator import (
    generate_assembly_analysis_pipeline_batch_summary,
)
from workflows.flows.analysis.assembly.tasks.make_samplesheet_assembly import (
    make_samplesheet_assembly,
)
from workflows.flows.analysis.assembly.tasks.set_post_assembly_analysis_states import (
    set_post_assembly_analysis_states,
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


@flow(
    name="Run assembly analysis pipeline-v6 for an assembly analysis batch (ASA, VIRIfy and MAP)"
)
def run_assembly_analysis_pipeline_batch(
    assembly_analysis_batch_id: uuid.UUID,
):
    """
    Run the assembly analysis pipeline-v6 for a given batch, including ASA, VIRify, and MAP.

    This function orchestrates the execution of the ASA pipeline, VIRify, and MAP. It handles tasks such as generating
    samplesheets, managing pipeline states, and storing relevant metadata into the database for further processing.

    Additionally, it imports analysis download files for each pipeline, validating results, and generates
    the study summary files for the batch.

    :param assembly_analysis_batch_id: Unique identifier for the assembly analysis batch to process.
    :type assembly_analysis_batch_id: uuid.UUID
    :raises ClusterJobFailedException: If the cluster job fails during the ASA pipeline execution.
    :raises Exception: For any unexpected errors encountered during the pipeline execution.
    """
    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analysis_batch_id
    )

    # Store Prefect flow run ID, this will be persisted in the db later in the code
    assembly_analysis_batch.asa_flow_run_id = flow_run.id

    # Each batch is used to keep track of the pipelines executed.
    assembly_analysis_batch.set_pipeline_version(
        AssemblyAnalysisPipeline.ASA,
        settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
    )

    # Mark the batch as running (the analyses are marked as RUNNING too)
    assembly_analysis_batch.set_pipeline_status(
        AssemblyAnalysisPipeline.ASA,
        AssemblyAnalysisPipelineStatus.RUNNING,
    )
    assembly_analysis_batch.batch_analyses.update(
        asa_status=AssemblyAnalysisPipelineStatus.RUNNING
    )

    mgnify_study: Study = assembly_analysis_batch.study

    # Generate ASA samplesheet using the task
    samplesheet, _ = make_samplesheet_assembly(
        assembly_analysis_batch.study,
        assembly_analysis_batch.analyses.all(),
        output_dir=Path(assembly_analysis_batch.workspace_dir) / "samplesheets",
    )

    # Store samplesheet path
    assembly_analysis_batch.asa_samplesheet_path = str(samplesheet)
    assembly_analysis_batch.save()

    assembly_analyses_workspace_dir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.ASA
    )

    logger.info(
        f"Using {assembly_analyses_workspace_dir} as the batch nextflow outdir for ASA pipeline"
    )

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
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_config_file,
            ),
            (
                "-profile",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_nf_profile,
            ),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", assembly_analyses_workspace_dir),
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
            working_dir=assembly_analyses_workspace_dir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except Exception as e:
        error_type = (
            "ASA pipeline failed"
            if isinstance(e, ClusterJobFailedException)
            else "Unexpected error in ASA pipeline"
        )
        logger.error(f"{error_type} for study {mgnify_study.ena_study.accession}: {e}")
        assembly_analysis_batch.last_error = str(e)
        assembly_analysis_batch.set_pipeline_status(
            AssemblyAnalysisPipeline.ASA,
            AssemblyAnalysisPipelineStatus.FAILED,
        )
        assembly_analysis_batch.batch_analyses.update(
            asa_status=AssemblyAnalysisPipelineStatus.FAILED
        )
    else:
        assembly_analyses_ids = list(
            assembly_analysis_batch.analyses.values_list("id", flat=True)
        )

        # Set post-assembly analysis states based on pipeline output CSVs
        # Docs https://github.com/EBI-Metagenomics/assembly-analysis-pipeline/blob/dev/docs/output.md#per-study-output-files
        set_post_assembly_analysis_states(
            assembly_analyses_workspace_dir, assembly_analyses_ids
        )

        logger.info("Validating and importing completed ASA analyses...")
        try:
            assembly_analysis_batch_results_importer(
                assembly_analyses_batch_id=assembly_analysis_batch_id,
                pipeline_type=AssemblyAnalysisPipeline.ASA,
            )
            assembly_analysis_batch.set_pipeline_status(
                AssemblyAnalysisPipeline.ASA, AssemblyAnalysisPipelineStatus.COMPLETED
            )
        except PipelineValidationError as e:
            logger.error(f"Error validating ASA pipeline results: {e}")
            assembly_analysis_batch.last_error = str(e)
            assembly_analysis_batch.set_pipeline_status(
                AssemblyAnalysisPipeline.ASA, AssemblyAnalysisPipelineStatus.FAILED
            )
            assembly_analysis_batch.batch_analyses.update(
                asa_status=AssemblyAnalysisPipelineStatus.FAILED
            )
            # TODO: repeated
            to_update = []
            for analysis in assembly_analysis_batch.analyses.all():
                analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
                analysis.status[
                    f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
                ] = f"ASA results validation error. Exception: {e}"
                to_update.append(analysis)
            Analysis.objects.bulk_update(to_update, ["status"])
            # We stop the execution here as this is a problematic issue
            return

        ##################
        # === VIRify === #
        ##################
        logger.info("Starting VIRify pipeline")
        run_virify_batch(
            assembly_analyses_batch_id=assembly_analysis_batch.id,
        )
        # Refresh batch to get updates from run_virify_batch (samplesheet path, status, etc.)
        assembly_analysis_batch.refresh_from_db()
        logger.info("Validating and importing completed VIRify analyses.")
        # At least one analysis must be completed to mark the batch as completed
        completed_virify_analyses_count = assembly_analysis_batch.batch_analyses.filter(
            virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).count()
        if completed_virify_analyses_count:
            logger.info(
                f"Total: {completed_virify_analyses_count} VIRify analyses completed, marking this workflow step as completed."
            )
            try:
                assembly_analysis_batch_results_importer(
                    assembly_analyses_batch_id=assembly_analysis_batch_id,
                    pipeline_type=AssemblyAnalysisPipeline.VIRIFY,
                )
                assembly_analysis_batch.set_pipeline_status(
                    AssemblyAnalysisPipeline.VIRIFY,
                    AssemblyAnalysisPipelineStatus.COMPLETED,
                )
                assembly_analysis_batch.batch_analyses.update(
                    virify_status=AssemblyAnalysisPipelineStatus.COMPLETED
                )
            except PipelineValidationError as e:
                # TODO: maybe we should have a IMPORT ERROR?
                logger.error(f"Error validating VIRIfy pipeline results: {e}")
                assembly_analysis_batch.last_error = str(e)
                assembly_analysis_batch.set_pipeline_status(
                    AssemblyAnalysisPipeline.ASA, AssemblyAnalysisPipelineStatus.FAILED
                )
                assembly_analysis_batch.batch_analyses.update(
                    virify_status=AssemblyAnalysisPipelineStatus.FAILED
                )
                # TODO: repeated
                to_update = []
                for analysis in assembly_analysis_batch.analyses.all():
                    analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
                    analysis.status[
                        f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
                    ] = f"VIRIfy results validation error {e}"
                    to_update.append(analysis)
                Analysis.objects.bulk_update(
                    to_update,
                    [
                        "status",
                    ],
                )
                return
        else:
            logger.info("No VIRify analyses completed, marking batch as failed.")

        ###############
        # === MAP === #
        ###############
        logger.info("Starting MAP pipeline")
        run_map_batch(
            assembly_analyses_batch_id=assembly_analysis_batch.id,
        )
        # Refresh batch to get updates from run_map_batch (samplesheet path, status, etc.)
        assembly_analysis_batch.refresh_from_db()
        logger.info(
            "Validating and importing completed Mobilome Annotation Pipeline analyses."
        )
        # At least one analysis must be completed to mark the batch as completed
        completed_map_analyses_count = assembly_analysis_batch.batch_analyses.filter(
            map_status=AssemblyAnalysisPipelineStatus.COMPLETED
        ).count()
        if completed_map_analyses_count:
            logger.info(
                f"Total: {completed_map_analyses_count} MAP analyses completed, marking this workflow step as completed."
            )
            try:
                assembly_analysis_batch_results_importer(
                    assembly_analyses_batch_id=assembly_analysis_batch_id,
                    pipeline_type=AssemblyAnalysisPipeline.MAP,
                )
                assembly_analysis_batch.set_pipeline_status(
                    AssemblyAnalysisPipeline.MAP,
                    AssemblyAnalysisPipelineStatus.COMPLETED,
                )
                assembly_analysis_batch.batch_analyses.update(
                    map_status=AssemblyAnalysisPipelineStatus.COMPLETED
                )
            except PipelineValidationError as e:
                logger.error(f"Error validating MAP pipeline results: {e}")
                assembly_analysis_batch.last_error = str(e)
                assembly_analysis_batch.set_pipeline_status(
                    AssemblyAnalysisPipeline.ASA, AssemblyAnalysisPipelineStatus.FAILED
                )
                assembly_analysis_batch.batch_analyses.update(
                    map_status=AssemblyAnalysisPipelineStatus.FAILED
                )
                # TODO: repeated
                to_update = []
                for analysis in assembly_analysis_batch.analyses.all():
                    analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True
                    analysis.status[
                        f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"
                    ] = f"MAP results validation error {e}"
                    to_update.append(analysis)

                Analysis.objects.bulk_update(to_update, ["status"])
                return
        else:
            logger.info("No VIRify analyses completed, marking batch as failed.")

        # At this point, the batch is completed and all analyses are in the DB. #

        #######################################
        # === Study summary for the batch === #
        #######################################
        # The study summary is generated from the ASA results, VIRIfy and MAP results are layered on top of the GFF
        generate_assembly_analysis_pipeline_batch_summary(assembly_analysis_batch_id)
