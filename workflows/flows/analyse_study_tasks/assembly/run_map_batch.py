import uuid
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run

import analyses.models
from activate_django_first import EMG_CONFIG
from analyses.models import PipelineState
from workflows.data_io_utils.schemas.map import MapResultSchema
from workflows.data_io_utils.schemas.validation import sanity_check_pipeline_results
from workflows.flows.analyse_study_tasks.assembly.make_samplesheet_assembly import (
    make_samplesheet_for_map,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task
def sanity_check_map_results(
    analysis: analyses.models.Analysis,
    map_outdir: Path,
):
    """
    Validate MAP pipeline results using the unified schema.

    :param analysis: The analysis to validate results for
    :param map_outdir: The output directory of the MAP pipeline
    :return: Validated Directory object
    """
    schema = MapResultSchema()
    return sanity_check_pipeline_results(
        schema, map_outdir, analysis.assembly.first_accession, "MAP"
    )


@task
def add_map_gff_to_analysis_downloads(
    analysis: analyses.models.Analysis,
    map_outdir: Path,
):
    """
    Add the MAP (Mobilome Annotation Pipeline) GFF file to the analysis downloads using unified schema.

    :param analysis: The analysis to add the download to
    :param map_outdir: The output directory of the MAP pipeline
    """
    logger = get_run_logger()

    try:
        # Validate results first
        sanity_check_map_results(analysis, map_outdir)

        # Use schema to generate downloads
        schema = MapResultSchema()
        downloads = schema.generate_downloads(analysis, map_outdir)

        # Add downloads to analysis
        for download in downloads:
            try:
                analysis.add_download(download)
                logger.info(
                    f"Added MAP file {download.alias} to analysis {analysis.accession} downloads"
                )
            except FileExistsError:
                logger.warning(
                    f"Download with alias {download.alias} already exists for analysis {analysis.accession}"
                )
    except Exception as e:
        logger.error(f"Failed to add MAP downloads for {analysis.accession}: {e}")
        raise


@flow(name="Run MAP pipeline batch")
def run_map_batch(
    assembly_batch_id: uuid.UUID,
):
    """
    Run the MAP (Mobilome Annotation Pipeline) for a batch of assemblies.
    Updates business state at milestones - Prefect tracks execution details.

    :param assembly_batch_id: The AssemblyAnalysisBatch to process
    """
    logger = get_run_logger()

    assembly_batch = analyses.models.AssemblyAnalysisBatch.objects.get(
        id=assembly_batch_id
    )

    # Store Prefect flow run ID
    assembly_batch.map_flow_run_id = flow_run.id
    assembly_batch.save()

    # Record pipeline version
    # TODO: Update when MAP pipeline config is available in EMG_CONFIG
    assembly_batch.set_pipeline_version(
        analyses.models.AssemblyAnalysisBatch.PipelineType.MAP,
        EMG_CONFIG.virify_pipeline.pipeline_git_revision,  # Placeholder - replace with MAP config
    )

    # Mark business state as in progress
    assembly_batch.set_pipeline_state(
        analyses.models.AssemblyAnalysisBatch.PipelineType.MAP,
        PipelineState.IN_PROGRESS,
    )

    # Get analyses from batch
    assembly_analyses = list(assembly_batch.assembly_analysis_set.all())
    mgnify_study = assembly_batch.study

    # Create samplesheets directory in batch workspace
    samplesheets_dir = Path(assembly_batch.results_dir) / "samplesheets"
    samplesheets_dir.mkdir(parents=True, exist_ok=True)

    # Create the MAP samplesheet using the make_samplesheet_for_map function
    # The function uses the filesystem to find CDS and VIRify GFF files
    map_samplesheet_path, _ = make_samplesheet_for_map(
        assembly_batch, output_dir=samplesheets_dir
    )

    # Check if the MAP samplesheet exists
    if not map_samplesheet_path.exists():
        logger.warning(
            f"MAP samplesheet {map_samplesheet_path} does not exist. Skipping MAP pipeline."
        )
        assembly_batch.last_error = (
            f"MAP samplesheet not found at {map_samplesheet_path}"
        )
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.MAP, PipelineState.FAILED
        )
        return

    # Store samplesheet path
    assembly_batch.map_samplesheet_path = str(map_samplesheet_path)
    assembly_batch.save()

    map_outdir = assembly_batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.MAP.value
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
    except ClusterJobFailedException as cluster_exception:
        logger.error(
            f"MAP pipeline failed for study {mgnify_study.ena_study.accession}: {cluster_exception}"
        )
        # Mark batch as failed (MAP failure doesn't propagate to analyses)
        assembly_batch.last_error = str(cluster_exception)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.MAP, PipelineState.FAILED
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error in MAP pipeline: {e}")
        # Mark batch as failed (MAP failure doesn't propagate to analyses)
        assembly_batch.last_error = str(e)
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.MAP, PipelineState.FAILED
        )
        raise
    else:
        # Add MAP GFF files to each analysis's downloads
        for analysis in assembly_analyses:
            add_map_gff_to_analysis_downloads(analysis, map_outdir)

        # Mark MAP pipeline as ready
        assembly_batch.set_pipeline_state(
            analyses.models.AssemblyAnalysisBatch.PipelineType.MAP, PipelineState.READY
        )
        logger.info("MAP pipeline completed successfully and marked as READY")
