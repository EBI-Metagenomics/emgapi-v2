import uuid
from pathlib import Path

from prefect import get_run_logger
from prefect.deployments import run_deployment
from pydantic import BaseModel, Field

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study
from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    trailing_slash_ensured_dir,
)
from workflows.data_io_utils.schemas.assembly import AssemblyResultSchema
from workflows.data_io_utils.schemas.base import (
    PipelineDirectorySchema,
    PipelineResultSchema,
)
from workflows.data_io_utils.schemas.map import MapResultSchema
from workflows.data_io_utils.schemas.virify import VirifyResultSchema
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    DWCREADY_CSV,
    PIPELINE_CONFIGS,
    STUDY_SUMMARY_TSV,
)
from workflows.flows.analysis import AnalysisType
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.flows_utils import django_db_task as task


class CopyError(BaseModel):
    """Error from copying one pipeline result directory."""

    pipeline_name: str
    source: Path
    message: str


class BatchCopyResult(BaseModel):
    """Result for copying one analysis from an assembly batch."""

    analysis_id: int
    destination_folder: Path
    success: bool
    errors: list[CopyError] = Field(default_factory=list)


@task(
    name="Copy Pipeline Results",
    task_run_name="Copy Pipeline Results for {analysis_accession}",
)
def copy_v6_pipeline_results(analysis_accession: str, timeout: int = 14400):
    """
    Copies pipeline result files for a given analysis to a designated directory based
    on the privacy level of the analysis and study. The function ensures the correct
    destination is identified, filters files by specified extensions, and synchronizes
    data securely.

    :param analysis_accession: The accession of the analysis to copy results for.
    :param timeout: Timeout in seconds for the move operation (default: 4 hours)
    """
    # TODO: should we reconsider this as run in bulk for several analyses? (or a batch).
    logger = get_run_logger()

    analysis = Analysis.objects.get(accession=analysis_accession)
    study = analysis.study

    source = trailing_slash_ensured_dir(analysis.results_dir)

    experiment_type_label = Analysis.ExperimentTypes(
        analysis.experiment_type
    ).label.lower()

    assert (
        analysis.is_private == study.is_private == analysis.assembly_or_run.is_private
    )  # Shouldn't ever be untrue, just a helper for the future

    destination_root = (
        EMG_CONFIG.slurm.private_results_dir
        if analysis.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )

    destination = f"{destination_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/{accession_prefix_separated_dir_path(analysis.assembly_or_run.first_accession, -3)}/{analysis.pipeline_version}/{experiment_type_label}"
    logger.info(
        f"Will copy results for {analysis_accession} from {analysis.results_dir} to {destination}"
    )

    allowed_extensions = [
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "json",
        "gz",
        "fasta",
        "csv",
        "deoverlapped",
    ]

    command = cli_command(
        [
            "rsync",
            "-av",
            "--prune-empty-dirs",
            "--include=*/",
        ]
        + [f"--include=*.{ext}" for ext in allowed_extensions]
        + ["--exclude=*"]
    )

    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "move_command": command,
            "source": source,
            "target": destination,
        },
        job_variables={
            "partition": EMG_CONFIG.slurm.datamover_partition,
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    analysis.external_results_dir = Path(destination).relative_to(destination_root)

    logger.info(
        f"Analysis {analysis} now has results at {analysis.external_results_dir} in {EMG_CONFIG.slurm.ftp_results_dir}"
    )

    analysis.save()


@task(name="Copy Study Summaries")
def copy_v6_study_summaries(
    study_accession: str,
    analysis_type: AnalysisType = AnalysisType.AMPLICON,
    timeout: int = 14400,
):
    """
    Copy study summaries from a pipeline's results subdirectory to external results dir.

    The source directory is computed from ``study.results_dir`` and the pipeline config
    for the given ``analysis_type``, e.g. ``{results_dir}/amplicon_v6.1/``.
    This matches the directory layout produced by :func:`merge_study_summaries`.

    :param study_accession: The accession of the study to copy summaries for.
    :param analysis_type: Pipeline type whose summaries should be copied.
    :param timeout: Timeout in seconds for the move operation (default: 4 hours).
    """
    logger = get_run_logger()

    study = Study.objects.get(accession=study_accession)

    if not study.results_dir:
        logger.warning(f"Study {study} has no results dir, skipping")
        return

    pipeline_config = PIPELINE_CONFIGS[analysis_type]
    pipeline_subdir = (
        f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
    )
    source_dir = study.results_dir_path / pipeline_subdir

    command = cli_command(
        [
            "rsync",
            "-av",
            f"--include=PRJ*{STUDY_SUMMARY_TSV}",
            f"--include=[DES]RP*{STUDY_SUMMARY_TSV}",
            f"--include=PRJ*{DWCREADY_CSV}",
            f"--include=[DES]RP*{DWCREADY_CSV}",
            "--exclude=*",
        ]
    )
    source = trailing_slash_ensured_dir(str(source_dir))

    destination_root = (
        EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )
    destination = f"{destination_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/study-summaries/"

    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "move_command": command,
            "source": source,
            "target": destination,
            "make_target": True,
        },
        job_variables={
            "partition": EMG_CONFIG.slurm.datamover_partition,
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    study.external_results_dir = Path(destination).parent.relative_to(destination_root)
    study.save()

    logger.info(
        f"Study {study} now has results at {study.external_results_dir} in {destination_root}"
    )


@task(
    description="Copy an Assembly Batch Results",
    task_run_name="Copy Assembly Batch Results for batch {batch_id} to {destination_root}",
)
def copy_assembly_batch_results_to_destination_folder(
    batch_id: uuid.UUID,
    destination_root: str | Path,
    timeout: int = 14400,
) -> list[BatchCopyResult]:
    """
    Copy results from all three assembly analysis pipelines (ASA, VIRify, MAP) for all
    analyses in a batch to a destination results directory.

    For each analysis in the batch, creates the following structure:
    - {destination_root}/{study_path}/{assembly_accession}/ - ASA results at root
    - {destination_root}/{study_path}/{assembly_accession}/virify/ - VIRify results
    - {destination_root}/{study_path}/{assembly_accession}/mobilome-annotation/ - MAP results

    :param batch_id: The UUID of the AssemblyAnalysisBatch to copy results for
    :param destination_root: The root directory to copy results into
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    :return: One copy result per ASA-completed analysis in the batch
    """
    logger = get_run_logger()

    batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
    destination_root = Path(destination_root)

    # Only copy results for analyses where ASA completed successfully
    asa_completed_batch_jobs = batch.batch_analyses.filter(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
    ).select_related("analysis", "analysis__assembly")

    logger.info(
        f"Copying results for batch {batch_id} ({asa_completed_batch_jobs.count()} ASA-completed analyses "
        f"out of {batch.total_analyses} total) to {destination_root}"
    )

    copy_results: list[BatchCopyResult] = []
    for batch_analysis_job in asa_completed_batch_jobs:
        copy_results.append(
            copy_single_analysis_results(
                analysis=batch_analysis_job.analysis,
                batch_analysis_job=batch_analysis_job,
                batch=batch,
                destination_root=destination_root,
                timeout=timeout,
            )
        )

    logger.info(f"Completed copying results for batch {batch_id} to {destination_root}")

    return copy_results


@task
def copy_single_analysis_results(
    analysis: Analysis,
    batch_analysis_job,
    batch: AssemblyAnalysisBatch,
    destination_root: Path,
    timeout: int = 14400,
) -> BatchCopyResult:
    """
    Copy results for a single analysis from the batch workspace to external results.

    :param analysis: The analysis to copy results for
    :param batch_analysis_job: The batch relation containing per-pipeline statuses
    :param batch: The batch containing the analysis
    :param destination_root: The root directory for copied results
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    :return: Copy result for this analysis
    """
    logger = get_run_logger()

    # The results folder looks like ERPxxxx/ERZyyy/ERZyyyyy

    assembly_accession = analysis.assembly_or_run.first_accession
    study_accession = analysis.study.first_accession

    destination_base = (
        destination_root
        / accession_prefix_separated_dir_path(study_accession, -3)
        / accession_prefix_separated_dir_path(assembly_accession, -3)
        / analysis.pipeline_version
        / Analysis.ExperimentTypes.ASSEMBLY.label.lower()
    )

    logger.info(
        f"Copying results for {analysis.accession} (assembly: {assembly_accession})"
    )

    copy_errors: list[CopyError] = []

    asa_copy_success, asa_copy_errors = copy_schema_directories(
        schema=AssemblyResultSchema(),
        source_base=batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
        / assembly_accession,
        destination_base=destination_base,
        timeout=timeout,
    )
    copy_errors.extend(asa_copy_errors)

    # VIRIfy and MAP results are "optional", these pipelines may not render any results

    # Optional pipelines are controlled by the workflow state first. A directory merely
    # existing on disk is not enough because stale or partial outputs may be
    # present for non-completed runs.
    if batch_analysis_job.virify_status == AssemblyAnalysisPipelineStatus.COMPLETED:
        virify_source_base = (
            batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
            / assembly_accession
        )
        if virify_source_base.exists():
            _virify_copy_success, virify_copy_errors = copy_schema_directories(
                schema=VirifyResultSchema(),
                source_base=virify_source_base,
                destination_base=destination_base,
                timeout=timeout,
            )
            copy_errors.extend(virify_copy_errors)
        else:
            logger.info(
                f"No VIRify output found for {analysis.accession} at {virify_source_base}, skipping optional copy"
            )

    # MAP follows the same rule: only publish outputs that the batch relation
    # recorded as completed, then treat a missing optional directory as a no-op.
    if batch_analysis_job.map_status == AssemblyAnalysisPipelineStatus.COMPLETED:
        map_source_base = (
            batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)
            / assembly_accession
        )
        if map_source_base.exists():
            _map_copy_success, map_copy_errors = copy_schema_directories(
                schema=MapResultSchema(),
                source_base=map_source_base,
                destination_base=destination_base,
                timeout=timeout,
            )
            copy_errors.extend(map_copy_errors)
        else:
            logger.info(
                f"No MAP output found for {analysis.accession} at {map_source_base}, skipping optional copy"
            )

    if copy_errors:
        error_summary = "; ".join(error.message for error in copy_errors)
        logger.error(
            f"Failed to copy results for {analysis.accession}: {error_summary}"
        )
        return BatchCopyResult(
            analysis_id=analysis.id,
            destination_folder=destination_base,
            success=False,
            errors=copy_errors,
        )

    logger.info(f"Analysis {analysis.accession} results copied to {destination_base}")
    return BatchCopyResult(
        analysis_id=analysis.id,
        destination_folder=destination_base,
        success=True,
    )


@task
def copy_schema_directories(
    schema: PipelineResultSchema,
    source_base: Path,
    destination_base: Path,
    timeout: int = 14400,
) -> tuple[bool, list[CopyError]]:
    """Copy all top-level directories defined by a pipeline result schema.

    It returns a tuple, the first element is a boolean indicating success, and the
    second element is a list of CopyError objects.

    :return: A tuple containing a boolean indicating success and a list of CopyError objects.
    """

    logger = get_run_logger()
    errors: list[CopyError] = []
    logger.info(
        f"Copying {len(schema.directories)} top-level directories for {schema.pipeline_name} "
        f"from {source_base} to {destination_base}"
    )

    for directory_schema in schema.directories:

        copy_success, copy_directory_errors = copy_schema_directory(
            directory_schema=directory_schema,
            source_base=source_base,
            destination_base=destination_base,
            timeout=timeout,
        )
        errors.extend(copy_directory_errors)

    copy_success = len(errors) == 0
    if copy_success:
        logger.info(f"Copied {schema.pipeline_name} schema directories")
    else:
        logger.error(
            f"Failed to copy {len(errors)} {schema.pipeline_name} schema directories"
        )

    return copy_success, errors


@task
def copy_schema_directory(
    directory_schema: PipelineDirectorySchema,
    source_base: Path,
    destination_base: Path,
    timeout: int = 14400,
) -> tuple[bool, list[CopyError]]:
    """
    Copy result directories according to the schema's external folder mapping.

    ``external_folder_name`` is the copy boundary. When it is set, this function
    copies that schema directory as one tree and stops there; child schemas are
    intentionally ignored because validation has already checked the expected
    nested files and directories. When it is not set, the schema node is treated
    as an internal grouping/container and only its children can become copy
    destinations.

    :param directory_schema: Schema node to inspect for a copy destination.
    :param source_base: Directory containing ``directory_schema.folder_name``.
    :param destination_base: Root directory for copied results.
    :param timeout: Timeout in seconds for each move-data deployment.
    :return: ``(success, errors)`` for this schema branch.
    """

    logger = get_run_logger()
    source = source_base / directory_schema.folder_name
    if not directory_schema.external_folder_name:
        # Unmapped schema nodes are structure only. Walk down until a mapped
        # child is found, but do not copy the container itself.
        logger.info(
            f"Schema directory {directory_schema.folder_name} has no external folder mapping; "
            f"checking {len(directory_schema.subdirectories)} child directories"
        )
        errors: list[CopyError] = []
        success = False
        for subdirectory_schema in directory_schema.subdirectories:
            subdirectory_success, subdirectory_errors = copy_schema_directory(
                directory_schema=subdirectory_schema,
                source_base=source,
                destination_base=destination_base,
                timeout=timeout,
            )
            errors.extend(subdirectory_errors)
            success = success or subdirectory_success

        if success:
            logger.info(
                f"Finished child copy checks for unmapped schema directory {directory_schema.folder_name}"
            )
        else:
            logger.error(
                f"Failed child copy checks for unmapped schema directory {directory_schema.folder_name}: "
                f"{len(errors)} errors"
            )
        return success, errors

    # A mapped schema node owns the destination path, so copy it as a single
    # directory tree and stop instead of spawning rsyncs for nested schemas.
    destination = destination_base / directory_schema.external_folder_name
    logger.info(
        f"Schema directory {directory_schema.folder_name} maps to {directory_schema.external_folder_name}; "
        f"copying {source} to {destination}"
    )
    success, errors = copy_pipeline_results(
        pipeline_name=directory_schema.folder_name,
        source=source,
        destination=destination,
        timeout=timeout,
    )

    return success, errors


@task
def copy_pipeline_results(
    pipeline_name: str,
    source: Path,
    destination: Path,
    timeout: int = 14400,
) -> tuple[bool, list[CopyError]]:
    """Copy one pipeline result directory if present."""
    logger = get_run_logger()

    allowed_extensions = [
        "yml",
        "yaml",
        "txt",
        "html",
        "json",
        "fa",
        "fasta",
        "fai",
        "tsv",
        "csv",
        "gz",
        "csi",
        "gzi",
    ]
    command = cli_command(
        [
            "rsync",
            "-av",
            "--prune-empty-dirs",
            "--include=*/",
        ]
        + [f"--include=*.{ext}" for ext in allowed_extensions]
        + ["--exclude=*"]
    )

    logger.info(f"Copying {pipeline_name} results: {source} -> {destination}")
    try:
        flowrun = run_deployment(
            name="move-data/move_data_deployment",
            parameters={
                "move_command": command,
                "source": trailing_slash_ensured_dir(str(source)),
                "target": str(destination),
                "make_target": True,
            },
            job_variables={
                "partition": EMG_CONFIG.slurm.datamover_partition,
            },
            timeout=timeout,
        )
    except Exception as exc:
        message = f"{pipeline_name} copy failed: {exc}"
        logger.error(message)
        return False, [
            CopyError(
                pipeline_name=pipeline_name,
                source=source,
                message=message,
            )
        ]

    logger.info(f"{pipeline_name} mover flowrun is {flowrun}")

    return True, []
