import uuid
from pathlib import Path

from prefect import task, get_run_logger
from prefect.deployments import run_deployment

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study
from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    trailing_slash_ensured_dir,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import STUDY_SUMMARY_TSV
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipelineStatus,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipeline,
)
from workflows.prefect_utils.build_cli_command import cli_command


@task(
    name="Copy V6 Pipeline Results",
    task_run_name="Copy V6 Pipeline Results for {analysis_accession}",
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

    target_root = (
        EMG_CONFIG.slurm.private_results_dir
        if analysis.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )

    target = f"{target_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/{accession_prefix_separated_dir_path(analysis.assembly_or_run.first_accession, -3)}/{analysis.pipeline_version}/{experiment_type_label}"
    logger.info(
        f"Will copy results for {analysis_accession} from {analysis.results_dir} to {target}"
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
            "target": target,
        },
        job_variables={
            "partition": EMG_CONFIG.slurm.datamover_paritition,
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    analysis.external_results_dir = Path(target).relative_to(target_root)

    logger.info(
        f"Analysis {analysis} now has results at {analysis.external_results_dir} in {EMG_CONFIG.slurm.ftp_results_dir}"
    )

    analysis.save()


@task(name="Copy V6 Study Summaries")
def copy_v6_study_summaries(study_accession: str, timeout: int = 14400):
    """
    Copy study summaries from v6 pipeline results to external results dir

    :param study_accession: The accession of the study to copy summaries for
    :param timeout: Timeout in seconds for the move operation (default: 4 hours)
    """
    logger = get_run_logger()

    study = Study.objects.get(accession=study_accession)

    if not study.results_dir:
        logger.warning(f"Study {study} has no results dir, skipping")
        return

    command = cli_command(
        [
            "rsync",
            "-av",
            f"--include=PRJ*{STUDY_SUMMARY_TSV}",
            f"--include=[DES]RP*{STUDY_SUMMARY_TSV}",
            "--exclude=*",
        ]
    )
    source = trailing_slash_ensured_dir(study.results_dir)

    target_root = (
        EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )
    target = f"{target_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/study-summaries/"

    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "move_command": command,
            "source": source,
            "target": target,
            "make_target": True,
        },
        job_variables={
            "partition": EMG_CONFIG.slurm.datamover_paritition,
        },
        timeout=timeout,
    )
    logger.info(f"Mover flowrun is {flowrun}")

    study.external_results_dir = Path(target).parent.relative_to(target_root)
    study.save()

    logger.info(
        f"Study {study} now has results at {study.external_results_dir} in {target_root}"
    )


@task(
    name="Copy Assembly Batch Results",
    task_run_name="Copy Assembly Batch Results for batch {batch_id}",
)
def copy_assembly_batch_results(batch_id: uuid.UUID, timeout: int = 14400):
    """
    Copy results from all three assembly analysis pipelines (ASA, VIRify, MAP) for all
    analyses in a batch to external results directories.

    For each analysis in the batch, creates the following structure:
    - {target_root}/{study_path}/{assembly_accession}/ - ASA results at root
    - {target_root}/{study_path}/{assembly_accession}/virify/ - VIRify results
    - {target_root}/{study_path}/{assembly_accession}/mobilome-annotation/ - MAP results

    :param batch_id: The UUID of the AssemblyAnalysisBatch to copy results for
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    """
    logger = get_run_logger()

    batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
    study = batch.study

    # Determine target root based on privacy
    target_root = (
        EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )

    # Only copy results for analyses where ASA completed successfully
    asa_completed_relations = batch.batch_analyses.filter(
        asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
    ).select_related("analysis", "analysis__assembly")

    logger.info(
        f"Copying results for batch {batch_id} ({asa_completed_relations.count()} ASA-completed analyses "
        f"out of {batch.total_analyses} total) to {target_root}"
    )

    # Process each analysis that completed ASA
    copied_analysis_ids = []
    copy_failed_analysis_ids = []
    for batch_analysis_relation in asa_completed_relations:
        try:
            # TODO: This is calls another flow, which feels too nested. We should review this
            _copy_single_analysis_results(
                analysis=batch_analysis_relation.analysis,
                batch_analysis_relation=batch_analysis_relation,
                batch=batch,
                target_root=target_root,
                logger=logger,
                timeout=timeout,
            )
        except Exception as e:
            copy_failed_analysis_ids.append(batch_analysis_relation.analysis.id)
            logger.error(
                f"Failed to copy results for {batch_analysis_relation.analysis.accession}: {e}"
            )
            continue
        else:
            copied_analysis_ids.append(batch_analysis_relation.analysis.id)

    # Bulk update analysis statuses for all successfully copied analyses
    if copied_analysis_ids:
        analyses_to_update = Analysis.objects.filter(id__in=copied_analysis_ids)
        # Update status flags in bulk
        for analysis in analyses_to_update:
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = (
                True
            )
            # Unset failure/blocked statuses
            analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = False
            analysis.status[
                Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED
            ] = False
            analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] = False
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Bulk updated status for {len(copied_analysis_ids)} analyses")

    # TODO: this is repeated... I know.
    if copy_failed_analysis_ids:
        analyses_to_update = Analysis.objects.filter(id__in=copied_analysis_ids)
        # Update status flags in bulk
        for analysis in analyses_to_update:
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = (
                False
            )
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Bulk updated status for {len(copied_analysis_ids)} analyses")

    logger.info(f"Completed copying results for batch {batch_id}")


def _copy_single_analysis_results(
    analysis: Analysis,
    batch_analysis_relation: AssemblyAnalysisBatchAnalysis,
    batch: AssemblyAnalysisBatch,
    target_root: str,
    logger,
    timeout: int = 14400,
):
    """
    Copy results for a single analysis from the batch workspace to external results.

    :param analysis: The analysis to copy results for
    :param batch_analysis_relation: The batch-analysis relation containing pipeline statuses
    :param batch: The batch containing the analysis
    :param target_root: The root directory for external results
    :param logger: Prefect logger
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    """
    # Get assembly accession
    assembly_accession = analysis.assembly_or_run.first_accession
    study_accession = analysis.study.first_accession

    # Build target directory structure
    target_base = (
        Path(target_root)
        / accession_prefix_separated_dir_path(study_accession, -3)
        / accession_prefix_separated_dir_path(assembly_accession, -3)
        / analysis.pipeline_version
        / Analysis.ExperimentTypes.ASSEMBLY.label.lower()
    )

    logger.info(
        f"Copying results for {analysis.accession} (assembly: {assembly_accession})"
    )

    # rsync command for copying all files
    command = cli_command(
        [
            "rsync",
            "-av",
        ]
    )

    # Copy ASA results to root (ASA is always completed since we filter for it)
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    asa_source = asa_workspace / assembly_accession
    if asa_source.exists():
        logger.info(f"Copying ASA results: {asa_source} -> {target_base}")

        flowrun = run_deployment(
            name="move-data/move_data_deployment",
            parameters={
                "move_command": command,
                "source": trailing_slash_ensured_dir(str(asa_source)),
                "target": str(target_base),
                "make_target": True,
            },
            job_variables={
                "partition": EMG_CONFIG.slurm.datamover_paritition,
            },
            timeout=timeout,
        )
        logger.info(f"ASA mover flowrun is {flowrun}")

    else:
        logger.warning(
            f"ASA results not found for {assembly_accession} at {asa_source}"
        )

    # Copy VIRify results to virify/ (only if VIRify completed)
    virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
    virify_source = virify_workspace / assembly_accession
    virify_target = target_base / "virify"
    if (
        batch_analysis_relation.virify_status
        == AssemblyAnalysisPipelineStatus.COMPLETED
        and virify_source.exists()
    ):
        logger.info(f"Copying VIRify results: {virify_source} -> {virify_target}")
        flowrun = run_deployment(
            name="move-data/move_data_deployment",
            parameters={
                "move_command": command,
                "source": trailing_slash_ensured_dir(str(virify_source)),
                "target": str(virify_target),
                "make_target": True,
            },
            job_variables={
                "partition": EMG_CONFIG.slurm.datamover_paritition,
            },
            timeout=timeout,
        )
        logger.info(f"VIRify mover flowrun is {flowrun}")
    else:
        logger.info(
            f"VIRify not completed or results not found for {assembly_accession} at {virify_source}"
        )

    # Copy MAP results to mobilome-annotation/ (only if MAP completed)
    map_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)
    map_source = map_workspace / assembly_accession
    map_target = target_base / "mobilome-annotation"
    if (
        batch_analysis_relation.map_status == AssemblyAnalysisPipelineStatus.COMPLETED
        and map_source.exists()
    ):
        logger.info(f"Copying MAP results: {map_source} -> {map_target}")
        flowrun = run_deployment(
            name="move-data/move_data_deployment",
            parameters={
                "move_command": command,
                "source": trailing_slash_ensured_dir(str(map_source)),
                "target": str(map_target),
                "make_target": True,
            },
            job_variables={
                "partition": EMG_CONFIG.slurm.datamover_paritition,
            },
            timeout=timeout,
        )
        logger.info(f"MAP mover flowrun is {flowrun}")
    else:
        logger.info(
            f"MAP not completed or results not found for {assembly_accession} at {map_source}"
        )

    # Update analysis and study external_results_dir
    analysis.external_results_dir = target_base.relative_to(target_root)
    analysis.save()
    analysis.study.external_results_dir = target_base.parent.relative_to(target_root)
    analysis.study.save()

    logger.info(
        f"Analysis {analysis.accession} now has results at {analysis.external_results_dir} in {target_root}"
    )
