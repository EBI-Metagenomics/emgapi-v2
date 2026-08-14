from pathlib import Path
from typing import List

from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    copy_single_out_of_production_analysis_results,
)
from workflows.flows.analysis.assembly.flows.sync_assembly_batch_results import (
    update_analysis_statuses_from_copy_results,
    update_external_results_dirs_from_copy_results,
    update_results_dirs_from_copy_results,
)
from workflows.prefect_utils.flows_utils import django_db_task as task


@task
def copy_out_of_production_assembly_analysis_results(
    study_id: int, results_dir: Path, analysis_ids: List[int], timeout: int = 14400
):
    """
    Copy out-of-production assembly results to external storage and NFS mirror.

    FIXME: this function is very similar to copy_assembly_batch_results,
    needs refactoring (ochkalova).

    :param study_id: The id of the study to copy results for
    :param results_dir: The base path where the results are located
    :param analysis_ids: IDs of the analyses objects to copy results for
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    """
    study = Study.objects.get(id=study_id)

    # Analyses that failed validation/import earlier in the flow are marked
    # ANALYSIS_QC_FAILED in the DB. Re-query rather than trust the in-memory
    # objects, since they may have been fetched before that status was set.
    analysis_ids_ready_for_copy = list(
        Analysis.objects.filter(id__in=analysis_ids)
        .exclude_by_statuses([Analysis.AnalysisStates.ANALYSIS_QC_FAILED])
        .values_list("id", flat=True)
    )

    study_prefix = accession_prefix_separated_dir_path(study.first_accession, -3)
    nfs_results_root = Path(results_dir) / "results"

    external_results_root = (
        EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )

    external_copy_results = list(
        copy_out_of_production_analysis_results_to_destination_folder(
            analysis_ids=analysis_ids_ready_for_copy,
            results_workspace=Path(results_dir),
            destination_root=external_results_root,
            timeout=timeout,
        )
    )
    update_external_results_dirs_from_copy_results(
        external_copy_results,
        study_prefix=study_prefix,
    )
    update_analysis_statuses_from_copy_results(external_copy_results)

    # Copy the files to the NFS production
    nfs_copy_results = list(
        copy_out_of_production_analysis_results_to_destination_folder(
            analysis_ids=analysis_ids_ready_for_copy,
            results_workspace=Path(results_dir),
            destination_root=nfs_results_root,
            timeout=timeout,
        )
    )
    update_results_dirs_from_copy_results(
        nfs_copy_results,
        study_results_dir=nfs_results_root / study_prefix,
    )

    study.external_results_dir = str(study_prefix)
    study.save(update_fields=["external_results_dir"])


@task(
    description="Copy Out-of-Production Assembly Results",
    task_run_name="Copy out-of-production assembly results to {destination_root}",
)
def copy_out_of_production_analysis_results_to_destination_folder(
    analysis_ids: list[int],
    results_workspace: Path,
    destination_root: str | Path,
    timeout: int = 14400,
) -> list[BatchCopyResult]:
    """
    The source workspace is expected to have three pipeline subdirectories:
    ``asa/``, ``virify/``, and ``map/``, each with per-assembly result folders.

    :param analysis_ids: IDs of the analyses to copy results for
    :param results_workspace: Source workspace with ``asa/``, ``virify/`, and ``map/`` subdirectories
    :param destination_root: The root directory to copy results into
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    :return: One copy result per ASA-completed analysis in the batch
    """
    logger = get_run_logger()

    destination_root = Path(destination_root)

    copy_results: list[BatchCopyResult] = []
    for analysis_id in analysis_ids:
        copy_results.append(
            copy_single_out_of_production_analysis_results(
                analysis_id=analysis_id,
                results_workspace=results_workspace,
                destination_root=destination_root,
                timeout=timeout,
            )
        )

    logger.info(
        f"Completed copying out-of-production results for {len(copy_results)} analyses to {destination_root}"
    )

    return copy_results
