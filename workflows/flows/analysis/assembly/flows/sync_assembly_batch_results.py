import uuid
from pathlib import Path

from pydantic import BaseModel

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    copy_assembly_batch_results_to_destination_folder,
)
from workflows.models import (
    AssemblyAnalysisBatch,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow


class AssemblyBatchSyncResult(BaseModel):
    """Result from syncing one assembly analysis batch."""

    external_copy_results: list[BatchCopyResult]
    nfs_copy_results: list[BatchCopyResult]


@flow(
    description="Sync Assembly Batch Results: {assembly_analyses_batch_id}",
)
def copy_assembly_batch_results(
    assembly_analyses_batch_id: uuid.UUID,
) -> AssemblyBatchSyncResult:
    """Copy the results from the batch to the external storage and NFS mirror.

    After copying the files it updates the external_results_dir and results_dir for the analyses.

    :param assembly_analyses_batch_id: The ID of the assembly analysis batch to sync.
    """
    assembly_analysis_batch = AssemblyAnalysisBatch.objects.select_related("study").get(
        id=assembly_analyses_batch_id
    )
    mgnify_study = assembly_analysis_batch.study

    # Copy the files to the FTP server (external_results)
    external_results_root = (
        EMG_CONFIG.slurm.private_results_dir
        if mgnify_study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )
    external_copy_results = list(
        copy_assembly_batch_results_to_destination_folder(
            assembly_analyses_batch_id,
            destination_root=external_results_root,
        )
    )
    update_external_results_dirs_from_copy_results(
        external_copy_results,
        destination_root=Path(external_results_root),
    )
    update_analysis_statuses_from_copy_results(external_copy_results)

    # Copy the files to the NFS production
    nfs_copy_results = list(
        copy_assembly_batch_results_to_destination_folder(
            assembly_analyses_batch_id,
            destination_root=Path(assembly_analysis_batch.workspace_dir) / "results",
        )
    )
    update_results_dirs_from_copy_results(nfs_copy_results)

    return AssemblyBatchSyncResult(
        external_copy_results=external_copy_results,
        nfs_copy_results=nfs_copy_results,
    )


def update_analysis_statuses_from_copy_results(
    copy_results: list[BatchCopyResult],
) -> None:
    """Update analysis import status flags from external copy results."""
    if not copy_results:
        return

    analyses = {
        analysis.id: analysis
        for analysis in Analysis.objects.filter(
            id__in=[result.analysis_id for result in copy_results]
        )
    }

    for result in copy_results:
        analysis = analyses[result.analysis_id]
        analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = (
            result.success
        )
        if result.success:
            analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = False
            analysis.status[
                Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED
            ] = False
            analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] = False

    Analysis.objects.bulk_update(analyses.values(), ["status"])


def update_external_results_dirs_from_copy_results(
    copy_results: list[BatchCopyResult],
    destination_root: Path,
) -> None:
    """Set external_results_dir for analyses successfully copied to external storage."""
    successful_results = [result for result in copy_results if result.success]
    if not successful_results:
        return

    analyses = {
        analysis.id: analysis
        for analysis in Analysis.objects.select_related("study").filter(
            id__in=[result.analysis_id for result in successful_results]
        )
    }
    studies_to_update: dict[int, Study] = {}

    for result in successful_results:
        analysis = analyses[result.analysis_id]
        destination = Path(result.destination_folder)
        analysis.external_results_dir = destination.relative_to(destination_root)
        analysis.study.external_results_dir = destination.parent.relative_to(
            destination_root
        )
        studies_to_update[analysis.study_id] = analysis.study

    Analysis.objects.bulk_update(analyses.values(), ["external_results_dir"])
    Study.objects.bulk_update(studies_to_update.values(), ["external_results_dir"])


def update_results_dirs_from_copy_results(
    copy_results: list[BatchCopyResult],
) -> None:
    """Set results_dir for analyses successfully copied to the NFS mirror."""
    successful_results = [result for result in copy_results if result.success]
    if not successful_results:
        return

    analyses = {
        analysis.id: analysis
        for analysis in Analysis.objects.filter(
            id__in=[result.analysis_id for result in successful_results]
        )
    }

    for result in successful_results:
        analyses[result.analysis_id].results_dir = str(result.destination_folder)

    Analysis.objects.bulk_update(analyses.values(), ["results_dir"])
