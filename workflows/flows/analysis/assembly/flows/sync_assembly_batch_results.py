import uuid
from pathlib import Path

from pydantic import BaseModel

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    copy_assembly_batch_results_to_destination_folder,
)
from workflows.models import (
    AssemblyAnalysisBatch,
)
from workflows.prefect_utils.analyses_models_helpers import mark_analysis_status
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

    # Batch analysis results live under:
    #   {root}/{ena_study_prefix}/{ena_assembly_prefix}/{pipeline_version}/assembly
    # For example:
    #   PRJEB105/PRJEB105754/ERZ28775/ERZ28775516/V6/assembly
    # The study-level directory is the shared ENA study prefix parent for all analyses
    # in this batch.
    study_prefix = accession_prefix_separated_dir_path(mgnify_study.first_accession, -3)
    nfs_results_root = Path(assembly_analysis_batch.workspace_dir) / "results"

    # Copy the files to the FTP server (external_results)
    external_results_root = (
        EMG_CONFIG.slurm.private_results_dir
        if mgnify_study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )
    # Store the study roots up front. Individual Analysis rows will later point deeper
    # into the ENA-derived tree at
    # {ena_study_prefix}/{ena_assembly_prefix}/{pipeline_version}/assembly.
    mgnify_study.external_results_dir = str(study_prefix)
    mgnify_study.save(update_fields=["external_results_dir"])

    external_copy_results = list(
        copy_assembly_batch_results_to_destination_folder(
            assembly_analyses_batch_id,
            destination_root=external_results_root,
        )
    )
    update_external_results_dirs_from_copy_results(
        external_copy_results,
        study_prefix=study_prefix,
    )
    update_analysis_statuses_from_copy_results(external_copy_results)

    # Copy the files to the NFS production
    nfs_copy_results = list(
        copy_assembly_batch_results_to_destination_folder(
            assembly_analyses_batch_id,
            destination_root=nfs_results_root,
        )
    )
    update_results_dirs_from_copy_results(
        nfs_copy_results,
        study_results_dir=nfs_results_root / study_prefix,
    )

    return AssemblyBatchSyncResult(
        external_copy_results=external_copy_results,
        nfs_copy_results=nfs_copy_results,
    )


def update_analysis_statuses_from_copy_results(
    copy_results: list[BatchCopyResult],
) -> None:
    """Update analysis import status flags from external copy results."""
    successful_results = [result for result in copy_results if result.success]
    if not successful_results:
        return

    analyses = Analysis.objects.filter(
        id__in=[result.analysis_id for result in successful_results]
    )

    for analysis in analyses:
        mark_analysis_status(
            analysis,
            status=Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
            unset_statuses=[
                Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
                Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
                Analysis.AnalysisStates.ANALYSIS_BLOCKED,
            ],
        )


def update_external_results_dirs_from_copy_results(
    copy_results: list[BatchCopyResult],
    study_prefix: Path,
) -> None:
    """Set external_results_dir for analyses successfully copied to external storage."""
    successful_results = [result for result in copy_results if result.success]
    if not successful_results:
        return

    analyses = {
        analysis.id: analysis
        for analysis in Analysis.objects.select_related("study", "assembly").filter(
            id__in=[result.analysis_id for result in successful_results]
        )
    }

    for result in successful_results:
        analysis = analyses[result.analysis_id]
        analysis.external_results_dir = str(
            study_prefix
            / accession_prefix_separated_dir_path(
                analysis.assembly_or_run.first_accession, -3
            )
            / analysis.pipeline_version
            / Analysis.ExperimentTypes.ASSEMBLY.label.lower()
        )
        analysis.save()


def update_results_dirs_from_copy_results(
    copy_results: list[BatchCopyResult],
    study_results_dir: Path,
) -> None:
    """Set results_dir for analyses successfully copied to the NFS mirror."""
    successful_results = [result for result in copy_results if result.success]
    if not successful_results:
        return

    analyses = {
        analysis.id: analysis
        for analysis in Analysis.objects.select_related("assembly").filter(
            id__in=[result.analysis_id for result in successful_results]
        )
    }
    for result in successful_results:
        analysis = analyses[result.analysis_id]
        analysis.results_dir = str(
            study_results_dir
            / accession_prefix_separated_dir_path(
                analysis.assembly_or_run.first_accession, -3
            )
            / analysis.pipeline_version
            / Analysis.ExperimentTypes.ASSEMBLY.label.lower()
        )
        analysis.save()
