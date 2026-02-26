import uuid
from prefect import get_run_logger, task

from activate_django_first import EMG_CONFIG  # noqa

from analyses.models import Analysis
from workflows.data_io_utils.schemas.assembly import ImportResult

from typing import List

from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


def mark_analyses_with_failed_status(
    import_results: List[ImportResult],
    pipeline_type: AssemblyAnalysisPipeline,
    validation_only: bool = False,
) -> None:
    """
    Mark analyses as failed with appropriate error messages.

    Updates Analysis.status to ANALYSIS_QC_FAILED and adds detailed error reasons
    for all failed imports/validations. Uses bulk update for performance.

    This function can be called independently to update analyses without batch context.

    :param import_results: List of import results from the importer task
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    :param validation_only: Whether this was validation-only (no import)
    """
    logger = get_run_logger()

    # Extract failed analysis IDs
    failed_analysis_ids = [r.analysis_id for r in import_results if not r.success]

    if not failed_analysis_ids:
        return

    # Bulk fetch all failed analyses to avoid N+1 queries
    failed_analyses = {
        a.id: a for a in Analysis.objects.filter(id__in=failed_analysis_ids)
    }

    analyses_to_update = []
    for result in import_results:
        if not result.success:
            analysis = failed_analyses[result.analysis_id]

            # Ensure status is initialized as a dictionary
            if not analysis.status:
                analysis.status = Analysis.AnalysisStates.default_status()

            # Mark as QC failed
            analysis.status[f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}"] = True

            # Add detailed error reason
            error_prefix = "Validation error" if validation_only else "Import error"
            error_message = (
                f"{pipeline_type.value.upper()} {error_prefix}: {result.error}"
            )
            analysis.status[f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"] = (
                error_message
            )

            analyses_to_update.append(analysis)

    # Bulk update all analyses at once
    if analyses_to_update:
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Marked {len(analyses_to_update)} analyses as ANALYSIS_QC_FAILED")


@task(
    retries=2,
    retry_delay_seconds=60,
)
def process_import_results(
    assembly_analyses_batch_id: uuid.UUID,
    import_results: List[ImportResult],
    pipeline_type: AssemblyAnalysisPipeline,
    validation_only: bool = False,
):
    """
    Process the pipeline_schema import results and update statuses in bulk.

    Handles failed imports by:
    - Marking batch analysis status as FAILED
    - Marking Analysis status as ANALYSIS_QC_FAILED with reason

    :param assembly_analyses_batch_id: The AssemblyAnalysisBatch to process
    :param import_results: List of import results from the importer task
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    :param validation_only: Whether this was validation-only (no import)
    """
    logger = get_run_logger()

    batch = AssemblyAnalysisBatch.objects.get(id=assembly_analyses_batch_id)

    if not import_results:
        logger.warning("No import results to process")
        # Update counts to reflect the current status
        batch.update_pipeline_status_counts(pipeline_type)
        return

    failed_analysis_ids = [r.analysis_id for r in import_results if not r.success]

    # No failed analyses, nothing to do
    if not failed_analysis_ids:
        return

    # Bulk update batch analysis statuses to FAILED
    batch.batch_analyses.filter(analysis_id__in=failed_analysis_ids).update(
        **{f"{pipeline_type.value}_status": AssemblyAnalysisPipelineStatus.FAILED}
    )

    # Update analysis statuses
    mark_analyses_with_failed_status(import_results, pipeline_type, validation_only)

    # Log errors to batch error_log
    for result in import_results:
        if not result.success:
            batch.log_error(
                pipeline_type=pipeline_type,
                error_type="validation" if validation_only else "import",
                message=result.error,
                analysis_id=result.analysis_id,
                save=False,  # Batch will be saved once after all errors logged
            )

    # Persist the errors in the batch
    batch.save()
