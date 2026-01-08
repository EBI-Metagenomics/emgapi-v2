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


@task(
    retries=2,
    retry_delay_seconds=60,
)
def process_import_results(
    assembly_analyses_batch_id: uuid.UUID,
    import_results: List[ImportResult],
    pipeline_type: AssemblyAnalysisPipeline,
):
    """
    Process the pipeline_schema import results and update statuses in bulk.

    Handles failed imports by:
    - Marking batch analysis status as FAILED
    - Marking Analysis status as ANALYSIS_QC_FAILED with reason

    :param assembly_analyses_batch_id: The AssemblyAnalysisBatch to process
    :param import_results: List of import results from the importer task
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    """
    logger = get_run_logger()

    batch = AssemblyAnalysisBatch.objects.get(id=assembly_analyses_batch_id)

    if not import_results:
        logger.warning("No import results to process")
        # Update counts to reflect the current status
        batch.update_pipeline_status_counts(pipeline_type)
        return

    failed_analysis_ids = [r.analysis_id for r in import_results if not r.success]
    is_validation_only = import_results[0].validation_only if import_results else False

    # No failed analyses, nothing to do
    if not failed_analysis_ids:
        return

    mode = "validation" if is_validation_only else "import"
    logger.warning(
        f"{len(failed_analysis_ids)} {pipeline_type.value.upper()} analyses failed {mode}"
    )

    # Bulk update batch analysis statuses to FAILED
    batch.batch_analyses.filter(analysis_id__in=failed_analysis_ids).update(
        **{f"{pipeline_type.value}_status": AssemblyAnalysisPipelineStatus.FAILED}
    )

    # Bulk update Analysis statuses to ANALYSIS_QC_FAILED
    # Build lookup dict to avoid N+1 queries
    failed_analyses = {
        a.id: a for a in Analysis.objects.filter(id__in=failed_analysis_ids)
    }
    analyses_to_update = []
    for result in import_results:
        if not result.success:
            analysis = failed_analyses[result.analysis_id]
            analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = True

            # Distinguish between validation and import failures
            error_prefix = (
                "Validation error" if result.validation_only else "Import error"
            )
            error_message = (
                f"{pipeline_type.value.upper()} {error_prefix}: {result.error}"
            )
            analysis.status[f"{Analysis.AnalysisStates.ANALYSIS_QC_FAILED}__reason"] = (
                error_message
            )

            analyses_to_update.append(analysis)

            # Log error to batch error_log
            batch.log_error(
                pipeline_type=pipeline_type,
                error_type="validation" if result.validation_only else "import",
                message=result.error,
                analysis_id=result.analysis_id,
                save=False,  # Batch will be saved once after all errors logged
            )

    if analyses_to_update:
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Marked {len(analyses_to_update)} analyses as ANALYSIS_QC_FAILED")

    # Persist the errors in the batch
    if failed_analysis_ids:
        batch.save()
