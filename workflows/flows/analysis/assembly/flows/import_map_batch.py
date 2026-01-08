import uuid
from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG  # noqa

from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    assembly_analysis_batch_results_importer,
)
from workflows.flows.analysis.assembly.tasks.process_import_results import (
    process_import_results,
)

from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


@flow(
    flow_run_name="Import MAP Batch: {assembly_analyses_batch_id}",
    retries=2,
    retry_delay_seconds=60,
)
def import_map_batch(assembly_analyses_batch_id: uuid.UUID):
    """
    Imports MAP analysis results for a given batch.

    :param assembly_analyses_batch_id: The AssemblyAnalysisBatch to process
    """

    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    # Import results if any analyses completed
    completed_map_analyses_count = assembly_analysis_batch.batch_analyses.filter(
        map_status=AssemblyAnalysisPipelineStatus.COMPLETED
    ).count()

    if completed_map_analyses_count:
        logger.info(
            f"Validating results for {completed_map_analyses_count} MAP completed analyses"
        )
        # First validate
        validation_results = assembly_analysis_batch_results_importer(
            assembly_analyses_batch_id=assembly_analyses_batch_id,
            pipeline_type=AssemblyAnalysisPipeline.MAP,
            validation_only=True,
        )
        process_import_results(
            assembly_analysis_batch.id,
            validation_results,
            AssemblyAnalysisPipeline.MAP,
        )

        # Then import successful validations
        successful_validations = [r for r in validation_results if r.success]
        if successful_validations:
            logger.info(
                f"Importing {len(successful_validations)} validated MAP analyses..."
            )
            import_results = assembly_analysis_batch_results_importer(
                assembly_analyses_batch_id=assembly_analyses_batch_id,
                pipeline_type=AssemblyAnalysisPipeline.MAP,
                validation_only=False,
            )
            process_import_results(
                assembly_analysis_batch.id,
                import_results,
                AssemblyAnalysisPipeline.MAP,
            )
        else:
            logger.warning("No MAP analyses passed the results validation.")
    else:
        logger.info("No MAP analyses completed, skipping result import")
