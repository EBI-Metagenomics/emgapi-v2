import uuid
from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG  # noqa

from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    assembly_analysis_batch_results_importer,
)
from workflows.flows.analysis.assembly.tasks.process_import_results import (
    process_import_results,
)

from workflows.flows.analysis.assembly.tasks.set_post_assembly_analysis_states import (
    set_post_assembly_analysis_states,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
)


@flow(
    flow_run_name="Import ASA Batch: {assembly_analyses_batch_id}",
    retries=2,
    retry_delay_seconds=60,
)
def import_asa_batch(assembly_analyses_batch_id: uuid.UUID):
    """
    Imports Assembly Analysis Pipeline results for a given batch.

    This flow will validate the results, update statuses based on analysis outcomes,
    and import data if validation succeeds.

    :param assembly_analyses_batch_id: The AssemblyAnalysisBatch to process
    """
    logger = get_run_logger()

    assembly_analysis_batch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    assembly_analyses_workspace_dir = assembly_analysis_batch.get_pipeline_workspace(
        AssemblyAnalysisPipeline.ASA
    )
    assembly_analyses_ids = list(
        assembly_analysis_batch.analyses.values_list("id", flat=True)
    )

    # Set post-assembly analysis status based on pipeline output CSVs
    # Docs https://github.com/EBI-Metagenomics/assembly-analysis-pipeline/blob/dev/docs/output.md#per-study-output-files
    set_post_assembly_analysis_states(
        assembly_analyses_workspace_dir, assembly_analyses_ids
    )

    # First, validate schemas (without importing)
    # Note: This queries the database twice (validation + import). Could be optimized
    # to avoid the second query, but acceptable for current batch sizes.

    # TODO: this needs to be refactored even more I think (mbc), I was trying to split the validation from
    #       the status update of the records (which I somewhat achieved here). A design session on this would be
    #       needed.
    logger.info("Validating ASA results schemas...")
    validation_results = assembly_analysis_batch_results_importer(
        assembly_analyses_batch_id=assembly_analyses_batch_id,
        pipeline_type=AssemblyAnalysisPipeline.ASA,
        validation_only=True,
    )

    # Process validation results - mark failures as FAILED
    process_import_results(
        assembly_analysis_batch.id,
        validation_results,
        AssemblyAnalysisPipeline.ASA,
    )

    # Only import if validation passed for at least one analysis
    successful_validations = [r for r in validation_results if r.success]
    if successful_validations:
        logger.info(
            f"Importing {len(successful_validations)} validated ASA analyses..."
        )
        import_results = assembly_analysis_batch_results_importer(
            assembly_analyses_batch_id=assembly_analyses_batch_id,
            pipeline_type=AssemblyAnalysisPipeline.ASA,
            validation_only=False,
        )
        # Process import results (should all succeed since validation passed)
        process_import_results(
            assembly_analysis_batch.id,
            import_results,
            AssemblyAnalysisPipeline.ASA,
        )
    else:
        logger.error("No ASA analyses passed the results validation.")
