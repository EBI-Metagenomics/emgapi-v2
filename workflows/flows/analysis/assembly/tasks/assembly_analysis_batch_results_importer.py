import uuid

from prefect import task, get_run_logger

from workflows.data_io_utils.mgnify_v6_utils.assembly import AssemblyResultImporter
from workflows.data_io_utils.schemas import (
    MapResultSchema,
    VirifyResultSchema,
    AssemblyResultSchema,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


@task
def assembly_analysis_batch_results_importer(
    assembly_analyses_batch_id: uuid.UUID, pipeline_type: AssemblyAnalysisPipeline
):
    """
    Handles the import of mapping batch results into the database by processing
    and validating data for each analysis in the specified assembly batch.

    :param assembly_batch_id: The unique identifier of the assembly batch to process.
    :type assembly_batch_id: UUID
    :raises DoesNotExist: If the specified AssemblyAnalysisBatch does not exist.
    :raises ValidationError: If validation of the pipeline results fails for any analysis.
    """
    logger = get_run_logger()

    batch: AssemblyAnalysisBatch = AssemblyAnalysisBatch.objects.get(
        id=assembly_analyses_batch_id
    )

    schema = None
    base_path = None

    # This is a bit ugly, but it is done like this to avoid having Prefect to serialize a complex schema as input
    if pipeline_type == AssemblyAnalysisPipeline.ASA:
        schema = AssemblyResultSchema()
        base_path = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    if pipeline_type == AssemblyAnalysisPipeline.VIRIFY:
        schema = VirifyResultSchema()
        base_path = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)
    if pipeline_type == AssemblyAnalysisPipeline.MAP:
        schema = MapResultSchema()
        base_path = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.MAP)

    status_field = f"{pipeline_type.value}_status"
    completed_analysis_ids = batch.batch_analyses.filter(
        **{status_field: AssemblyAnalysisPipelineStatus.COMPLETED}
    ).values_list("analysis_id", flat=True)

    analyses = batch.analyses.filter(id__in=completed_analysis_ids).select_related(
        "assembly"
    )

    for analysis in analyses:
        logger.info(
            f"Validating and importing results into the DB the downloads for {analysis} using schema {schema}"
        )
        # TODO: extend this to capture errors and log them, if the validation fails it will just bubble up
        importer = AssemblyResultImporter(analysis)
        downloads_count = importer.import_results(
            schema=schema,
            base_path=base_path,
            validate_first=True,
        )
        logger.info(f"Successfully imported {downloads_count} downloads for {analysis}")
