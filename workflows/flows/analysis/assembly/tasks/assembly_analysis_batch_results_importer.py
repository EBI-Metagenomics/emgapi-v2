import uuid
from typing import List
import logging

from prefect import task, get_run_logger

import django

django.setup()

from analyses.models import Analysis

from workflows.data_io_utils.mgnify_v6_utils.assembly import AssemblyResultImporter
from workflows.data_io_utils.schemas import (
    MapResultSchema,
    VirifyResultSchema,
    AssemblyResultSchema,
    PipelineValidationError,
)
from workflows.data_io_utils.schemas.assembly import ImportResult
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)


def clear_pipeline_downloads(
    analysis: Analysis, pipeline_type: AssemblyAnalysisPipeline
):
    """
    Clear existing downloads for a specific pipeline from the analysis.

    This ensures idempotent imports by removing any previous download records
    before re-importing. This prevents FileExistsError on duplicate aliases
    when retrying failed imports.

    TODO: is this the best place for this?.. not sure (mbc)

    :param analysis: The Analysis object
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    """

    logger = logging.getLogger(__name__)

    # Map pipeline types to their download_group prefixes
    pipeline_download_groups = {
        AssemblyAnalysisPipeline.ASA: [
            "quality_control",
            "taxonomy",
            "annotation_summary",
            Analysis.CODING_SEQUENCES,
            Analysis.FUNCTIONAL_ANNOTATION,
        ],
        AssemblyAnalysisPipeline.VIRIFY: [Analysis.VIRIFY],
        AssemblyAnalysisPipeline.MAP: [Analysis.MAP],
    }

    if pipeline_type not in pipeline_download_groups:
        logger.warning(
            f"Unknown pipeline type {pipeline_type}, skipping download cleanup"
        )
        return

    prefixes = pipeline_download_groups[pipeline_type]

    # Filter out downloads matching any of the pipeline's prefixes
    original_count = len(analysis.downloads)
    analysis.downloads = [
        dl
        for dl in analysis.downloads
        if not any(
            dl.get("download_group", "").startswith(prefix) for prefix in prefixes
        )
    ]

    removed_count = original_count - len(analysis.downloads)
    if removed_count > 0:
        logger.info(
            f"Cleared {removed_count} existing {pipeline_type.value.upper()} downloads from {analysis.accession} for idempotent retry"
        )
        analysis.save()


@task
def assembly_analysis_batch_results_importer(
    assembly_analyses_batch_id: uuid.UUID,
    pipeline_type: AssemblyAnalysisPipeline,
    validation_only: bool = False,
) -> List[ImportResult]:
    """
    Handles the import of mapping batch results into the database by processing
    and validating data for each analysis in the specified assembly batch.

    :param assembly_batch_id: The unique identifier of the assembly batch to process.
    :type assembly_batch_id: UUID
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    :type pipeline_type: AssemblyAnalysisPipeline
    :param validation_only: If True, only validate schemas without importing (default: False)
    :type validation_only: bool
    :return: List of import results for each analysis
    :rtype: List[ImportResult]
    :raises DoesNotExist: If the specified AssemblyAnalysisBatch does not exist.
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

    if schema is None or base_path is None:
        raise ValueError(f"Unsupported pipeline type: {pipeline_type}")

    status_field = f"{pipeline_type.value}_status"
    completed_analysis_ids = batch.batch_analyses.filter(
        **{status_field: AssemblyAnalysisPipelineStatus.COMPLETED}
    ).values_list("analysis_id", flat=True)

    analyses = batch.analyses.filter(id__in=completed_analysis_ids).select_related(
        "assembly"
    )

    results = []
    for analysis in analyses:
        if validation_only:
            logger.info(
                f"Validating (without import) results for {analysis} using schema {schema}"
            )
        else:
            logger.info(
                f"Validating and importing results into the DB the downloads for {analysis} using schema {schema}"
            )

        try:
            # TODO: I think this needs a bit of thought, it just doesn't feel right. I smell repetition and things
            #       that just don't feel "natural"
            if validation_only:
                # Only validate, don't import
                schema.validate_results(base_path, analysis.assembly.first_accession)
                logger.info(f"Validation successful for {analysis}")
                results.append(ImportResult(analysis_id=analysis.id, success=True))
            else:
                # Validate and import
                importer = AssemblyResultImporter(analysis)

                # Clear existing downloads for this pipeline to ensure idempotency
                # This prevents duplicate download errors on retry
                clear_pipeline_downloads(analysis, pipeline_type)

                downloads_count = importer.import_results(
                    schema=schema,
                    base_path=base_path,
                    validate_first=True,
                )
                logger.info(
                    f"Successfully imported {downloads_count} downloads for {analysis}"
                )
                results.append(
                    ImportResult(
                        analysis_id=analysis.id,
                        success=True,
                        downloads_count=downloads_count,
                    )
                )
        except PipelineValidationError as e:
            logger.exception(f"Validation failed for {analysis}: {e}")
            results.append(
                ImportResult(
                    analysis_id=analysis.id,
                    success=False,
                    error=str(e),
                )
            )
        except Exception as e:
            logger.exception(
                f"Unexpected error {'validating' if validation_only else 'importing'} {analysis}: {e}"
            )
            results.append(
                ImportResult(
                    analysis_id=analysis.id,
                    success=False,
                    error=f"Unexpected error: {str(e)}",
                )
            )

    return results
