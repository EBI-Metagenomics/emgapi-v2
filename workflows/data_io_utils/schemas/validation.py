from pathlib import Path
from typing import Type

from workflows.data_io_utils.file_rules.nodes import Directory
from .base import PipelineResultSchema
from .exceptions import PipelineValidationError


def sanity_check_pipeline_results(
    schema: Type[PipelineResultSchema],
    base_path: Path,
    identifier: str,
    pipeline_name: str = None,
) -> Directory:
    """
    Unified validation function for pipeline results.

    This function provides a common interface for validating pipeline outputs
    using the appropriate schema, with consistent error handling and logging.

    :param schema: The pipeline result schema class to use for validation
    :param base_path: Base path containing the pipeline results
    :param identifier: Pipeline run identifier (e.g., assembly_id)
    :param pipeline_name: Optional pipeline name for error messages
    :return: Validated Directory object
    :raises PipelineValidationError: If validation fails
    """
    if pipeline_name is None:
        pipeline_name = getattr(schema, "pipeline_name", "Unknown Pipeline")

    try:
        return schema.validate_results(base_path, identifier)
    except PipelineValidationError as e:
        # Re-raise with additional context if needed
        if not e.pipeline_name:
            e.pipeline_name = pipeline_name
        raise e
    except Exception as e:
        # Convert unexpected errors to PipelineValidationError
        raise PipelineValidationError(
            f"Unexpected error during {pipeline_name} validation: {e}",
            pipeline_name=pipeline_name,
        ) from e
