from pathlib import Path

from prefect import task
from prefect.tasks import task_input_hash

import analyses.models
from workflows.data_io_utils.mgnify_v6_utils.assembly import (
    create_assembly_v6_schema,
)


@task(
    cache_key_fn=task_input_hash,
)
def sanity_check_assembly_analysis_results(
    assembly_analysis_current_outdir: Path, analysis: analyses.models.Analysis
):
    """
    Validate assembly analysis results using the unified schema.

    :param assembly_analysis_current_outdir: Path to the directory containing the pipeline output
    :param analysis: The analysis to validate results for
    """
    schema = create_assembly_v6_schema()
    validated_directory = schema.validate_directory_structure(
        assembly_analysis_current_outdir,
        analysis.assembly.first_accession,
    )
    return validated_directory


def create_assembly_analysis_schema(assembly_current_outdir: Path, assembly_id: str):
    """
    Legacy function for backwards compatibility.

    Create a schema for the assembly pipeline output using the unified schema approach.

    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param assembly_id: Assembly ID (e.g. ERZ123456)
    :return: Directory object representing the assembly analysis schema
    """
    schema = create_assembly_v6_schema()
    return schema.validate_directory_structure(assembly_current_outdir, assembly_id)
