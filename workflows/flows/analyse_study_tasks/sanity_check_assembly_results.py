from pathlib import Path

from prefect import task
from prefect.tasks import task_input_hash

import analyses.models
from workflows.data_io_utils.schemas.assembly import AssemblyResultSchema
from workflows.data_io_utils.schemas.validation import sanity_check_pipeline_results


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
    :return: Validated Directory object
    """
    schema = AssemblyResultSchema()
    return sanity_check_pipeline_results(
        schema,
        assembly_analysis_current_outdir,
        analysis.assembly.first_accession,
        "Assembly",
    )


def create_assembly_analysis_schema(assembly_current_outdir: Path, assembly_id: str):
    """
    Legacy function for backwards compatibility.

    Create a schema for the assembly pipeline output using the unified schema approach.

    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param assembly_id: Assembly ID (e.g. ERZ123456)
    :return: Directory object representing the assembly analysis schema
    """
    schema = AssemblyResultSchema()
    return sanity_check_pipeline_results(
        schema, assembly_current_outdir, assembly_id, "Assembly"
    )
