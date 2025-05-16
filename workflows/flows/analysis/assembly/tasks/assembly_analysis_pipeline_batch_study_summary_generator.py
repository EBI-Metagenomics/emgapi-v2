import uuid
from pathlib import Path
from typing import Union, List

import click
from prefect import get_run_logger, task

from mgnify_pipelines_toolkit.analysis.assembly import study_summary_generator

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.flows.analyse_study_tasks.shared.study_summary import STUDY_SUMMARY_TSV
from workflows.models import AssemblyAnalysisBatch, AssemblyAnalysisPipeline
from workflows.prefect_utils.dir_context import chdir


@task
def generate_assembly_analysis_pipeline_batch_summary(
    assembly_batch_id: uuid.UUID,
) -> Union[List[Path], None]:
    """
    Generate a study summary file for a single assembly analysis batch.

    It will use the analyses where the workflow_status for ASA is set to COMPLETED.

    The study summaries are written to the study.results_dir, these are partial files for a batch.
    They will be merged later, and the assumption is that they live in this directory.

    [NOTE]
    There is a bit of repetition with workflows/flows/analyse_study_tasks/shared/study_summary.py
    I (mbc) didn't want to refactor the former to account for batches, as we are testing the batch approach here.
    If it works we can refactor the shared code to be more generic.

    :param assembly_batch_id: The AssemblyAnalysisBatch to summarize
    :return: List of paths to the study summary files generated
    """

    logger = get_run_logger()

    assembly_batch = AssemblyAnalysisBatch.objects.get(id=assembly_batch_id)

    study = assembly_batch.study

    logger.info(f"Generating assembly batch summary for {assembly_batch}")

    # ASA workspace contains the analysis results
    asa_workspace = assembly_batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    logger.info(f"Expecting to find analysis results in {asa_workspace}")

    # Ensure the study has a results_dir to write summaries to
    study.set_results_dir_default()

    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )

    logger.info(
        f"Study results_dir, where summaries will be made, is {study.results_dir}"
    )

    with chdir(study.results_dir):
        # TODO: we need to expose the summary as a lib component we can just import instead of having to use
        #       click to bootstrap the environment
        with click.Context(study_summary_generator.summarise_analyses) as ctx:
            ctx.invoke(
                study_summary_generator.summarise_analyses,
                output_prefix=assembly_batch.id,
                assemblies=Path(
                    assembly_batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
                    / "analysed_assemblies.csv"
                ).absolute(),
                study_dir=Path(asa_workspace).absolute(),
                outdir=study.results_dir,
            )

    generated_files = list(
        study_dir.path.glob(f"{assembly_batch.id}*_{STUDY_SUMMARY_TSV}")
    )

    logger.info(f"Assembly batch summary generator made files: {generated_files}")

    return generated_files
