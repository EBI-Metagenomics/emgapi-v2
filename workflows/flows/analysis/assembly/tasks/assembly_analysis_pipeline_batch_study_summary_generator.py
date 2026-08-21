import uuid
from pathlib import Path
from typing import List, Union

import click
from mgnify_pipelines_toolkit.analysis.assembly import study_summary_generator
from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from analyses.models import Study
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.flows.analyse_study_tasks.shared.study_summary import STUDY_SUMMARY_TSV
from workflows.models import AssemblyAnalysisBatch, AssemblyAnalysisPipeline
from workflows.prefect_utils.dir_context import chdir
from workflows.prefect_utils.flows_utils import django_db_task as task


def _run_assembly_summary_generator(
    output_dir: Path,
    asa_workspace: Path,
    assemblies_csv: Path,
    output_prefix: str,
) -> List[Path]:

    logger = get_run_logger()

    logger.info(f"Study results_dir, where summaries will be made, is {output_dir}")

    with chdir(asa_workspace):
        # TODO: we need to expose the summary as a lib component we can just import instead of having to use
        #       click to bootstrap the environment
        with click.Context(study_summary_generator.summarise_analyses) as ctx:
            ctx.invoke(
                study_summary_generator.summarise_analyses,
                output_prefix=output_prefix,
                assemblies=assemblies_csv.absolute(),
                study_dir=asa_workspace.absolute(),
                outdir=output_dir.absolute(),
            )

    generated_files = list(output_dir.glob(f"{output_prefix}*{STUDY_SUMMARY_TSV}"))

    if not generated_files:
        raise FileNotFoundError(
            f"No study summary files were generated in {output_dir} "
            f"with prefix {output_prefix}"
        )

    logger.info(f"Assembly summary generator made files: {generated_files}")

    return generated_files


@task()
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

    return generate_assembly_analysis_pipeline_summary(
        study_accession=study.accession,
        asa_workspace=asa_workspace,
        output_prefix=str(assembly_batch.id),
    )


@task
def generate_assembly_analysis_pipeline_summary(
    study_accession: str,
    asa_workspace: Path,
    output_prefix: Union[str, None] = None,
) -> Union[List[Path], None]:
    """
    Generate a study summary file for assembly analysis results.

    The study summaries are written to the study.results_dir.

    :param study_accession: Accession of the Study to summarize annotation results for.
    :param asa_workspace: Directory containing the ASA end-of-run reports
        (``analysed_assemblies.csv``) to summarise.
    :param output_prefix: Prefix for the generated summary file names.
        Defaults to the study's first accession. Batch summary runs
        override this with their own identifier, because each batch
        only produces a partial summary that is merged with others later.
    :return: List of paths to the study summary files generated
    """

    logger = get_run_logger()

    study = Study.objects.get(accession=study_accession)

    logger.info(f"Generating assembly summary for {study.id}")
    logger.info(f"Expecting to find analysis results in {asa_workspace}")

    # Ensure the study has a canonical results_dir to write summaries to.
    study.set_results_dir_default()

    assemblies_csv = asa_workspace / "analysed_assemblies.csv"

    pipeline_config = EMG_CONFIG.assembly_analysis_pipeline
    summary_dir = Directory(
        path=(
            study.results_dir_path
            / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
            / "summaries"
        ),
    )
    summary_dir.path.mkdir(parents=True, exist_ok=True)

    return _run_assembly_summary_generator(
        output_dir=summary_dir.path,
        asa_workspace=asa_workspace,
        assemblies_csv=assemblies_csv,
        output_prefix=output_prefix or study.first_accession,
    )
