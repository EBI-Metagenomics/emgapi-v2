from pathlib import Path
import click

from activate_django_first import EMG_CONFIG

from prefect import flow, get_run_logger
from analyses.models import Study

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

from mgnify_pipelines_toolkit.analysis.shared.dwc_summary_generator import (
    generate_dwcready_summaries,
)

from workflows.prefect_utils.dir_context import chdir


@flow
def generate_dwc_ready_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> list[Path] | None:
    logger = get_run_logger()

    study = Study.objects.get(accession=mgnify_study_accession)

    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )

    logger.info(pipeline_outdir)
    logger.info(completed_runs_filename)
    logger.info(
        f"Generating Darwin Core Ready (DwC-R) summary files for a pipeline execution of study {mgnify_study_accession}"
    )
    results_dir = Directory(
        path=pipeline_outdir,
        rules=[DirectoryExistsRule],
    )
    results_dir.files.append(
        File(
            path=results_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    runs = results_dir.files[0].path
    logger.info(f"Expecting to find taxonomy summaries in {results_dir.path}")
    logger.info(f"Using runs from {runs}")

    with chdir(study.results_dir):
        prefix = pipeline_outdir.name

        logger.debug(f"For DwC-R summary, {study_dir.path = }, {runs = }, {prefix = }")
        logger.debug(f"Glob of input_path is {list(results_dir.path.glob('*'))}")

        content = runs.read_text()
        logger.debug(f"Content of runs file is\n{content}")

        with click.Context(generate_dwcready_summaries) as ctx:
            ctx.invoke(
                generate_dwcready_summaries,
                runs=runs,
                analyses_dir=results_dir.path,
                output_prefix=prefix,
            )

    generated_files = list(Path(study.results_dir).glob("*_dwcready.csv"))

    logger.info(generated_files)

    return generated_files


@flow
def merge_dwc_ready_summaries(
    mgnify_study_accession: str,
    cleanup_partials: bool = False,
    bludgeon: bool = True,
) -> list[Path] | None:
    logger = get_run_logger()
    logger.warning("Not implemented yet")
    # TODO: implement me :)
    return []
