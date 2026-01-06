from pathlib import Path
import tempfile
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
    mgnify_study_accession: Study,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> list[Path] | None:
    logger = get_run_logger()
    logger.warning("Not implemented yet")
    # TODO: implement me :)
    logger.info(mgnify_study_accession)
    logger.info(pipeline_outdir)
    logger.info(completed_runs_filename)
    logger.info(
        f"Generating Darwin Core Ready (DwC-R) summary files for a pipeline execution of study {mgnify_study_accession}"
    )
    results_dir = Directory(
        path=Path("/app/data/tests/amplicon_v6_output/"),
        rules=[DirectoryExistsRule],
    )
    results_dir.files.append(
        File(
            path=results_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    input_path = results_dir.path
    runs = results_dir.files[0].path
    logger.info(f"Expecting to find taxonomy summaries in {input_path}")
    logger.info(f"Using runs from {runs}")

    with tempfile.TemporaryDirectory() as workdir:
        with chdir(workdir):
            logger.info(f"Using temporary workdir {workdir}")
            prefix = pipeline_outdir.name

            logger.debug(f"For DwC-R summary, {input_path = }, {runs = }, {prefix = }")
            logger.debug(f"Glob of input_path is {list(input_path.glob('*'))}")

            content = runs.read_text()
            logger.debug(f"Content of runs file is\n{content}")

            with click.Context(generate_dwcready_summaries) as ctx:
                ctx.invoke(
                    generate_dwcready_summaries,
                    runs=runs,
                    analyses_dir="/app/data/tests/amplicon_v6_output/",
                    output_prefix="chunk1",
                )

    return []


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
