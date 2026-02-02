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

from workflows.ena_utils.ena_accession_matching import (
    INSDC_PROJECT_ACCESSION_GLOB,
    INSDC_STUDY_ACCESSION_GLOB,
)

from workflows.flows.analyse_study_tasks.shared.study_summary import DWCREADY_CSV


from mgnify_pipelines_toolkit.analysis.shared.dwc_summary_generator import (
    generate_dwcready_summaries,
    merge_dwcr_summaries,
)

from workflows.prefect_utils.dir_context import chdir


@flow
def generate_dwc_ready_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path,
    refdb_otus_dir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> list[Path] | None:
    logger = get_run_logger()

    study = Study.objects.get(accession=mgnify_study_accession)

    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline
    pipeline_run_dir = Directory(
        path=pipeline_outdir,
        rules=[DirectoryExistsRule],
    )
    summary_dir = Directory(
        path=(
            study.results_dir_path
            / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
            / "summaries"
        ),
    )
    summary_dir.path.mkdir(exist_ok=True)

    logger.info(pipeline_outdir)
    logger.info(completed_runs_filename)
    logger.info(
        f"Generating Darwin Core Ready (DwC-R) summary files for a pipeline execution of study {mgnify_study_accession}"
    )
    pipeline_run_dir.files.append(
        File(
            path=pipeline_run_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    runs = pipeline_run_dir.files[0].path
    logger.info(f"Expecting to find taxonomy summaries in {pipeline_run_dir.path}")
    logger.info(f"Using runs from {runs}")

    with chdir(pipeline_run_dir.path):
        prefix = f"{summary_dir.path}/{pipeline_run_dir.path.name}"

        logger.debug(
            f"For DwC-R summary, {pipeline_run_dir.path = }, {runs = }, {prefix = }"
        )
        logger.debug(f"Glob of input_path is {list(pipeline_run_dir.path.glob('*'))}")

        content = runs.read_text()
        logger.debug(f"Content of runs file is\n{content}")

        with click.Context(generate_dwcready_summaries) as ctx:
            ctx.invoke(
                generate_dwcready_summaries,
                runs=runs,
                analyses_dir=pipeline_run_dir.path,
                otu_dir=refdb_otus_dir,
                output_prefix=prefix,
            )

    generated_files = list(summary_dir.path.glob("*_dwcready.csv"))

    logger.info(generated_files)

    return generated_files


@flow
def merge_dwc_ready_summaries(
    mgnify_study_accession: str,
    cleanup_partials: bool = False,
    bludgeon: bool = True,
) -> list[Path] | None:
    logger = get_run_logger()

    study = Study.objects.get(accession=mgnify_study_accession)

    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline
    study_dir = Directory(
        path=(
            study.results_dir_path
            / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
        ),
        rules=[DirectoryExistsRule],
    )
    summary_dir = Directory(
        path=study_dir.path / "summaries",
        rules=[DirectoryExistsRule],
    )

    logger.info(f"Merging DwC-R summaries for {study}, in {summary_dir}")

    if not summary_dir:
        logger.warning(
            f"Study {study} has no results_dir, so cannot merge DwC-R summaries"
        )
        return []

    logger.debug(f"Glob of dir is {list(summary_dir.path.glob('*'))}")
    existing_merged_files = list(
        study_dir.path.glob(f"{INSDC_PROJECT_ACCESSION_GLOB}{DWCREADY_CSV}")
    ) + list(study_dir.path.glob(f"{INSDC_STUDY_ACCESSION_GLOB}{DWCREADY_CSV}"))
    if existing_merged_files:
        logger.warning(
            f"{len(existing_merged_files)} DwC-R summaries already exist in {study_dir.path}"
        )
    if bludgeon:
        for existing_merged_file in existing_merged_files:
            logger.warning(f"Deleting {existing_merged_file}")
            existing_merged_file.unlink()

    summary_files = list(summary_dir.path.glob(f"*{DWCREADY_CSV}"))
    logger.info(
        f"There appear to be {len(summary_files)} DwC-R summary files in {summary_dir.path}"
    )

    logger.info(
        f"Study results_dir, where DwC-R summaries will be merged, is {study_dir.path}"
    )

    with chdir(summary_dir.path):
        with click.Context(merge_dwcr_summaries) as ctx:
            ctx.invoke(
                merge_dwcr_summaries,
                analyses_dir=summary_dir.path,
                output_prefix=f"{study_dir.path}/{study.first_accession}",
            )

    generated_files = list(
        study_dir.path.glob(f"{study.first_accession}*{DWCREADY_CSV}")
    )

    if not generated_files:
        logger.warning(f"No DwC-R summary was merged in {study_dir.path}")
        return []

    logger.info(f"These are the merged DwC-R summary files: {generated_files}")

    if cleanup_partials:
        for file in summary_files:
            logger.info(f"Removing partial study summary file {file}")
            assert not file.name.startswith(
                study.first_accession
            )  # ensure we do not delete merged files
            file.unlink()
