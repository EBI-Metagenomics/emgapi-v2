from pathlib import Path
import click

from activate_django_first import EMG_CONFIG

from prefect import flow, task, get_run_logger
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

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)


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
    summary_dir.path.mkdir(parents=True, exist_ok=True)

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


@task
def add_dwcr_summaries_to_downloads(mgnify_study_accession: str):
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)
    # Set the results_dir if it hasn't been set yet, so that it can be used in the summary generator'
    study.set_results_dir_default()

    pipeline_config = EMG_CONFIG.amplicon_pipeline

    summary_dir = (
        study.results_dir_path
        / f"{pipeline_config.pipeline_name}_{pipeline_config.pipeline_version}"
    )

    # Add ASV DwC-R summary files

    dada2_study_summary_files = list(
        Path(summary_dir).glob(f"{study.first_accession}_DADA2*{DWCREADY_CSV}")
    )

    for summary_file in dada2_study_summary_files:
        db = summary_file.stem.split("_")[1].lstrip("DADA2_")
        region = summary_file.stem.split("_")[2]
        try:
            study.add_download(
                DownloadFile(
                    path=Path("study-summaries") / summary_file.name,
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group="study_summary",
                    file_type=DownloadFileType.CSV,
                    short_description=f"DwC-Ready summary of {region} ASV taxonomies using {db} as ref DB",
                    long_description=f"DwC-Ready summary of {region} ASV taxonomies using {db} as ref DB, across all runs in the study",
                    alias=summary_file.name,
                )
            )
        except FileExistsError:
            logger.warning(
                f"File {summary_file} already exists in downloads list, skipping"
            )
        logger.info(f"Added {summary_file} to downloads of {study}")

    # Add closed reference DwC-R summary files
    closedref_study_summary_files = list(
        Path(summary_dir).glob(f"{study.first_accession}_closedref*{DWCREADY_CSV}")
    )
    for summary_file in closedref_study_summary_files:
        db = summary_file.stem.split("_")[2]
        try:
            study.add_download(
                DownloadFile(
                    path=Path("study-summaries") / summary_file.name,
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group="study_summary",
                    file_type=DownloadFileType.CSV,
                    short_description=f"DwC-Ready summary of closed-ref taxonomies using {db} as ref DB",
                    long_description=f"DwC-Ready summary of closed-reference taxonomies using {db} as ref DB, across all runs in the study",
                    alias=summary_file.name,
                )
            )
        except FileExistsError:
            logger.warning(
                f"File {summary_file} already exists in downloads list, skipping"
            )
        logger.info(f"Added {summary_file} to downloads of {study}")

    study.refresh_from_db()
    logger.info(
        f"Study download aliases are now {[d.alias for d in study.downloads_as_objects]}"
    )
