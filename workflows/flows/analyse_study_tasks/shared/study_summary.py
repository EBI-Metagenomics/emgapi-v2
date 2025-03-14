from pathlib import Path

import click
from mgnify_pipelines_toolkit.analysis.shared.study_summary_generator import (
    summarise_analyses,
    merge_summaries,
)
from prefect import flow, get_run_logger, task

from activate_django_first import EMG_CONFIG
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)

from analyses.models import Study
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File
from workflows.prefect_utils.dir_context import chdir


@flow
def generate_study_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> [Path]:
    """
    Generate a study summary file for an analysis pipeline execution,
    e.g. a run of the V6 Amplicon pipeline on a samplesheet of runs.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param pipeline_outdir: The path to dir where pipeline published results are, e.g. /nfs/my/dir/abcedfg
    :param completed_runs_filename: E.g. qs_completed_runs.csv, expects to be found in pipeline_outdir
    :return: List of paths to the study summary files generated in the study dir
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)

    logger.info(f"Generating study summary for a pipeline execution of {study}")

    results_dir = Directory(
        path=Path(pipeline_outdir),
        rules=[DirectoryExistsRule],
    )
    results_dir.files.append(
        File(
            path=results_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )
    logger.info(f"Expecting to find taxonomy summaries in {results_dir.path}")
    logger.info(f"Using runs from {results_dir.files[0].path}")

    if not study.results_dir:
        study.results_dir = (
            Path(EMG_CONFIG.slurm.default_workdir) / f"{study.ena_study.accession}_v6"
        )
        logger.info(f"Setting {study}'s results_dir to default {study.results_dir}")
        study.results_dir.mkdir(exist_ok=True)
        study.save()
        # TODO: needs to be synced after merge
    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )

    logger.info(
        f"Study results_dir, where summaries will be made, is {study.results_dir}"
    )
    with chdir(study.results_dir):
        with click.Context(summarise_analyses) as ctx:
            ctx.invoke(
                summarise_analyses,
                runs=results_dir.files[0].path,
                analyses_dir=results_dir.path,
                non_insdc=EMG_CONFIG.amplicon_pipeline.allow_non_insdc_run_names,
                output_prefix=pipeline_outdir.name,  # e.g. a hash of the samplesheet
            )

    generated_files = list(
        study_dir.path.glob(f"{pipeline_outdir.name}*_study_summary.tsv")
    )
    logger.info(f"Study summary generator made files: {generated_files}")
    return generated_files


@flow
def merge_study_summaries(
    mgnify_study_accession: str, cleanup_partials: bool = False
) -> [Path]:
    """
    Merge multiple study summary files for a study, where each part was made by e.g. a single samplesheet.
    The files will be found in the study's results_dir.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param cleanup_partials: If True, will also delete the partial study summary files if and when they're merged.
    :return: List of paths to the study summary files generated in the study dir
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)

    logger.info(f"Merging study summaries for {study}")
    summary_files = list(Path(study.results_dir).glob("*study_summary.tsv"))
    logger.info(
        f"There appear to be {len(summary_files)} study summary files in {study.results_dir}"
    )

    logger.info(
        f"Study results_dir, where summaries will be merged, is {study.results_dir}"
    )
    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )
    with chdir(study.results_dir):
        with click.Context(merge_summaries) as ctx:
            ctx.invoke(
                merge_summaries,
                analyses_dir=study_dir.path,
                output_prefix=study.first_accession,
            )

    generated_files = list(
        study_dir.path.glob(f"{study.first_accession}*_study_summary.tsv")
    )

    if not generated_files:
        logger.warning(f"No study summary was merged in {study.results_dir}")
        return []

    if cleanup_partials:
        for file in summary_files:
            logger.info(f"Removing partial study summary file {file}")
            assert not file.name.startswith(
                study.first_accession
            )  # ensure we do not delete merged files
            file.unlink()


@task
def add_study_summaries_to_downloads(mgnify_study_accession: str):
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)

    for summary_file in Path(study.results_dir).glob(
        f"{study.first_accession}*study_summary.tsv"
    ):
        db_or_region = (
            summary_file.stem.split("_")[1].rstrip("_study_summary.tsv").rstrip("asv")
        )
        try:
            study.add_download(
                DownloadFile(
                    path=summary_file.relative_to(study.results_dir),
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group="study_summary",
                    file_type=DownloadFileType.TSV,
                    short_description=f"Summary of {db_or_region} taxonomies",
                    long_description=f"Summary of {db_or_region} taxonomic assignments, across all runs in the study",
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
