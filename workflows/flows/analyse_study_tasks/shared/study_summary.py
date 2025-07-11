from pathlib import Path
from typing import Union, Tuple, Literal, List

import click
from mgnify_pipelines_toolkit.analysis.amplicon import (
    study_summary_generator as amplicon_study_summary_generator,
)
from mgnify_pipelines_toolkit.analysis.assembly import (
    study_summary_generator as assembly_study_summary_generator,
)
from mgnify_pipelines_toolkit.analysis.rawreads import (
    study_summary_generator as rawreads_study_summary_generator,
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
from workflows.ena_utils.ena_accession_matching import (
    INSDC_PROJECT_ACCESSION_GLOB,
    INSDC_STUDY_ACCESSION_GLOB,
)
from workflows.prefect_utils.dir_context import chdir

STUDY_MULTIQC_REPORT = "_report.html"
STUDY_SUMMARY = "study_summary"
STUDY_SUMMARY_TSV = STUDY_SUMMARY + ".tsv"

STUDY_SUMMARY_GENERATORS = {
    "amplicon": amplicon_study_summary_generator,
    "rawreads": rawreads_study_summary_generator,
    "assembly": assembly_study_summary_generator,
}
PIPELINE_CONFIGS = {
    "amplicon": EMG_CONFIG.amplicon_pipeline,
    "rawreads": EMG_CONFIG.rawreads_pipeline,
    "assembly": EMG_CONFIG.assembly_analysis_pipeline,
}


@flow
def generate_study_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    analysis_type: Literal["amplicon", "assembly", "rawreads"] = "amplicon",
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> Union[List[Path], None]:
    """
    Generate a study summary file for an analysis pipeline execution,
    e.g. a run of the V6 Amplicon pipeline on a samplesheet of runs.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param pipeline_outdir: The path to dir where pipeline published results are, e.g. /nfs/my/dir/abcedfg
    :param analysis_type: "amplicon", "rawreads" or "assembly" (different summaries are generated)
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
    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )

    if analysis_type not in STUDY_SUMMARY_GENERATORS and (
        analysis_type in PIPELINE_CONFIGS
    ):
        raise ValueError(
            f"analysis_type must be 'amplicon', 'rawreads' or 'assembly', got {analysis_type}"
        )

    study_summary_generator = STUDY_SUMMARY_GENERATORS[analysis_type]
    pipeline_config = PIPELINE_CONFIGS[analysis_type]

    summary_generator_kwargs = {}
    if "allow_non_insdc_run_names" in pipeline_config:
        summary_generator_kwargs["non_insdc"] = (
            pipeline_config.allow_non_insdc_run_names
        )
    if analysis_type in {"rawreads", "amplicon"}:
        summary_generator_kwargs["runs"] = results_dir.files[0].path
        summary_generator_kwargs["analyses_dir"] = results_dir.path
    if analysis_type in {"assembly"}:
        summary_generator_kwargs["assemblies"] = results_dir.files[0].path
        summary_generator_kwargs["study_dir"] = results_dir.path

    logger.info(
        f"Study results_dir, where summaries will be made, is {study.results_dir}"
    )

    with chdir(study.results_dir):
        with click.Context(study_summary_generator.summarise_analyses) as ctx:
            ctx.invoke(
                study_summary_generator.summarise_analyses,
                output_prefix=pipeline_outdir.name,  # e.g. a hash of the samplesheet
                **summary_generator_kwargs,
            )

    generated_files = list(
        study_dir.path.glob(f"{pipeline_outdir.name}*_{STUDY_SUMMARY_TSV}")
    )
    logger.info(f"Study summary generator made files: {generated_files}")
    return generated_files


@flow
def merge_study_summaries(
    mgnify_study_accession: str,
    analysis_type: Literal["amplicon", "rawreads", "assembly"] = "amplicon",
    cleanup_partials: bool = False,
    bludgeon: bool = True,
) -> Union[List[Path], None]:
    """
    Merge multiple study summary files for a study, where each part was made by e.g. a single samplesheet.
    The files will be found in the study's results_dir.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param analysis_type: "amplicon", "rawreads" or "assembly" (different summaries are generated)
    :param cleanup_partials: If True, will also delete the partial study summary files if and when they're merged.
    :param bludgeon: If True, will delete any existing study-level summaries before merging.
    :return: List of paths to the study summary files generated in the study dir
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)

    logger.info(f"Merging study summaries for {study}, in {study.results_dir}")
    if not study.results_dir:
        logger.warning(f"Study {study} has no results_dir, so cannot merge summaries")
        return []
    logger.debug(f"Glob of dir is {list(Path(study.results_dir).glob('*'))}")
    existing_merged_files = list(
        Path(study.results_dir).glob(
            f"{INSDC_PROJECT_ACCESSION_GLOB}{STUDY_SUMMARY_TSV}"
        )
    ) + list(
        Path(study.results_dir).glob(f"{INSDC_STUDY_ACCESSION_GLOB}{STUDY_SUMMARY_TSV}")
    )
    if existing_merged_files:
        logger.warning(
            f"{len(existing_merged_files)} study-level summaries already exist in {study.results_dir}"
        )
    if bludgeon:
        for existing_merged_file in existing_merged_files:
            logger.warning(f"Deleting {existing_merged_file}")
            existing_merged_file.unlink()

    summary_files = list(Path(study.results_dir).glob(f"*{STUDY_SUMMARY_TSV}"))
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
        if analysis_type == "amplicon":
            with click.Context(amplicon_study_summary_generator.merge_summaries) as ctx:
                ctx.invoke(
                    amplicon_study_summary_generator.merge_summaries,
                    analyses_dir=study_dir.path,
                    output_prefix=study.first_accession,
                )
        elif analysis_type == "rawreads":
            with click.Context(rawreads_study_summary_generator.merge_summaries) as ctx:
                ctx.invoke(
                    rawreads_study_summary_generator.merge_summaries,
                    analyses_dir=study_dir.path,
                    output_prefix=study.first_accession,
                )
        elif analysis_type == "assembly":
            with click.Context(assembly_study_summary_generator.merge_summaries) as ctx:
                ctx.invoke(
                    assembly_study_summary_generator.merge_summaries,
                    study_dir=study_dir.path,
                    output_prefix=study.first_accession,
                )
        else:
            raise ValueError(
                f"analysis_type must be 'amplicon', 'rawreads' or 'assembly', got {analysis_type}"
            )

    generated_files = list(
        study_dir.path.glob(f"{study.first_accession}*_{STUDY_SUMMARY_TSV}")
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
    if not study.results_dir:
        logger.warning(
            f"Study {study} has no results_dir, so cannot add study summaries to downloads"
        )
        return

    for summary_file in Path(study.results_dir).glob(
        f"{study.first_accession}*{STUDY_SUMMARY_TSV}"
    ):
        db_or_region = (
            summary_file.stem.split("_")[1]
            .rstrip(f"_{STUDY_SUMMARY_TSV}")
            .rstrip("asv")
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


def _get_analysis_source(
    summary_file: Path,
) -> Tuple[Union[str, None], Union[str, None]]:
    if summary_file.stem.endswith(STUDY_MULTIQC_REPORT):
        analysis_source = summary_file.stem.rstrip(STUDY_MULTIQC_REPORT).split("_")
    elif summary_file.stem.endswith(STUDY_SUMMARY_TSV):
        analysis_source = summary_file.stem.rstrip(STUDY_SUMMARY_TSV).split("_")[1:]
    else:
        analysis_source = []

    if len(analysis_source) == 1:
        return analysis_source[0], None
    elif len(analysis_source) == 2:
        return analysis_source[0], analysis_source[1]
    else:
        return None, None


def _get_download_file(
    analysis_source: str,
    analysis_layer: Union[str, None],
    summary_file: Path,
    study: Study,
) -> Union[DownloadFile, None]:
    if analysis_source in EMG_CONFIG.rawreads_pipeline.taxonomy_analysis_sources:
        return DownloadFile(
            path=summary_file.relative_to(study.results_dir),
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            download_group=f"study_summary.{analysis_source}.{analysis_layer}",
            file_type=DownloadFileType.TSV,
            short_description=f"Summary of {analysis_source} taxonomies.",
            long_description=f"Summary of {analysis_source} taxonomic assignments, across all runs in the study.",
            alias=summary_file.name,
        )
    if (analysis_source in EMG_CONFIG.rawreads_pipeline.function_analysis_sources) and (
        analysis_layer is not None
    ):
        return DownloadFile(
            path=summary_file.relative_to(study.results_dir),
            download_type=DownloadType.FUNCTIONAL_ANALYSIS,
            download_group=f"study_summary.{analysis_source}.{analysis_layer}",
            file_type=DownloadFileType.TSV,
            short_description=f"Summary of {analysis_source} function {analysis_layer}.",
            long_description=f"Summary of {analysis_source} functional assignment {analysis_layer}, across all runs in the study.",
            alias=summary_file.name,
        )
    if analysis_source in {"multiqc"}:
        return DownloadFile(
            path=summary_file.relative_to(study.results_dir),
            file_type=DownloadFileType.HTML,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="study_summary.multiqc",
            short_description="Study MultiQC report",
            long_description="MultiQC webpage showing quality control steps and metrics for the whole study.",
            alias=summary_file.name,
        )


@task
def add_rawreads_study_summaries_to_downloads(mgnify_study_accession: str):
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)
    if not study.results_dir:
        logger.warning(
            f"Study {study} has no results_dir, so cannot add study summaries to downloads"
        )
        return

    logger.warning(
        f"Looking for study multiQC in {Path(study.results_dir) / EMG_CONFIG.rawreads_pipeline.study_multiqc_folder}."
    )

    study_summary_files = list(
        Path(study.results_dir).glob(f"{study.first_accession}*{STUDY_SUMMARY_TSV}")
    ) + list(
        Path(study.results_dir).glob(
            f"{EMG_CONFIG.rawreads_pipeline.study_multiqc_folder}/*{STUDY_MULTIQC_REPORT}"
        )
    )

    for summary_file in study_summary_files:
        analysis_source, analysis_layer = _get_analysis_source(summary_file)
        if analysis_source is None:
            logger.warning(
                f"Study {study} summary file {summary_file} has an unexpeced number of sections in its name"
            )
            continue

        download_file = _get_download_file(
            analysis_source, analysis_layer, summary_file, study
        )
        if download_file is None:
            logger.warning(
                f"Study {study} summary file {summary_file} is not from a recognised source ({analysis_source})"
            )
            continue
        try:
            study.add_download(download_file)
        except FileExistsError:
            logger.warning(
                f"File {summary_file} already exists in downloads list, skipping"
            )
        logger.info(f"Added {summary_file} to downloads of {study}")
    study.save()
    study.refresh_from_db()
    logger.info(
        f"Study download aliases are now {[d.alias for d in study.downloads_as_objects]}."
    )
