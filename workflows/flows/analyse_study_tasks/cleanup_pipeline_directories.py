import shutil
from pathlib import Path
from prefect import task, get_run_logger
from typing import List, Union
import analyses.models
import glob
import hashlib

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task()
def delete_pipeline_workdir(workdir: Path):
    # The pipeline uses a directory as a work directory. This should be deleted on completion.
    # This function deletes the directory passed to it.
    shutil.rmtree(workdir)


@task()
def delete_study_nextflow_workdir(
    study_workdir: Path,
    analyses_to_attempt: List[Union[str, int]],
):
    logger = get_run_logger()

    # check for failed analyses first AnalysisStates.ANALYSIS_FAILED
    failed_analyses = (
        analyses.models.Analysis.objects.filter(
            id__in=analyses_to_attempt,
            run__metadata__fastq_ftps__isnull=False,
            status__contains={AnalysisStates.ANALYSIS_FAILED: True},
        )
        .order_by("id")
        .values_list("id", flat=True)
    )

    # delete work directory
    if failed_analyses:
        logger.warning(
            f"Detected some analyses failed, not deleting Nextflow work directory {study_workdir}"
        )
    else:
        logger.info(f"Deleting Nextflow work directory {study_workdir}")
        shutil.rmtree(study_workdir)


@task()
def delete_study_results_dir(
    study: analyses.models.Study,
):
    logger = get_run_logger()

    if not study.results_dir:
        logger.warning(f"Study {study} has no results dir, skipping")
        return

    if not study.external_results_dir:
        logger.warning(f"Study {study} has no external results dir, skipping")
        return

    # check that contents of results_dir are in the external location
    def getmd5(fp):
        return hashlib.md5(open(fp, "rb").read()).hexdigest()

    allowed_extensions = {
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "json",
        "gz",
        "fasta",
        "csv",
        "deoverlapped",
    }
    source_files = {
        Path(fp).name: getmd5(fp)
        for fp in glob.glob(str(Path(study.results_dir) / "**/*"), recursive=True)
        if Path(fp).name.split(".")[-1] in allowed_extensions
    }
    dest_files = {
        Path(fp).name: getmd5(fp)
        for fp in glob.glob(
            str(Path(study.external_results_dir) / "**/*"), recursive=True
        )
        if Path(fp).name.split(".")[-1] in allowed_extensions
    }
    if not all(
        [
            ((k in dest_files) and (dest_files[k] == md5))
            for k, md5 in source_files.items()
        ]
    ):
        logger.warning(
            f"Study results directory contents not all found in external results directory ({study.external_results_dir}). Not deleting {study.results_dir}."
        )
        return

    # delete work directory
    logger.info(f"Deleting study results directory {study.results_dir}")
    shutil.rmtree(study.results_dir)
