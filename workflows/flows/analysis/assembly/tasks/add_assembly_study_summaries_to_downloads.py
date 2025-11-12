from pathlib import Path

from prefect import task, get_run_logger

import activate_django_first  # noqa

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from analyses.models import Study
from workflows.data_io_utils.schemas.assembly import AssemblyStudySummary


@task
def add_assembly_study_summaries_to_downloads(
    mgnify_study_accession: str,
) -> int:
    """
    Add assembly study summary files to a study's download list.

    This task is idempotent - it will clear any existing study_summary downloads
    before adding new ones, ensuring it can be safely re-run.

    :param mgnify_study_accession: The study accession (e.g., MGYS00000001)
    :return: Number of summary files added
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)

    if not study.results_dir:
        logger.error(
            f"Study {study} has no results_dir, cannot add study summaries to downloads"
        )
        return 0

    # Find all study summary files matching the study accession
    summary_files = list(
        Path(study.results_dir).glob(f"{study.first_accession}*_study_summary.tsv")
    )

    if not summary_files:
        logger.error(f"No study summary files found in {study.results_dir}")
        return 0

    logger.info(
        f"Found {len(summary_files)} study summary files in {study.results_dir}"
    )

    # Clear existing downloads that match the aliases of files we're about to add
    # This makes the task idempotent by removing only what will be re-added
    file_aliases = {f.name for f in summary_files}
    original_count = len(study.downloads)
    study.downloads = [
        dl for dl in study.downloads if dl.get("alias") not in file_aliases
    ]

    removed_count = original_count - len(study.downloads)
    if removed_count > 0:
        logger.info(
            f"Cleared {removed_count} existing downloads with matching aliases for idempotent retry"
        )

    # Save to ensure a clean state before adding new downloads
    study.save()
    study.refresh_from_db()

    added_count = 0
    for summary_file in summary_files:
        matched_type = None
        for summary_type in AssemblyStudySummary.all_types():
            if summary_type.matches_file(summary_file):
                matched_type = summary_type
                break

        if matched_type is None:
            logger.warning(
                f"Could not match {summary_file.name} to a known assembly study summary type"
            )
            continue

        # Determine if this is taxonomy or functional based on a source
        is_taxonomy = matched_type.source == "taxonomy"

        download_file = DownloadFile(
            path=summary_file.relative_to(study.results_dir),
            download_type=(
                DownloadType.TAXONOMIC_ANALYSIS
                if is_taxonomy
                else DownloadType.FUNCTIONAL_ANALYSIS
            ),
            download_group=f"study_summary.{matched_type.source}",
            file_type=DownloadFileType.TSV,
            short_description=matched_type.short_description,
            long_description=matched_type.long_description,
            alias=summary_file.name,
        )

        study.add_download(download_file)
        logger.info(f"Added {summary_file.name} to downloads of {study}")
        added_count += 1

    study.save()
    logger.info(f"Added {added_count} study summaries.")

    return added_count
