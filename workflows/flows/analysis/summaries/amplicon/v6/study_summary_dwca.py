from __future__ import annotations

from pathlib import Path

import pandas as pd

from analyses.base_models.with_downloads_models import DownloadFileType
from analyses.models import Study
from workflows.flows.analysis.summaries.amplicon.shared.dwca.writer import (
    write_amplicon_taxonomy_dwca_archive,
)

DWCA_SUFFIX = ".dwca.zip"


def write_grouped_dwca_archives(
    df: pd.DataFrame,
    study: Study,
    output_dir: Path,
    output_prefix: str,
) -> list[Path]:
    """Write grouped ASV and closed-reference Darwin Core Archives.

    :param df: Merged canonical study dataframe.
    :param study: Study used to populate EML metadata.
    :param output_dir: Directory where public outputs should be written.
    :param output_prefix: Common filename prefix for public outputs.
    :return: Paths to generated Darwin Core Archive zip files.
    """
    paths: list[Path] = []
    asv_df = df.loc[df["analysis_method"] == "asv"]
    asv_groups = asv_df[["reference_database", "amplified_region"]].drop_duplicates()
    for _, group in asv_groups.iterrows():
        reference_database = group["reference_database"]
        amplified_region = group["amplified_region"]
        path = output_dir / (
            f"{output_prefix}_asv_{filename_token(reference_database)}"
            f"_{filename_token(amplified_region)}{DWCA_SUFFIX}"
        )
        write_amplicon_taxonomy_dwca_archive(
            df,
            study,
            path,
            analysis_method="asv",
            reference_database=reference_database,
            amplified_region=amplified_region,
        )
        paths.append(path)

    closed_reference_df = df.loc[df["analysis_method"] == "closed_reference"]
    closed_reference_dbs = closed_reference_df["reference_database"].dropna().unique()
    for reference_database in sorted(closed_reference_dbs):
        path = output_dir / (
            f"{output_prefix}_closed_reference_{filename_token(reference_database)}"
            f"{DWCA_SUFFIX}"
        )
        write_amplicon_taxonomy_dwca_archive(
            df,
            study,
            path,
            analysis_method="closed_reference",
            reference_database=reference_database,
        )
        paths.append(path)

    return paths


def dwca_download_entries(
    output_dir: Path,
    output_prefix: str,
    pipeline_version: str,
) -> list[tuple[Path, DownloadFileType, str, str, str]]:
    """Build download metadata entries for generated Darwin Core Archives.

    :param output_dir: Directory containing public study-level outputs.
    :param output_prefix: Common filename prefix for public outputs.
    :param pipeline_version: Amplicon pipeline version used in download groups.
    :return: Download metadata tuple entries for generated DwCA archives.
    """
    entries = []
    for path in sorted(output_dir.glob(f"{output_prefix}_asv_*{DWCA_SUFFIX}")):
        entries.append(
            (
                path,
                DownloadFileType.ZIP,
                f"study_summary.{pipeline_version}.amplicon.dwca.asv",
                "Amplicon ASV Darwin Core Archive",
                "Study-level amplicon ASV taxonomic occurrences as a Darwin Core Archive.",
            )
        )
    for path in sorted(
        output_dir.glob(f"{output_prefix}_closed_reference_*{DWCA_SUFFIX}")
    ):
        entries.append(
            (
                path,
                DownloadFileType.ZIP,
                f"study_summary.{pipeline_version}.amplicon.dwca.closed_reference",
                "Amplicon closed-reference Darwin Core Archive",
                "Study-level amplicon closed-reference taxonomic occurrences as a Darwin Core Archive.",
            )
        )
    return entries


def filename_token(value: object) -> str:
    """Normalize a dataframe value for use in public filenames."""
    return str(value).replace("/", "-").replace(" ", "_")
