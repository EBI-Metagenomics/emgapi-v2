import json
import gzip
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from django.conf import settings
from pydantic import BaseModel, Field

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.csv.csv_comment_handler import (
    CSVDelimiter,
    move_file_pointer_past_comment_lines,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    FileConformsToRawReadsTaxonomyTSVSchemaRule,
    FileConformsToRawReadsMotusTaxonomyTSVSchemaRule,
    FileConformsToFunctionalTSVSchemaRule,
    GlobOfRawReadsQcFolderHasFastpAndMultiqc,
    GlobOfTaxonomyFolderHasTxtGzRule,
    GlobOfTaxonomyFolderHasKronaHtmlRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

EMG_CONFIG = settings.EMG_CONFIG

_TAXONOMY = analyses.models.Analysis.TAXONOMIES
_FUNCTIONAL = analyses.models.Analysis.FUNCTIONAL_ANNOTATION


class RawReadsV6TaxonomyFolderSchema(BaseModel):
    taxonomy_summary_folder_name: Path
    expect_krona: bool = Field(True)
    expect_txt: bool = Field(True)


class RawReadsV6FunctionFolderSchema(BaseModel):
    function_summary_folder_name: Path
    expect_txt: bool = Field(True)
    expect_stats: bool = Field(True)


RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES = {
    analyses.models.Analysis.TaxonomySources.MOTUS: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("motus"), expect_krona=True, expect_txt=True
    ),
    analyses.models.Analysis.TaxonomySources.SSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("silva-ssu"),
        expect_krona=True,
        expect_txt=True,
    ),
    analyses.models.Analysis.TaxonomySources.LSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("silva-lsu"),
        expect_krona=True,
        expect_txt=True,
    ),
}


RESULT_SCHEMAS_FOR_FUNCTIONAL_SOURCES = {
    analyses.models.Analysis.FunctionalSources.PFAM: RawReadsV6FunctionFolderSchema(
        function_summary_folder_name=Path("pfam"), expect_stats=True, expect_txt=True
    ),
}


def get_annotations_from_tax_table(tax_table: File) -> Tuple[List[dict], Optional[int]]:
    """
    Reads the taxonomy TSV table from tax_table.path

    :param tax_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of taxonomies (lineages) present along with their read count, and the total read count.
    """
    if tax_table.path.name[-3:] == ".gz":
        file_opener = gzip.open(tax_table.path, "rt")
    else:
        file_opener = tax_table.path.open("r")
    with file_opener as tax_tsv:
        move_file_pointer_past_comment_lines(
            tax_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )
        try:
            tax_df = pd.read_csv(tax_tsv, sep=CSVDelimiter.TAB)
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty taxonomy TSV at {tax_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    tax_df["organism"] = [
        ";".join([v for v in r[list(tax_df.columns)[1:]] if isinstance(v, str)]).strip(
            ";"
        )
        for _, r in tax_df.iterrows()
    ]
    tax_df["count"] = (
        pd.to_numeric(tax_df["Count"], errors="coerce").fillna(0).astype(int)
    )

    tax_df: pd.DataFrame = tax_df[["organism", "count"]]

    return tax_df.to_dict(orient="records"), int(tax_df["count"].sum())


def import_taxonomy(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    source: analyses.models.Analysis.TaxonomySources,
    allow_non_exist: bool = True,
):
    schema = RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES.get(source)

    if not schema:
        raise NotImplementedError(
            f"There is no support for importing {source} annotations because a directory structure is not known for those."
        )

    # FILE CHECKS
    # TODO: dedupe this with sanity check flow
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []
    glob_rules = []
    if schema.expect_krona and not allow_non_exist:
        glob_rules.append(GlobOfTaxonomyFolderHasKronaHtmlRule)
    if schema.expect_txt and not allow_non_exist:
        glob_rules.append(GlobOfTaxonomyFolderHasTxtGzRule)

    tax_dir = Directory(
        path=dir_for_analysis
        / EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder
        / schema.taxonomy_summary_folder_name,
        rules=dir_rules,
        glob_rules=glob_rules,
    )

    if not tax_dir.path.is_dir():
        print(f"No tax dir at {tax_dir.path} – no {source} taxa to import.")
        return

    if schema.expect_txt:
        if source in {analyses.models.Analysis.TaxonomySources.MOTUS}:
            file_contents_rule = FileConformsToRawReadsMotusTaxonomyTSVSchemaRule
        else:
            file_contents_rule = FileConformsToRawReadsTaxonomyTSVSchemaRule
        tax_table = File(
            path=tax_dir.path
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.txt.gz",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                file_contents_rule,
            ],
        )

        tax_dir.files.append(tax_table)

        taxonomies_to_import, total_read_count = get_annotations_from_tax_table(
            tax_table
        )
        analysis_taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
        if not analysis_taxonomies:
            analysis_taxonomies = {}
        analysis_taxonomies[source] = taxonomies_to_import
        analysis.annotations[analysis.TAXONOMIES] = analysis_taxonomies

        # DOWNLOAD FILE IMPORTS
        analysis.add_download(
            DownloadFile(
                path=tax_table.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=tax_dir.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.closed_reference.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} taxonomy table",
                long_description="Table with read counts for each taxonomic assignment",
                file_size_bytes=None,
                index_file=None,
            )
        )

    if schema.expect_krona:
        krona_files = list(
            tax_dir.path.glob(
                f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.html"
            )
        )
        for krona_file in krona_files:
            krona = File(
                path=Path(krona_file),
                rules=[
                    FileExistsRule,
                    FileIsNotEmptyRule,
                ],
            )
            tax_dir.files.append(krona)

        # DOWNLOAD FILE IMPORTS
        for krona in krona_files:
            analysis.add_download(
                DownloadFile(
                    path=Path(krona).relative_to(analysis.results_dir),
                    file_type=DownloadFileType.HTML,
                    alias=f"{Path(krona).stem}_{schema.taxonomy_summary_folder_name}{Path(krona).suffix}",
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=f"{_TAXONOMY}.closed_reference.{schema.taxonomy_summary_folder_name}",
                    parent_identifier=analysis.accession,
                    short_description=f"{schema.taxonomy_summary_folder_name} Krona plot",
                    long_description=f"Krona plot webpage showing taxonomic assignments from {schema.taxonomy_summary_folder_name} annotation",
                    file_size_bytes=None,
                    index_file=None,
                )
            )

    analysis.save()


def get_annotations_from_func_table(
    func_table: File,
) -> Tuple[List[dict], List[dict], List[dict], Optional[int]]:
    """
    Reads the functional profile TSV table from func_table.path

    :param func_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of functions (genes) present along with their read count, and the total read count.
    """
    if func_table.path.name[-3:] == ".gz":
        file_opener = gzip.open(func_table.path, "rt")
    else:
        file_opener = func_table.path.open("r")
    with file_opener as func_tsv:
        move_file_pointer_past_comment_lines(
            func_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )
        try:
            func_df = pd.read_csv(func_tsv, sep=CSVDelimiter.TAB)
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty functional profile TSV at {func_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    func_df["read_count"] = (
        pd.to_numeric(func_df["read_count"], errors="coerce").fillna(1).astype(int)
    )
    func_df["coverage_depth"] = (
        pd.to_numeric(func_df["coverage_depth"], errors="coerce")
        .fillna(0)
        .astype(float)
    )
    func_df["coverage_breadth"] = (
        pd.to_numeric(func_df["coverage_breadth"], errors="coerce")
        .fillna(0)
        .astype(float)
    )

    count_df: pd.DataFrame = func_df[["function", "read_count"]]
    depth_df: pd.DataFrame = func_df[["function", "coverage_depth"]]
    breadth_df: pd.DataFrame = func_df[["function", "coverage_breadth"]]

    return (
        count_df.to_dict(orient="records"),
        depth_df.to_dict(orient="records"),
        breadth_df.to_dict(orient="records"),
        int(count_df["read_count"].sum()),
    )


def import_functional(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    source: analyses.models.Analysis.FunctionalSources,
    allow_non_exist: bool = True,
):
    schema = RESULT_SCHEMAS_FOR_FUNCTIONAL_SOURCES.get(source)

    if not schema:
        raise NotImplementedError(
            f"There is no support for importing {source} annotations because a directory structure is not known for those."
        )

    # FILE CHECKS
    # TODO: dedupe this with sanity check flow
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []

    func_dir = Directory(
        path=dir_for_analysis
        / EMG_CONFIG.rawreads_pipeline.function_summary_folder
        / schema.function_summary_folder_name,
        rules=dir_rules,
    )

    if not func_dir.path.is_dir():
        print(f"No func dir at {func_dir.path} – no {source} functions to import.")
        return

    if schema.expect_txt:
        func_table = File(
            path=func_dir.path
            / f"{analysis.run.first_accession}_{schema.function_summary_folder_name}.txt.gz",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToFunctionalTSVSchemaRule,
            ],
        )

        func_dir.files.append(func_table)

        # DOWNLOAD FILE IMPORTS
        (
            functions_count_to_import,
            functions_depth_to_import,
            functions_breadth_to_import,
            total_read_count,
        ) = get_annotations_from_func_table(func_table)
        functions_to_import = {
            "read_count": functions_count_to_import,
            "coverage_depth": functions_depth_to_import,
            "coverage_breadth": functions_breadth_to_import,
        }
        analysis_functions = analysis.annotations.get(
            analysis.FUNCTIONAL_ANNOTATION, {}
        )
        if not analysis_functions:
            analysis_functions = {}
        analysis_functions[source] = functions_to_import
        analysis.annotations[analysis.FUNCTIONAL_ANNOTATION] = analysis_functions

        analysis.add_download(
            DownloadFile(
                path=func_table.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=func_dir.path.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{_FUNCTIONAL}.{schema.function_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.function_summary_folder_name} functional profile table",
                long_description="Table with read counts for each functional assignment",
                file_size_bytes=None,
                index_file=None,
            )
        )

    if schema.expect_stats:
        stats_out = File(
            path=func_dir.path
            / f"{analysis.run.first_accession}_{schema.function_summary_folder_name}.stats.json",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )

        func_dir.files.append(stats_out)

        # DOWNLOAD FILE IMPORTS
        analysis.add_download(
            DownloadFile(
                path=stats_out.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.JSON,
                alias=stats_out.path.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{_FUNCTIONAL}.{schema.function_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.function_summary_folder_name} summary statistics for functional profiling.",
                long_description=f"{schema.function_summary_folder_name} summary statistics for functional profiling.",
                file_size_bytes=None,
                index_file=None,
            )
        )

    analysis.save()


def import_qc(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    rules = []
    glob_rules = []
    if not allow_non_exist:
        rules.append(DirectoryExistsRule)
        glob_rules.append(GlobOfRawReadsQcFolderHasFastpAndMultiqc)

    qc_dir = Directory(
        path=dir_for_analysis / EMG_CONFIG.rawreads_pipeline.qc_folder,
        rules=rules,
        glob_rules=glob_rules,
    )

    if not qc_dir.path.is_dir():
        print(f"No qc dir at {qc_dir.path}. Nothing to import.")

    qc_fastp = File(
        path=qc_dir.path / f"{analysis.run.first_accession}_qc.fastp.json",
        rules=(
            [
                FileExistsRule,
                FileIsNotEmptyRule,
            ]
            if not allow_non_exist
            else []
        ),
    )
    if not qc_fastp.path.is_file():
        print(f"No QC fastp file for {analysis.run.first_accession}.")

    qc_dir.files.append(qc_fastp)
    analysis.add_download(
        DownloadFile(
            path=qc_fastp.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.JSON,
            alias=qc_fastp.path.name,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="quality_control",
            parent_identifier=analysis.accession,
            short_description="FastP report",
            long_description="FastP output showing quality control metrics",
            file_size_bytes=None,
            index_file=None,
        )
    )

    with qc_fastp.path.open("r") as fastp_reader:
        qc_fastp_content = json.load(fastp_reader)
    qc_fastp_summary = qc_fastp_content.get("summary")

    decontam_fastp = File(
        path=qc_dir.path / f"{analysis.run.first_accession}_decontamination.fastp.json",
        rules=(
            [
                FileExistsRule,
                FileIsNotEmptyRule,
            ]
            if not allow_non_exist
            else []
        ),
    )
    if not decontam_fastp.path.is_file():
        print(f"No decontamination fastp file for {analysis.run.first_accession}.")

    qc_dir.files.append(decontam_fastp)
    analysis.add_download(
        DownloadFile(
            path=decontam_fastp.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.JSON,
            alias=decontam_fastp.path.name,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="quality_control",
            parent_identifier=analysis.accession,
            short_description="FastP post-decontamination report",
            long_description="FastP output showing post-decontamination metrics",
            file_size_bytes=None,
            index_file=None,
        )
    )

    with decontam_fastp.path.open("r") as fastp_reader:
        decontam_fastp_content = json.load(fastp_reader)
    decontam_fastp_summary = decontam_fastp_content.get("summary")

    multiqc = File(
        path=qc_dir.path / f"{analysis.run.first_accession}_multiqc_report.html",
        rules=[
            FileExistsRule,
        ],
    )
    qc_dir.files.append(multiqc)
    analysis.add_download(
        DownloadFile(
            path=multiqc.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.HTML,
            alias=multiqc.path.name,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="quality_control",
            parent_identifier=analysis.accession,
            short_description="MultiQC report",
            long_description="MultiQC webpage showing quality control and decontamination steps and metrics",
            file_size_bytes=None,
            index_file=None,
        )
    )

    qc_summary = {}

    if qc_fastp_summary:
        qc_summary["fastp"] = qc_fastp_summary
    else:
        print(f"No fastp QC summary for {analysis.run.first_accession}.")

    if decontam_fastp_summary:
        qc_summary["decontam"] = decontam_fastp_summary
    else:
        print(f"No decontam summary for {analysis.run.first_accession}.")

    if not qc_summary:
        print(f"No QC summary for {analysis.run.first_accession}.")
        return

    # TODO: a pydantic schema for this file would be nice... but will be very different for other pipelines
    analysis.quality_control = qc_summary
    analysis.save()
