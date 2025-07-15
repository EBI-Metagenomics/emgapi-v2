import json
import logging
from pathlib import Path
from typing import List, Optional

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
    FileConformsToFunctionalTSVSchemaRule,
    GlobOfQcFolderHasFastp,
    GlobOfMultiqcFolderHasMultiqc,
    GlobOfDecontamFolderHasSummaryStats,
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

EMG_CONFIG = settings.EMG_CONFIG

_TAXONOMY = analyses.models.Analysis.TAXONOMIES
_FUNCTIONAL = analyses.models.Analysis.FUNCTIONAL_ANNOTATION


class RawReadsV6TaxonomyFolderSchema(BaseModel):
    taxonomy_summary_folder_name: Path
    expect_krona: bool = Field(True)
    expect_mseq: bool = Field(True)
    expect_tsv: bool = Field(True)
    expect_raw: bool = Field(False)


class RawReadsV6FunctionFolderSchema(BaseModel):
    function_summary_folder_name: Path
    expect_tsv: bool = Field(True)
    expect_raw: bool = Field(True)


RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES = {
    analyses.models.Analysis.TaxonomySources.MOTUS: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("mOTUs"),
        expect_mseq=False,
        expect_raw=True,
    ),
    analyses.models.Analysis.TaxonomySources.SSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-SSU"),
        expect_mseq=True,
        expect_raw=False,
    ),
    analyses.models.Analysis.TaxonomySources.LSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-LSU"),
        expect_mseq=True,
        expect_raw=False,
    ),
}


RESULT_SCHEMAS_FOR_FUNCTIONAL_SOURCES = {
    analyses.models.Analysis.FunctionalSources.PFAM: RawReadsV6FunctionFolderSchema(
        function_summary_folder_name=Path("Pfam-A"),
    ),
}


def get_annotations_from_tax_table(tax_table: File) -> (List[dict], Optional[int]):
    """
    Reads the taxonomy TSV table from tax_table.path

    :param tax_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of taxonomies (lineages) present along with their read count, and the total read count.
    """
    with tax_table.path.open("r") as tax_tsv:
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
        ";".join(r[list(tax_df.columns)[1:]]).strip(";") for _, r in tax_df.iterrows()
    ]
    tax_df["count"] = (
        pd.to_numeric(tax_df["Count"], errors="coerce").fillna(1).astype(int)
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
    if schema.expect_mseq and schema.expect_krona and not allow_non_exist:
        glob_rules.append(GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule)

    tax_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder  # taxonomy-summary/
        / schema.taxonomy_summary_folder_name,  # SILVA-SSU/
        rules=dir_rules,
        glob_rules=glob_rules,
    )

    if not tax_dir.path.is_dir():
        print(f"No tax dir at {tax_dir.path} – no {source} taxa to import.")
        return

    if schema.expect_tsv:
        tax_table = File(
            path=tax_dir.path
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.txt",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToRawReadsTaxonomyTSVSchemaRule,
            ],
        )

        tax_dir.files.append(tax_table)

    if schema.expect_krona:
        krona_files = list(
            tax_dir.path.glob(
                f"krona/{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.html"
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

    if schema.expect_mseq:
        mapseq = File(
            path=tax_dir.path
            / "mapseq"
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.mseq",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )
        tax_dir.files.append(mapseq)

    if schema.expect_raw:
        raw_out = File(
            path=tax_dir.path
            / "raw"
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.out",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )
        tax_dir.files.append(raw_out)

    # DOWNLOAD FILE IMPORTS
    if schema.expect_tsv:
        taxonomies_to_import, total_read_count = get_annotations_from_tax_table(
            tax_table
        )
        analysis_taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
        if not analysis_taxonomies:
            analysis_taxonomies = {}
        analysis_taxonomies[source] = taxonomies_to_import
        analysis.annotations[analysis.TAXONOMIES] = analysis_taxonomies

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
            )
        )

    if schema.expect_krona:
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
                )
            )

    if schema.expect_mseq:
        analysis.add_download(
            DownloadFile(
                path=mapseq.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=mapseq.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.closed_reference.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} MAPseq output",
                long_description="MAPseq output table with taxonomic database hit details",
            )
        )

    if schema.expect_raw:
        analysis.add_download(
            DownloadFile(
                path=raw_out.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=raw_out.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.closed_reference.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} raw mOTUs output",
                long_description="mOTUs output table with taxonomic database hit details",
            )
        )

    analysis.save()


def get_annotations_from_func_table(func_table: File) -> (List[dict], Optional[int]):
    """
    Reads the functional profile TSV table from func_table.path

    :param func_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of functions (genes) present along with their read count, and the total read count.
    """
    with func_table.path.open("r") as func_tsv:
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
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.rawreads_pipeline.function_summary_folder  # function-summary/
        / schema.function_summary_folder_name,  # Pfam-A/
        rules=dir_rules,
    )

    if not func_dir.path.is_dir():
        print(f"No func dir at {func_dir.path} – no {source} functions to import.")
        return

    if schema.expect_tsv:
        func_table = File(
            path=func_dir.path
            / f"{analysis.run.first_accession}_{schema.function_summary_folder_name}.txt",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToFunctionalTSVSchemaRule,
            ],
        )

        func_dir.files.append(func_table)

    if schema.expect_raw:
        raw_out = File(
            path=func_dir.path
            / "raw"
            / f"{analysis.run.first_accession}_{schema.function_summary_folder_name}.domtbl",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )

        func_dir.files.append(raw_out)

    # DOWNLOAD FILE IMPORTS
    if schema.expect_tsv:
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
            )
        )

    if schema.expect_raw:
        analysis.add_download(
            DownloadFile(
                path=raw_out.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=raw_out.path.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{_FUNCTIONAL}.{schema.function_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.function_summary_folder_name} raw HMMer output",
                long_description="HMMer output table with hit details",
            )
        )

    analysis.save()


def get_summary_numbers_from_samtools_stats_output(
    stats_file: File,
) -> tuple[dict[str, int], Optional[int]]:
    summary_numbers = {}
    with stats_file.path.open("r") as f:
        for line in f:
            if line[:2] == "SN":
                l_ = [v.strip() for v in line.split("\t")]
                k, v = l_[1:3]
                summary_numbers[k[:-1]] = int(v)

    read_count = (
        summary_numbers["raw total sequences"]
        if "raw total sequences" in summary_numbers
        else None
    )

    return summary_numbers, read_count


def import_qc(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    if not allow_non_exist:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.qc_folder,  # qc/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfQcFolderHasFastp],
        )
    else:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.qc_folder  # qc/
        )

    if not qc_dir.path.is_dir():
        print(f"No qc dir at {qc_dir.path}. Nothing to import.")

    fastp = File(
        path=qc_dir.path / "fastp" / f"{analysis.run.first_accession}_fastp.json",
        rules=(
            [
                FileExistsRule,
                FileIsNotEmptyRule,
            ]
            if not allow_non_exist
            else []
        ),
    )
    if not fastp.path.is_file():
        print(f"No fastp file for {analysis.run.first_accession}.")

    qc_dir.files.append(fastp)
    analysis.add_download(
        DownloadFile(
            path=fastp.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.JSON,
            alias=fastp.path.name,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="quality_control",
            parent_identifier=analysis.accession,
            short_description="FastP report",
            long_description="FastP output showing quality control metrics",
        )
    )

    with fastp.path.open("r") as fastp_reader:
        fastp_content = json.load(fastp_reader)
    fastp_summary = fastp_content.get("summary")

    if not allow_non_exist:
        decontam_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.decontam_folder,  # qc/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfDecontamFolderHasSummaryStats],
        )
    else:
        decontam_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.decontam_folder  # qc/
        )

    if not decontam_dir.path.is_dir():
        print(
            f"No decontamination dir for {analysis.run.first_accession} at {decontam_dir.path}. Nothing to import."
        )

    possible_files = {}
    for reads_set in ["all", "mapped", "unmapped"]:
        for decontam_db in ["host", "phix"]:
            for reads_type in ["short_read", "long_read"]:
                if (not reads_type == "short_read") and (decontam_db == "phix"):
                    continue
                possible_files[(decontam_db, reads_type, reads_set)] = (
                    decontam_dir.path
                    / decontam_db
                    / f"{analysis.run.first_accession}_{reads_type}_{decontam_db}_{reads_set}_summary_stats.txt"
                )

    present_files = {}
    for k, fp in possible_files.items():
        if not fp.exists():
            continue
        present_files[k] = File(
            path=fp,
            rules=(
                [
                    FileExistsRule,
                    FileIsNotEmptyRule,
                ]
                if not allow_non_exist
                else []
            ),
        )

    if not allow_non_exist and len(present_files) < 1:
        print(
            f"No decontamination stats files for {analysis.run.first_accession} at {decontam_dir.path}. Nothing to import."
        )

    decontam_summary = {}
    for (decontam_db, reads_type, reads_set), fp in present_files.items():
        _, read_count = get_summary_numbers_from_samtools_stats_output(fp)
        if read_count:
            decontam_summary[f"{reads_type}.{decontam_db}.{reads_set}"] = read_count

    if not allow_non_exist:
        multiqc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.multiqc_folder,  # multiqc/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfMultiqcFolderHasMultiqc],
        )
    else:
        multiqc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.rawreads_pipeline.multiqc_folder  # multiqc/
        )

    if not multiqc_dir.path.is_dir():
        print(f"No multiqc dir at {multiqc_dir.path}. Nothing to import.")

    multiqc = File(
        path=multiqc_dir.path / f"{analysis.run.first_accession}_multiqc_report.html",
        rules=[
            FileExistsRule,
        ],
    )
    multiqc_dir.files.append(multiqc)
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
        )
    )

    qc_summary = {}

    if fastp_summary:
        fastp_summary["fastp"] = fastp_summary
    else:
        print(f"No fastp QC summary for {analysis.run.first_accession}.")

    if decontam_summary:
        qc_summary["decontam"] = decontam_summary
    else:
        print(f"No decontam summary for {analysis.run.first_accession}.")

    if not qc_summary:
        print(f"No QC summary for {analysis.run.first_accession}.")
        return

    # TODO: a pydantic schema for this file would be nice... but will be very different for other pipelines
    analysis.quality_control = qc_summary
    analysis.save()
