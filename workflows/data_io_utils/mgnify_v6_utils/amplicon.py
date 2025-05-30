import json
import logging
from pathlib import Path
from typing import List, Literal, Optional

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
    GlobHasFilesCountRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    FileConformsToTaxonomyTSVSchemaRule,
    GlobOfQcFolderHasFastpAndMultiqc,
    GlobOfTaxonomyFolderHasHtmlAndMseqRule,
    GlobOfAsvFolderHasRegionFolders,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

EMG_CONFIG = settings.EMG_CONFIG

_TAXONOMY = analyses.models.Analysis.TAXONOMIES
_CLOSED_REFERENCE = analyses.models.Analysis.CLOSED_REFERENCE
_ASV = analyses.models.Analysis.ASV


class AmpliconV6TaxonomyFolderSchema(BaseModel):
    taxonomy_summary_folder_name: Path
    reference_type: Literal["asv", "closed_reference"]
    expect_krona: bool = Field(True)
    expect_mseq: bool = Field(True)
    expect_tsv: bool = Field(True)


RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES = {
    analyses.models.Analysis.TaxonomySources.SSU: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-SSU"),
        reference_type=_CLOSED_REFERENCE,
    ),
    analyses.models.Analysis.TaxonomySources.LSU: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-LSU"),
        reference_type=_CLOSED_REFERENCE,
    ),
    analyses.models.Analysis.TaxonomySources.PR2: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("PR2"), reference_type=_CLOSED_REFERENCE
    ),
    analyses.models.Analysis.TaxonomySources.UNITE: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("UNITE"), reference_type=_CLOSED_REFERENCE
    ),
    analyses.models.Analysis.TaxonomySources.ITS_ONE_DB: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("ITSoneDB"), reference_type=_CLOSED_REFERENCE
    ),
    analyses.models.Analysis.TaxonomySources.DADA2_PR2: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("DADA2-PR2"),
        expect_tsv=False,
        reference_type=_ASV,
    ),
    analyses.models.Analysis.TaxonomySources.DADA2_SILVA: AmpliconV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("DADA2-SILVA"),
        expect_tsv=False,
        reference_type=_ASV,
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
            tax_df = pd.read_csv(tax_tsv, sep=CSVDelimiter.TAB, usecols=[0, 1, 2])
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty taxonomy TSV at {tax_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    for read_count_column_possible_name in ["SSU", "PR2", "ITSonedb", "UNITE", "LSU"]:
        if read_count_column_possible_name in tax_df.columns:
            tax_df.rename(
                columns={read_count_column_possible_name: "count"}, inplace=True
            )
            break
    else:
        tax_df["count"] = 0

    tax_df["count"] = (
        pd.to_numeric(tax_df["count"], errors="coerce").fillna(1).astype(int)
    )

    tax_df = tax_df.rename(columns={"taxonomy": "organism"})
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
        glob_rules.append(GlobOfTaxonomyFolderHasHtmlAndMseqRule)

    tax_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder  # taxonomy-summary/
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
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.tsv",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToTaxonomyTSVSchemaRule,
            ],
        )

        tax_dir.files.append(tax_table)

    if schema.expect_krona:
        krona_files = list(tax_dir.path.glob(f"{analysis.run.first_accession}*.html"))
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
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.mseq",
            rules=[
                FileExistsRule,
            ],
        )
        tax_dir.files.append(mapseq)

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
                download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
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
                    download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
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
                download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} MAPseq output",
                long_description="MAPseq output table with taxonomic database hit details",
            )
        )

    analysis.save()


def import_qc(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    if not allow_non_exist:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder,  # qc/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfQcFolderHasFastpAndMultiqc],
        )
    else:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder  # qc/
        )

    if not qc_dir.path.is_dir():
        print(f"No qc dir at {qc_dir.path}. Nothing to import.")
        return

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
            short_description="MultiQC quality control report",
            long_description="MultiQC webpage showing quality control steps and metrics",
        )
    )
    analysis.save()

    fastp = File(
        path=qc_dir.path / f"{analysis.run.first_accession}.fastp.json",
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
        return

    qc_dir.files.append(fastp)

    with fastp.path.open("r") as fastp_reader:
        fastp_content = json.load(fastp_reader)
    fastp_summary = fastp_content.get("summary")

    if not fastp_summary:
        print(f"No fastp summary for {analysis.run.first_accession}.")
        return

    # TODO: a pydantic schema for this file would be nice... but will be very different for other pipelines
    analysis.quality_control = fastp_summary
    analysis.save()


def import_asv(analysis: analyses.models.Analysis, dir_for_analysis: Path):
    if not (dir_for_analysis / EMG_CONFIG.amplicon_pipeline.asv_folder).is_dir():
        print(f"No asv dir in {dir_for_analysis}. Nothing to import.")
        return

    asv_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.amplicon_pipeline.asv_folder,  # asv/
        rules=[DirectoryExistsRule],
        glob_rules=[
            GlobHasFilesCountRule[4:],  # stats, pr2+silva, sequences
            GlobOfAsvFolderHasRegionFolders,
        ],
    )

    dada2_stats_file = File(
        path=asv_dir.path / f"{analysis.run.first_accession}_dada2_stats.tsv",
        rules=[FileExistsRule, FileIsNotEmptyRule],
    )

    asv_dir.files.append(dada2_stats_file)

    analysis.add_download(
        DownloadFile(
            path=dada2_stats_file.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.TSV,
            alias=dada2_stats_file.path.name,
            download_group="asv.stats",
            download_type=DownloadType.QUALITY_CONTROL,
            short_description="Quality control statistics from dada2",
            long_description="Quality control statistics for read trimming from dada2",
            parent_identifier=analysis.accession,
        )
    )

    asv_counts_file_paths = asv_dir.path.glob(
        "*/*_asv_read_counts.tsv"
    )  # e.g. ./16S-V3-V4/SRR999_16S-V3-V4_asv_read_counts.tsv
    for region_read_counts_file_path in asv_counts_file_paths:
        fp = Path(region_read_counts_file_path)
        region = fp.parent.name
        analysis.add_download(
            DownloadFile(
                path=fp.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=fp.name,
                download_type=DownloadType.STATISTICS,
                download_group="asv.distribution",
                short_description=f"Read counts for {region} ASVs",
                long_description=f"Read counts for each amplicon sequence variant in the {region} region",
                parent_identifier=analysis.accession,
            )
        )

    asv_tax_file_paths = asv_dir.path.glob(
        "*DADA2*_asv_tax.tsv"
    )  # e.g. ./SRR999_DADA2-SILVA_asv_tax.tsv
    for dada2_tax_file_path in asv_tax_file_paths:
        fp = Path(dada2_tax_file_path)

        ref_db = "other"
        if "silva" in dada2_tax_file_path.name.lower():
            ref_db = "SILVA"
        elif "pr2" in dada2_tax_file_path.name.lower():
            ref_db = "PR2"
        if ref_db == "Other":
            # Just in case something changes in future
            logging.warning(
                f"Unknown reference DB for asv tax file {dada2_tax_file_path}."
            )

        analysis.add_download(
            DownloadFile(
                path=fp.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=dada2_tax_file_path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group="asv.distribution",
                short_description=f"{ref_db} taxonomic lineages assigned to ASVs",
                long_description=f"Taxonomic lineages for each amplicon sequence variant, from {ref_db}",
                parent_identifier=analysis.accession,
            )
        )

    asv_seqs = File(
        path=asv_dir.path / f"{analysis.run.first_accession}_asv_seqs.fasta",
        rules=[FileExistsRule],
    )
    asv_dir.files.append(asv_seqs)
    analysis.add_download(
        DownloadFile(
            path=asv_seqs.path,
            file_type=DownloadFileType.FASTA,
            alias=asv_seqs.path.name,
            download_type=DownloadType.SEQUENCE_DATA,
            download_group="asv.sequences",
            short_description="ASV sequences",
            long_description="FASTA sequences of the amplicon sequence variants",
            parent_identifier=analysis.accession,
        )
    )
