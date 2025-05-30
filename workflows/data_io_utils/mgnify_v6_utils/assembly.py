import gzip
import logging
from pathlib import Path

import pandas as pd
from mgnify_pipelines_toolkit.analysis.assembly.study_summary_generator import (
    TAXONOMY_COLUMN_NAMES,
)
from pydantic import BaseModel, Field, ValidationError

from activate_django_first import EMG_CONFIG
import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
    DownloadFileIndexFile,
)
from workflows.data_io_utils.csv.csv_comment_handler import (
    move_file_pointer_past_comment_lines,
    CSVDelimiter,
)

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
    GlobHasFilesRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
    GlobOfFolderHasTsvGzAndIndex,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File


def import_qc(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    if not allow_non_exist:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/ERZ999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder,  # qc/
            rules=[DirectoryExistsRule],
        )
    else:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/ERZ999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder  # qc/
        )

    if not qc_dir.path.is_dir():
        print(f"No qc dir at {qc_dir.path}. Nothing to import.")
        return

    multiqc = File(
        path=qc_dir.path / "multiqc_report.html",
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


def get_annotations_from_tax_table(tax_table: File) -> (list[dict], int | None):
    """
    Reads the taxonomy TSV table from tax_table.path

    TSV format like:
    ```
    7	sk__Archaea	k__Thermoproteati	p__Nitrososphaerota	c__Nitrososphaeria	o__Nitrosopumilales	f__Nitrosopumilaceae	g__Nitrosopumilus	s__Candidatus Nitrosopumilus koreensis
    3	sk__Archaea	k__Thermoproteati	p__Nitrososphaerota	c__Nitrososphaeria	o__Nitrosotaleales	f__Nitrosotaleaceae	g__Nitrosotalea	s__Nitrosotalea devaniterrae
    98	sk__Bacteria
    ```

    :param tax_table: File whose property .path points at a TSV file (of a contig count and then multi-column nullable lineage parts)
    :return:  A records-oriented list of taxonomies and the total contig-annotation count.
    """
    # with tax_table.path.open("r") as tax_tsv:
    with gzip.open(tax_table.path, "rt") as tax_tsv:
        move_file_pointer_past_comment_lines(
            tax_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )  # comments not expected - defensive measure
        try:
            tax_df = pd.read_csv(
                tax_tsv, sep=CSVDelimiter.TAB, names=TAXONOMY_COLUMN_NAMES
            ).fillna("")
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty taxonomy TSV at {tax_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    # TODO: could also validate here with toolkit:schemas.shemas.TaxonRecord
    tax_df["organism"] = (
        tax_df[TAXONOMY_COLUMN_NAMES[1:]].agg(";".join, axis=1).str.strip(";")
    )
    tax_df.rename(columns={"Count": "count"}, inplace=True)

    tax_df["count"] = (
        pd.to_numeric(tax_df["count"], errors="coerce").fillna(1).astype(int)
    )

    tax_df: pd.DataFrame = tax_df[["organism", "count"]]

    return tax_df.to_dict(orient="records"), int(tax_df["count"].sum())


def import_taxonomy(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []

    glob_rules = []

    if not allow_non_exist:
        glob_rules.append(GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule)

    tax_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/ERZ999/
        / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder,  # taxonomy/
        rules=dir_rules,
        glob_rules=glob_rules,
    )

    if not tax_dir.path.is_dir():
        print(f"No tax dir at {tax_dir.path} – no taxa to import.")
        return

    tax_table = File(
        path=tax_dir.path / f"{analysis.assembly.first_accession}.krona.txt.gz",
        rules=[
            FileExistsRule,
            FileIsNotEmptyRule,
        ],
    )

    tax_dir.files.append(tax_table)

    krona_files = list(tax_dir.path.glob(f"{analysis.assembly.first_accession}*.html"))
    for krona_file in krona_files:
        krona = File(
            path=Path(krona_file),
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )
        tax_dir.files.append(krona)

    taxonomies_to_import, total_read_count = get_annotations_from_tax_table(tax_table)
    analysis_taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
    if not analysis_taxonomies:
        analysis_taxonomies = {}
    analysis_taxonomies[analysis.TaxonomySources.UNIREF] = taxonomies_to_import
    analysis.annotations[analysis.TAXONOMIES] = analysis_taxonomies
    analysis.save()
    analysis.refresh_from_db()

    analysis.add_download(
        DownloadFile(
            path=tax_table.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.TSV,
            alias=tax_dir.path.name,
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            download_group=f"{analysis.TAXONOMIES}.{analysis.CLOSED_REFERENCE}.{analysis.TaxonomySources.UNIREF}",
            parent_identifier=analysis.accession,
            short_description="UniRef90 contig taxonomy table",
            long_description="Table with contig counts for each taxonomic assignment",
        )
    )


class AssemblyV6FunctionalTableSchema(BaseModel):
    folder_name: str
    tsv_suffix: str
    expect_index: bool = Field(True)
    download_subgroup: str
    import_to_annotations_key: str | None = Field(None)
    import_from_column: str | None = Field(None)
    short_description: str
    long_description: str


FUNCTIONAL_TABLE_SCHEMAS = [
    AssemblyV6FunctionalTableSchema(
        folder_name="go",
        tsv_suffix="_go_summary.tsv.gz",
        expect_index=True,
        download_subgroup=analyses.models.Analysis.GO_TERMS,
        import_to_annotations_key=None,
        short_description="GO Term counts",
        long_description="Table with counts for each Gene Ontology (GO) Term found",
    ),
    AssemblyV6FunctionalTableSchema(
        folder_name="go",
        tsv_suffix="_goslim_summary.tsv.gz",
        expect_index=True,
        download_subgroup=analyses.models.Analysis.GO_SLIMS,
        import_to_annotations_key=analyses.models.Analysis.GO_SLIMS,
        import_from_column="go",
        short_description="GO-Slim Term counts",
        long_description="Table with counts for each Gene Ontology (GO)-Slim Term found",
    ),
    AssemblyV6FunctionalTableSchema(
        folder_name="interpro",
        tsv_suffix="_interpro_summary.tsv.gz",
        expect_index=True,
        download_subgroup=analyses.models.Analysis.INTERPRO_IDENTIFIERS,
        import_to_annotations_key=analyses.models.Analysis.INTERPRO_IDENTIFIERS,
        import_from_column="interpro_identifier",
        short_description="InterPro Identifier counts",
        long_description="Table with counts for each InterPro identifier found",
    ),
    AssemblyV6FunctionalTableSchema(
        folder_name="pfam",
        tsv_suffix="_pfam_summary.tsv.gz",
        expect_index=True,
        download_subgroup=analyses.models.Analysis.PFAMS,
        import_to_annotations_key=None,
        short_description="Pfam accession counts",
        long_description="Table with counts for each Pfam accession found",
    ),
    AssemblyV6FunctionalTableSchema(
        folder_name="rhea-reactions",
        tsv_suffix="_proteins2rhea.tsv.gz",
        expect_index=True,
        download_subgroup=analyses.models.Analysis.RHEA_REACTIONS,
        import_to_annotations_key=None,
        short_description="Rhea reaction counts",
        long_description="Table with counts of each Rhea reaction found",
    ),
]


def import_functions(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []
    functional_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/ERZ999/
        / EMG_CONFIG.assembly_analysis_pipeline.functional_folder,  # functional-annotation/
        rules=dir_rules,
    )

    for schema in FUNCTIONAL_TABLE_SCHEMAS:
        try:
            annot_dir = Directory(
                path=functional_dir.path / schema.folder_name,
                rules=[DirectoryExistsRule],
            )
        except ValidationError:
            logging.warning(
                f"No functional-annotation dir at {functional_dir.path / schema.folder_name}. Nothing to import."
            )
            continue

        try:
            if schema.expect_index:
                annot_dir.glob_rules = [GlobOfFolderHasTsvGzAndIndex]
            else:
                annot_dir.glob_rules = [GlobHasFilesRule]
        except ValidationError as e:
            logging.warning(
                f"Validation failure for TSV files at {annot_dir.path}: {e}. Skipping."
            )
            continue

        annot_tsv = (
            annot_dir.path / f"{analysis.assembly.first_accession}{schema.tsv_suffix}"
        )

        if not schema.expect_index:
            index = None
        else:
            index = DownloadFileIndexFile(
                path=annot_tsv.with_suffix(".gz.gzi").relative_to(analysis.results_dir),
                index_type="gzi",
            )

        analysis.add_download(
            DownloadFile(
                path=annot_tsv.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=annot_tsv.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{schema.download_subgroup}",
                parent_identifier=analysis.accession,
                short_description=schema.short_description,
                long_description=schema.long_description,
                index_file=index,
            )
        )

        if schema.import_to_annotations_key:
            try:
                df = pd.read_csv(annot_tsv, sep=CSVDelimiter.TAB)
            except pd.errors.EmptyDataError:
                logging.error(
                    f"Found empty functional annotations TSV at {annot_tsv}. Probably unit testing, otherwise we should never be here"
                )
            else:
                analysis.annotations[schema.import_to_annotations_key] = df[
                    schema.import_from_column
                ].to_list()
                analysis.save()
