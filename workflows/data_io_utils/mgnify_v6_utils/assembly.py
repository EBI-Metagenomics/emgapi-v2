"""
Unified schema-based validation and import system for assembly analysis results.

This module provides a unified approach to both validate pipeline output directories
and import their contents into the database. It extends the existing Pydantic-based
validation patterns with import capabilities.
"""

import gzip
import logging
import pandas as pd
from pathlib import Path
from typing import List, Optional, Union

from prefect import get_run_logger
from pydantic import BaseModel, Field

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
from workflows.data_io_utils.file_rules.base_rules import (
    FileRule,
    DirectoryRule,
    GlobRule,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
    FileIfExistsIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, create_directory
from mgnify_pipelines_toolkit.analysis.assembly.study_summary_generator import (
    TAXONOMY_COLUMN_NAMES,
)


class AssemblyFileSchema(BaseModel):
    """Schema for individual files within an assembly analysis directory."""

    filename_template: str = Field(
        ..., description="Template for filename with {assembly_id} placeholder"
    )
    file_type: DownloadFileType = Field(
        ..., description="Type of file for download system"
    )
    download_type: DownloadType = Field(..., description="Category of download")
    download_group: str = Field(..., description="Group identifier for downloads")
    short_description: str = Field(..., description="Short description of file")
    long_description: str = Field(..., description="Long description of file")
    validation_rules: List[Union[FileRule, DirectoryRule]] = Field(
        default_factory=list, description="List of validation rule names"
    )
    required: bool = Field(True, description="Whether file is required to exist")
    import_to_annotations_key: Optional[str] = Field(
        None, description="Key for importing to annotations"
    )
    import_from_column: Optional[str] = Field(None, description="Column to import from")

    def get_filename(self, assembly_id: str) -> str:
        """Get the actual filename for a given assembly ID."""
        return self.filename_template.format(assembly_id=assembly_id)

    def import_annotations(
        self, analysis: "analyses.models.Analysis", file_path: Path
    ) -> None:
        """Import annotations from this file into the analysis."""
        if not self.import_to_annotations_key:
            return

        try:
            df = pd.read_csv(file_path, sep=CSVDelimiter.TAB)
            # Store the entire dataframe as a list of dictionaries
            analysis.annotations[self.import_to_annotations_key] = df.to_dict(
                orient="records"
            )
            analysis.save()
        except pd.errors.EmptyDataError:
            logging.error(f"Found empty annotation TSV at {file_path}")
        except Exception as e:
            logging.error(f"Failed to import annotations from {file_path}: {e}")


class AssemblyDirectorySchema(BaseModel):
    """Schema for a directory within an assembly analysis output."""

    folder_name: str = Field(..., description="Name of the folder")
    files: List[AssemblyFileSchema] = Field(
        default_factory=list, description="Files in this directory"
    )
    subdirectories: List["AssemblyDirectorySchema"] = Field(
        default_factory=list, description="Subdirectories"
    )
    validation_rules: List[Union[FileRule, DirectoryRule]] = Field(
        default_factory=list, description="Directory validation rules"
    )
    glob_rules: List[GlobRule] = Field(
        default_factory=list, description="Glob validation rules"
    )
    required: bool = Field(True, description="Whether directory is required to exist")


class AssemblyAnnotationTableSchema(BaseModel):
    """Enhanced schema for annotation tables with validation and import capabilities."""

    folder_name: str = Field(..., description="Name of the folder containing the table")
    tsv_suffix: str = Field(..., description="Suffix for the TSV file")
    expect_index: bool = Field(True, description="Whether to expect a .gzi index file")
    download_subgroup: str = Field(..., description="Subgroup for downloads")
    import_to_annotations_key: Optional[str] = Field(
        None, description="Key for importing to annotations"
    )
    import_from_column: Optional[str] = Field(None, description="Column to import from")
    short_description: str = Field(..., description="Short description")
    long_description: str = Field(..., description="Long description")
    validation_rules: List[Union[FileRule, DirectoryRule]] = Field(
        default_factory=list, description="File validation rules"
    )
    required: bool = Field(True, description="Whether table is required")

    def get_filename(self, assembly_id: str) -> str:
        """Get the TSV filename for a given assembly ID."""
        return f"{assembly_id}{self.tsv_suffix}"

    def create_download_file(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> DownloadFile:
        """Create a DownloadFile object for this annotation table."""
        tsv_path = (
            base_path
            / self.folder_name
            / self.get_filename(analysis.assembly.first_accession)
        )

        index_file = None
        if self.expect_index:
            index_file = DownloadFileIndexFile(
                path=tsv_path.with_suffix(".gz.gzi").relative_to(analysis.results_dir),
                index_type="gzi",
            )

        return DownloadFile(
            path=tsv_path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.TSV,
            alias=tsv_path.name,
            download_type=DownloadType.FUNCTIONAL_ANALYSIS,
            download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{self.download_subgroup}",
            parent_identifier=analysis.accession,
            short_description=self.short_description,
            long_description=self.long_description,
            index_file=index_file,
        )

    def import_annotations(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import annotations from this table into the analysis."""
        if not self.import_to_annotations_key or not self.import_from_column:
            return

        tsv_path = (
            base_path
            / self.folder_name
            / self.get_filename(analysis.assembly.first_accession)
        )

        try:
            df = pd.read_csv(tsv_path, sep=CSVDelimiter.TAB)
            analysis.annotations[self.import_to_annotations_key] = df[
                self.import_from_column
            ].to_list()
            analysis.save()
        except pd.errors.EmptyDataError:
            logging.error(f"Found empty annotation TSV at {tsv_path}")
        except Exception as e:
            logging.error(f"Failed to import annotations from {tsv_path}: {e}")


class AssemblyAnalysisDirectorySchema(BaseModel):
    """Unified schema for assembly analysis directory validation and import."""

    qc_directory: AssemblyDirectorySchema
    cds_directory: AssemblyDirectorySchema
    taxonomy_directory: AssemblyDirectorySchema
    functional_annotation_directory: AssemblyDirectorySchema
    pathways_systems_directory: AssemblyDirectorySchema
    annotation_summary_directory: AssemblyDirectorySchema

    def validate_directory_structure(
        self, base_path: Path, assembly_id: str
    ) -> Directory:
        """Validate the complete directory structure and return the validated Directory object."""
        # Create the main directory
        main_dir = create_directory(base_path / assembly_id)

        # Validate each subdirectory
        for dir_schema in [
            self.qc_directory,
            self.cds_directory,
            self.taxonomy_directory,
            self.functional_annotation_directory,
            self.pathways_systems_directory,
            self.annotation_summary_directory,
        ]:
            validated_subdir = self._validate_subdirectory(
                main_dir.path, dir_schema, assembly_id
            )
            main_dir.files.append(validated_subdir)

        return main_dir

    def _validate_subdirectory(
        self, base_path: Path, dir_schema: AssemblyDirectorySchema, assembly_id: str
    ) -> Directory:
        """Validate a single subdirectory based on its schema."""
        subdir_path = base_path / dir_schema.folder_name

        # Prepare files for validation
        files_to_validate = []
        for file_schema in dir_schema.files:
            filename = file_schema.get_filename(assembly_id)
            if file_schema.required:
                files_to_validate.append((filename, file_schema.validation_rules))

        # Create the directory with validation
        validated_dir = create_directory(
            subdir_path,
            files=files_to_validate,
            rules=dir_schema.validation_rules,
            glob_rules=dir_schema.glob_rules,
        )

        # Handle subdirectories recursively
        for subdir_schema in dir_schema.subdirectories:
            sub_validated_dir = self._validate_subdirectory(
                subdir_path, subdir_schema, assembly_id
            )
            validated_dir.files.append(sub_validated_dir)

        return validated_dir

    def import_analysis_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import all analysis results from the validated directory structure."""

        logger = get_run_logger()

        logger.info(f"Importing results from {base_path}")

        logger.info("Importing QC results...")
        self._import_qc_results(analysis, base_path)

        logger.info("Importing CDS results...")
        self._import_cds_results(analysis, base_path)

        logger.info("Importing Taxonomy results...")
        self._import_taxonomy_results(analysis, base_path)

        logger.info("Importing Functional annotation results...")
        self._import_functional_results(analysis, base_path)

        logger.info("Importing Pathways and systems results...")
        self._import_pathways_results(analysis, base_path)

    def _import_qc_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import QC results."""
        qc_path = (
            base_path
            / analysis.assembly.first_accession
            / self.qc_directory.folder_name
        )

        if not qc_path.exists():
            logging.info(f"No QC directory at {qc_path}")
            return

        # Import MultiQC report
        for file_schema in self.qc_directory.files:
            if "multiqc_report.html" in file_schema.filename_template:
                download_file = DownloadFile(
                    path=Path(
                        qc_path
                        / file_schema.get_filename(analysis.assembly.first_accession)
                    ).relative_to(analysis.results_dir),
                    file_type=file_schema.file_type,
                    alias=file_schema.get_filename(analysis.assembly.first_accession),
                    download_type=file_schema.download_type,
                    download_group=file_schema.download_group,
                    parent_identifier=analysis.accession,
                    short_description=file_schema.short_description,
                    long_description=file_schema.long_description,
                )
                analysis.add_download(download_file)

        analysis.save()

    def _import_cds_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import CDS results."""
        cds_path = (
            base_path
            / analysis.assembly.first_accession
            / self.cds_directory.folder_name
        )

        if not cds_path.exists():
            logging.info(f"No CDS directory at {cds_path}")
            return

        for file_schema in self.cds_directory.files:
            download_file = DownloadFile(
                path=Path(
                    cds_path
                    / file_schema.get_filename(analysis.assembly.first_accession)
                ).relative_to(analysis.results_dir),
                file_type=file_schema.file_type,
                alias=file_schema.get_filename(analysis.assembly.first_accession),
                download_type=file_schema.download_type,
                download_group=file_schema.download_group,
                parent_identifier=analysis.accession,
                short_description=file_schema.short_description,
                long_description=file_schema.long_description,
            )
            analysis.add_download(download_file)

        analysis.save()

    def _import_taxonomy_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import taxonomy results."""
        tax_path = (
            base_path
            / analysis.assembly.first_accession
            / self.taxonomy_directory.folder_name
        )

        if not tax_path.exists():
            logging.info(f"No taxonomy directory at {tax_path}")
            return

        # Import Krona file and parse taxonomies
        krona_file = tax_path / f"{analysis.assembly.first_accession}.krona.txt.gz"
        if krona_file.exists():
            taxonomies_to_import, total_read_count = (
                self._get_annotations_from_tax_table(krona_file)
            )
            analysis_taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
            if not analysis_taxonomies:
                analysis_taxonomies = {}
            analysis_taxonomies[analysis.TaxonomySources.UNIREF] = taxonomies_to_import
            analysis.annotations[analysis.TAXONOMIES] = analysis_taxonomies

            # Add download
            download_file = DownloadFile(
                path=krona_file.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=krona_file.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{analysis.TAXONOMIES}.{analysis.CLOSED_REFERENCE}.{analysis.TaxonomySources.UNIREF}",
                parent_identifier=analysis.accession,
                short_description="UniRef90 contig taxonomy table",
                long_description="Table with contig counts for each taxonomic assignment",
            )
            analysis.add_download(download_file)

        analysis.save()

    def _import_functional_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import functional annotation results using the annotation table schemas."""
        func_path = (
            base_path
            / analysis.assembly.first_accession
            / self.functional_annotation_directory.folder_name
        )

        if not func_path.exists():
            raise ValueError(f"No functional annotation directory at {func_path}")

        # Process each functional annotation subdirectory
        for subdir_schema in self.functional_annotation_directory.subdirectories:
            print(f"Subdir {subdir_schema.folder_name}")
            subdir_path = func_path / subdir_schema.folder_name
            if not subdir_path.exists():
                continue

            print(f"Subdir {subdir_schema.folder_name} exists, importing files... ")

            # Import files in this subdirectory
            for file_schema in subdir_schema.files:
                file_path = subdir_path / file_schema.get_filename(
                    analysis.assembly.first_accession
                )
                if file_path.exists():
                    download_file = DownloadFile(
                        path=file_path.relative_to(analysis.results_dir),
                        file_type=file_schema.file_type,
                        alias=file_path.name,
                        download_type=file_schema.download_type,
                        download_group=file_schema.download_group,
                        parent_identifier=analysis.accession,
                        short_description=file_schema.short_description,
                        long_description=file_schema.long_description,
                    )
                    analysis.add_download(download_file)

                    logging.info(f"{file_path.name}")

                    # Import annotations if the schema has import_to_annotations_key and import_from_column
                    if file_schema.import_to_annotations_key:
                        file_schema.import_annotations(analysis, file_path)

        analysis.save()

    def _import_pathways_results(
        self, analysis: "analyses.models.Analysis", base_path: Path
    ) -> None:
        """Import pathways and systems results."""
        pathways_path = (
            base_path
            / analysis.assembly.first_accession
            / self.pathways_systems_directory.folder_name
        )

        if not pathways_path.exists():
            logging.info(f"No pathways directory at {pathways_path}")
            return

        # Process each pathway subdirectory
        for subdir_schema in self.pathways_systems_directory.subdirectories:
            subdir_path = pathways_path / subdir_schema.folder_name
            if not subdir_path.exists():
                continue

            # Import files in this subdirectory
            for file_schema in subdir_schema.files:
                file_path = subdir_path / file_schema.get_filename(
                    analysis.assembly.first_accession
                )
                if file_path.exists():
                    download_file = DownloadFile(
                        path=file_path.relative_to(analysis.results_dir),
                        file_type=file_schema.file_type,
                        alias=file_path.name,
                        download_type=file_schema.download_type,
                        download_group=file_schema.download_group,
                        parent_identifier=analysis.accession,
                        short_description=file_schema.short_description,
                        long_description=file_schema.long_description,
                    )
                    analysis.add_download(download_file)

                    # Import annotations if the schema has import_to_annotations_key and import_from_column
                    if (
                        hasattr(file_schema, "import_to_annotations_key")
                        and file_schema.import_to_annotations_key
                    ):
                        file_schema.import_annotations(analysis, file_path)

        analysis.save()

    def _get_annotations_from_tax_table(
        self, tax_table_path: Path
    ) -> tuple[list[dict], int]:
        """Parse taxonomy table and return annotations."""
        try:
            with gzip.open(tax_table_path, "rt") as tax_tsv:
                move_file_pointer_past_comment_lines(
                    tax_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
                )
                tax_df = pd.read_csv(
                    tax_tsv, sep=CSVDelimiter.TAB, names=TAXONOMY_COLUMN_NAMES
                ).fillna("")
        except pd.errors.EmptyDataError:
            logging.error(f"Found empty taxonomy TSV at {tax_table_path}")
            return [], 0

        tax_df["organism"] = (
            tax_df[TAXONOMY_COLUMN_NAMES[1:]].agg(";".join, axis=1).str.strip(";")
        )
        tax_df.rename(columns={"Count": "count"}, inplace=True)
        tax_df["count"] = (
            pd.to_numeric(tax_df["count"], errors="coerce").fillna(1).astype(int)
        )
        tax_df = tax_df[["organism", "count"]]

        return tax_df.to_dict(orient="records"), int(tax_df["count"].sum())


def create_assembly_v6_schema() -> AssemblyAnalysisDirectorySchema:
    """Create the default assembly analysis schema for v6 pipeline."""

    # QC Directory
    qc_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.qc_folder,
        validation_rules=[DirectoryExistsRule],
        files=[
            AssemblyFileSchema(
                filename_template="{assembly_id}_filtered_contigs.fasta.gz",
                file_type=DownloadFileType.FASTA,
                download_type=DownloadType.QUALITY_CONTROL,
                download_group="quality_control",
                short_description="Filtered contigs FASTA file",
                long_description="Filtered contigs FASTA file used in downstream pipelines",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="{assembly_id}.tsv",
                file_type=DownloadFileType.TSV,
                download_type=DownloadType.QUALITY_CONTROL,
                download_group="quality_control",
                short_description="Assembly QC metrics",
                long_description="Quality control metrics for the assembly",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="multiqc_report.html",
                file_type=DownloadFileType.HTML,
                download_type=DownloadType.QUALITY_CONTROL,
                download_group="quality_control",
                short_description="MultiQC quality control report",
                long_description="MultiQC webpage showing quality control steps and metrics",
                validation_rules=[FileExistsRule],
            ),
        ],
        subdirectories=[
            AssemblyDirectorySchema(
                folder_name="multiqc_data",
                validation_rules=[DirectoryExistsRule],
            )
        ],
    )

    # CDS Directory
    cds_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.cds_folder,
        validation_rules=[DirectoryExistsRule],
        files=[
            AssemblyFileSchema(
                filename_template="{assembly_id}_predicted_orf.ffn.gz",
                file_type=DownloadFileType.FASTA,
                download_type=DownloadType.CODING_SEQUENCES,
                download_group=analyses.models.Analysis.CODING_SEQUENCES,
                short_description="Predicted ORF nucleotide sequences",
                long_description="Predicted ORF nucleotide sequences",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="{assembly_id}_predicted_cds.faa.gz",
                file_type=DownloadFileType.FASTA,
                download_type=DownloadType.CODING_SEQUENCES,
                download_group=analyses.models.Analysis.CODING_SEQUENCES,
                short_description="Predicted CDS proteins",
                long_description="Predicted CDS proteins",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="{assembly_id}_predicted_cds.gff.gz",
                file_type=DownloadFileType.GFF,
                download_type=DownloadType.CODING_SEQUENCES,
                download_group=analyses.models.Analysis.CODING_SEQUENCES,
                short_description="Predicted CDS",
                long_description="Predicted CDS annotations",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
        ],
    )

    # Taxonomy Directory
    taxonomy_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder,
        validation_rules=[DirectoryExistsRule],
        glob_rules=[GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule],
        files=[
            AssemblyFileSchema(
                filename_template="{assembly_id}_contigs_taxonomy.tsv.gz",
                file_type=DownloadFileType.TSV,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group="taxonomy",
                short_description="Contig taxonomy table",
                long_description="Taxonomic assignments for contigs",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="{assembly_id}.krona.txt.gz",
                file_type=DownloadFileType.TSV,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group="taxonomy",
                short_description="Krona taxonomy table",
                long_description="Taxonomy table for Krona visualization",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
            AssemblyFileSchema(
                filename_template="{assembly_id}.html",
                file_type=DownloadFileType.HTML,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group="taxonomy",
                short_description="Krona taxonomy visualization",
                long_description="Interactive Krona taxonomy chart",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
        ],
    )

    # Functional Annotation Directory with subdirectories
    functional_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder,
        validation_rules=[DirectoryExistsRule],
        subdirectories=[
            # InterPro subdirectory
            AssemblyDirectorySchema(
                folder_name="interpro",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_interproscan.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.interpro",
                        short_description="InterProScan results",
                        long_description="InterProScan functional annotation results",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_interpro_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.INTERPRO_IDENTIFIERS}",
                        short_description="InterPro Identifier counts",
                        long_description="Table with counts for each InterPro identifier found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.INTERPRO_IDENTIFIERS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_interpro_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.INTERPRO_IDENTIFIERS}",
                        short_description="InterPro summary index",
                        long_description="Index file for InterPro summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # Pfam subdirectory
            AssemblyDirectorySchema(
                folder_name="pfam",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_pfam_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.PFAMS}",
                        short_description="Pfam accession counts",
                        long_description="Table with counts for each Pfam accession found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.PFAMS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_pfam_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.PFAMS}",
                        short_description="Pfam summary index",
                        long_description="Index file for Pfam summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # GO subdirectory
            AssemblyDirectorySchema(
                folder_name="go",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_go_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.GO_TERMS}",
                        short_description="GO Term counts",
                        long_description="Table with counts for each Gene Ontology (GO) Term found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.GO_TERMS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_go_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.GO_TERMS}",
                        short_description="GO summary index",
                        long_description="Index file for GO summary",
                        validation_rules=[FileExistsRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_goslim_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.GO_SLIMS}",
                        short_description="GO-Slim Term counts",
                        long_description="Table with counts for each Gene Ontology (GO)-Slim Term found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.GO_SLIMS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_goslim_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.GO_SLIMS}",
                        short_description="GO-Slim summary index",
                        long_description="Index file for GO-Slim summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # EggNOG subdirectory
            AssemblyDirectorySchema(
                folder_name="eggnog",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_emapper_seed_orthologs.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.eggnog",
                        short_description="EggNOG seed orthologs",
                        long_description="EggNOG seed ortholog assignments",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_emapper_annotations.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.eggnog",
                        short_description="EggNOG annotations",
                        long_description="EggNOG functional annotations",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                ],
            ),
            # KEGG subdirectory
            AssemblyDirectorySchema(
                folder_name="kegg",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_ko_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg",
                        short_description="KEGG KO summary",
                        long_description="KEGG Orthology assignments summary",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_ko_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg",
                        short_description="KEGG KO summary index",
                        long_description="Index file for KEGG KO summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # Rhea subdirectory
            AssemblyDirectorySchema(
                folder_name="rhea-reactions",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_proteins2rhea.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.RHEA_REACTIONS}",
                        short_description="Rhea reaction counts",
                        long_description="Table with counts of each Rhea reaction found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.RHEA_REACTIONS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_proteins2rhea.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.RHEA_REACTIONS}",
                        short_description="Rhea reactions index",
                        long_description="Index file for Rhea reactions",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # dbCAN subdirectory
            AssemblyDirectorySchema(
                folder_name="dbcan",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dbcan_cgc.gff.gz",
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                        short_description="dbCAN CGC annotations",
                        long_description="dbCAN carbohydrate-active enzyme gene cluster annotations",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dbcan_standard_out.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                        short_description="dbCAN standard output",
                        long_description="dbCAN standard analysis output",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dbcan_overview.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                        short_description="dbCAN overview",
                        long_description="dbCAN analysis overview",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dbcan_sub_hmm.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                        short_description="dbCAN sub-HMM results",
                        long_description="dbCAN sub-HMM analysis results",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dbcan_substrates.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                        short_description="dbCAN substrates",
                        long_description="dbCAN predicted substrates",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                ],
            ),
        ],
    )

    # Pathways and Systems Directory with subdirectories
    pathways_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder,
        validation_rules=[DirectoryExistsRule],
        subdirectories=[
            # antiSMASH subdirectory
            AssemblyDirectorySchema(
                folder_name="antismash",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_antismash.gbk.gz",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.antismash",
                        short_description="antiSMASH GenBank output",
                        long_description="antiSMASH biosynthetic gene cluster predictions in GenBank format",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_antismash.gff.gz",
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.antismash",
                        short_description="antiSMASH GFF output",
                        long_description="antiSMASH biosynthetic gene cluster predictions in GFF format",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_antismash_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS}",
                        short_description="antiSMASH BGC counts",
                        long_description="Table with counts for each BGC found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_antismash_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS}",
                        short_description="antiSMASH summary index",
                        long_description="Index file for antiSMASH summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # SanntiS subdirectory
            AssemblyDirectorySchema(
                folder_name="sanntis",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_sanntis_concatenated.gff.gz",
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.sanntis",
                        short_description="SanntiS GFF output",
                        long_description="SanntiS biosynthetic gene cluster predictions",
                        validation_rules=[FileIfExistsIsNotEmptyRule],
                        required=False,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_sanntis_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.SANNTIS_GENE_CLUSTERS}",
                        short_description="SanntiS BGC counts",
                        long_description="Table with counts for each BGC found",
                        validation_rules=[FileIfExistsIsNotEmptyRule],
                        required=False,
                        import_to_annotations_key=analyses.models.Analysis.SANNTIS_GENE_CLUSTERS,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_sanntis_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.SANNTIS_GENE_CLUSTERS}",
                        short_description="SanntiS summary index",
                        long_description="Index file for SanntiS summary",
                        validation_rules=[FileIfExistsIsNotEmptyRule],
                        required=False,
                    ),
                ],
            ),
            # Genome Properties subdirectory
            AssemblyDirectorySchema(
                folder_name="genome-properties",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_gp.json.gz",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.genome_properties",
                        short_description="Genome Properties JSON",
                        long_description="Genome Properties results in JSON format",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_gp.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.GENOME_PROPERTIES}",
                        short_description="Genome Properties counts",
                        long_description="Table with counts for each Genome Property found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.GENOME_PROPERTIES,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_gp.txt.gz",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.genome_properties",
                        short_description="Genome Properties text output",
                        long_description="Genome Properties results in text format",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                ],
            ),
            # KEGG Modules subdirectory
            AssemblyDirectorySchema(
                folder_name="kegg-modules",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_kegg_modules_per_contigs.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg_modules",
                        short_description="KEGG Modules per contig",
                        long_description="KEGG Modules found per contig",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_kegg_modules_per_contigs.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg_modules",
                        short_description="KEGG Modules per contig index",
                        long_description="Index file for KEGG Modules per contig",
                        validation_rules=[FileExistsRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_kegg_modules_summary.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.KEGG_MODULES}",
                        short_description="KEGG Modules counts",
                        long_description="Table with counts for each KEGG Module found",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                        import_to_annotations_key=analyses.models.Analysis.KEGG_MODULES,
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_kegg_modules_summary.tsv.gz.gzi",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.{analyses.models.Analysis.KEGG_MODULES}",
                        short_description="KEGG Modules summary index",
                        long_description="Index file for KEGG Modules summary",
                        validation_rules=[FileExistsRule],
                    ),
                ],
            ),
            # DRAM Distill subdirectory
            AssemblyDirectorySchema(
                folder_name="dram-distill",
                validation_rules=[DirectoryExistsRule],
                files=[
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dram.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                        short_description="DRAM Distill results",
                        long_description="Table with DRAM Distill results",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_dram.html.gz",
                        file_type=DownloadFileType.HTML,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                        short_description="DRAM Distill HTML report",
                        long_description="DRAM Distill HTML visualization",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_genome_stats.tsv.gz",
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                        short_description="DRAM genome statistics",
                        long_description="DRAM genome statistics table",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                    AssemblyFileSchema(
                        filename_template="{assembly_id}_metabolism_summary.xlsx.gz",
                        file_type=DownloadFileType.OTHER,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                        short_description="DRAM metabolism summary",
                        long_description="DRAM metabolism summary spreadsheet",
                        validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    ),
                ],
            ),
        ],
    )

    # Annotation Summary Directory
    annotation_summary_dir = AssemblyDirectorySchema(
        folder_name=EMG_CONFIG.assembly_analysis_pipeline.annotation_summary_folder,
        validation_rules=[DirectoryExistsRule],
        files=[
            AssemblyFileSchema(
                filename_template="{assembly_id}_annotation_summary.gff.gz",
                file_type=DownloadFileType.GFF,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group="annotation_summary",
                short_description="Annotation summary GFF",
                long_description="Comprehensive annotation summary in GFF format",
                validation_rules=[FileExistsRule, FileIsNotEmptyRule],
            ),
        ],
    )

    return AssemblyAnalysisDirectorySchema(
        qc_directory=qc_dir,
        cds_directory=cds_dir,
        taxonomy_directory=taxonomy_dir,
        functional_annotation_directory=functional_dir,
        pathways_systems_directory=pathways_dir,
        annotation_summary_directory=annotation_summary_dir,
    )
