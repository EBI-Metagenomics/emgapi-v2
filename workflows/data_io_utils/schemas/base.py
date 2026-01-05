import gzip
import json
import logging
from pathlib import Path
from typing import List, Optional, Type

import pandas as pd
import pandera.pandas as pa
from pydantic import BaseModel, Field

from analyses.base_models.with_downloads_models import (
    DownloadFileMetadata,
    DownloadFileIndexFileMetadata,
    DownloadFileIndexFile,
)
from workflows.data_io_utils.file_rules.base_rules import (
    FileRule,
    DirectoryRule,
    GlobRule,
)
from workflows.data_io_utils.file_rules.common_rules import (
    FileExistsRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File
from .exceptions import PipelineValidationError

logger = logging.getLogger(__name__)


class ImportConfig(BaseModel):
    """
    Configuration for importing data from pipeline output files.

    This class defines how pipeline output files should be processed
    and imported into the analysis annotations.
    """

    annotations_key: str = Field(
        ..., description="Key for storing in analysis.annotations"
    )
    import_column: Optional[str] = Field(
        None,
        description="Specific column to import (if None, imports entire dataframe)",
    )
    import_as_records: bool = Field(
        True, description="Whether to import as list of records (dict) or just values"
    )


class PipelineFileSchema(BaseModel):
    """
    Unified schema for individual files within pipeline output directories.

    This class combines validation, download generation, and import functionality
    for pipeline output files using composition rather than inheritance.
    """

    filename_template: str = Field(
        ..., description="Template for filename with {identifier} placeholder"
    )
    validation_rules: List[FileRule] = Field(
        default_factory=list, description="Validation rules to apply to this file"
    )
    download_metadata: DownloadFileMetadata = Field(
        ..., description="Metadata for download file generation"
    )
    import_config: Optional[ImportConfig] = Field(
        None, description="Configuration for importing data from this file"
    )
    content_validator: Optional[Type[pa.DataFrameModel]] = Field(
        None, description="Pandera schema for validating file contents"
    )
    index_files: Optional[list[DownloadFileIndexFileMetadata]] = Field(
        None,
        description="List of index file types (e.g. a .gzi) that should exist for this file",
    )

    def get_filename(self, identifier: str) -> str:
        """
        Get the actual filename for a given identifier.

        :param identifier: The pipeline run identifier (e.g., assembly_id)
        :return: The actual filename
        """
        return self.filename_template.format(identifier=identifier)

    def validate_file(self, path: Path) -> tuple[bool, list[str]]:
        """
        Validate this file using its validation rules, content validator, and expected index files.

        File existence is determined by validation rules:
        - FileExistsRule = required file
        - FileIfExistsIsNotEmptyRule = optional file

        :param path: Path to the file to validate
        :return: Tuple of (success, list of error messages)
        """
        errors = []

        # File-level validation rules (existence, size, etc.)
        failed_rules = []
        for rule in self.validation_rules:
            try:
                if not rule.test(path):
                    failed_rules.append(rule.rule_name)
            except Exception as e:
                logging.error(f"Error applying rule {rule.rule_name} to {path}: {e}")
                failed_rules.append(f"{rule.rule_name} (error: {e})")

        if failed_rules:
            errors.append(f"File {path.name}: Failed rules: {', '.join(failed_rules)}")

        # Content validation using Pandera schema (only if file exists)
        if path.exists() and self.content_validator is not None:
            try:
                self._validate_content(path)
            except PipelineValidationError as e:
                errors.append(f"File {path.name}: {str(e)}")

        # Index file validation
        if self.index_files and FileExistsRule in self.validation_rules:
            for index_file_meta in self.index_files:
                index_file = DownloadFileIndexFile.from_indexed_file_path_and_metadata(
                    path, index_file_meta
                )
                if not Path(index_file.path).exists():
                    errors.append(
                        f"File {path.name}: Missing index file {index_file.path}"
                    )

        return len(errors) == 0, errors

    def _validate_content(self, path: Path) -> None:
        """
        Validate file contents using Pandera schema.

        Reads gzipped files on-the-fly without decompressing to disk.
        Automatically detects separator based on file extension:
        - .tsv / .tsv.gz → tab-separated
        - .csv / .csv.gz → comma-separated

        :param path: Path to the file to validate
        :raises PipelineValidationError: If content validation fails
        """
        try:
            # Determine separator from file extension (handle .gz files)
            if path.suffix == ".gz":
                # Get extension before .gz (e.g., .tsv from .tsv.gz)
                actual_extension = Path(path.stem).suffix
            else:
                # Get extension directly
                actual_extension = path.suffix

            separator = "\t" if actual_extension == ".tsv" else ","

            # Read file (handles gzip automatically)
            if path.suffix == ".gz":
                with gzip.open(path, "rt") as f:
                    df = pd.read_csv(f, sep=separator)
            else:
                df = pd.read_csv(path, sep=separator)

            # Validate using Pandera schema
            self.content_validator.validate(df, lazy=True)

            logger.info(
                f"Content validation successful for {path.name} ({len(df)} rows)"
            )

        except pa.errors.SchemaErrors as e:
            try:
                error_details = json.dumps(e.message, indent=2)
            except (TypeError, ValueError):
                error_details = str(e.message)
            error_msg = f"Content validation failed:\n{error_details}"
            logger.error(f"{path}: {error_msg}")
            raise PipelineValidationError(error_msg) from e

        except Exception as e:
            error_msg = f"Unexpected error validating content: {e}"
            logger.error(f"{path.name}: {error_msg}")
            raise PipelineValidationError(error_msg) from e


class PipelineDirectorySchema(BaseModel):
    """
    Unified schema for directories within pipeline output.

    This class handles directory-level validation and manages
    the files and subdirectories within pipeline output directories.
    """

    folder_name: str = Field(..., description="Name of the folder")
    external_folder_name: str = Field(
        None,
        description="External name of the folder (if different)",
        examples=["pretty-subdir/gff"],
    )
    files: List[PipelineFileSchema] = Field(
        default_factory=list, description="Files in this directory"
    )
    subdirectories: List["PipelineDirectorySchema"] = Field(
        default_factory=list, description="Subdirectories"
    )
    validation_rules: List[DirectoryRule] = Field(
        default_factory=list, description="Directory validation rules"
    )
    glob_rules: List[GlobRule] = Field(
        default_factory=list, description="Glob validation rules"
    )

    def validate_directory(
        self, base_path: Path, identifier: str
    ) -> tuple[Directory | None, list[str]]:
        """
        Validate this directory and return a validated Directory object.

        Directory existence is determined by validation rules:
        - DirectoryExistsRule in validation_rules = required directory

        :param base_path: Base path containing the pipeline results
        :param identifier: Pipeline run identifier
        :return: Tuple of (validated Directory object or None, list of error messages)
        """
        dir_path = base_path / self.folder_name
        errors = []

        # Prepare files for validation - create File objects
        files_to_validate = []
        for file_schema in self.files:
            filename = file_schema.get_filename(identifier)
            file_path = dir_path / filename
            # Create a File object with validation rules from schema
            try:
                file_obj = File(
                    path=file_path,
                    rules=file_schema.validation_rules,
                )
                files_to_validate.append(file_obj)
            except ValueError as e:
                # File validation failed - collect error
                errors.append(
                    f"Directory {self.folder_name}: File {filename} validation failed: {e}"
                )

        # Create a Directory object with validation
        validated_dir = None
        try:
            validated_dir = Directory(
                path=dir_path,
                files=files_to_validate,
                rules=self.validation_rules,
                glob_rules=self.glob_rules,
            )
        except ValueError as e:
            errors.append(
                f"Directory {self.folder_name}: Directory validation failed: {e}"
            )

        # Validate file contents using schema validators - collect all errors
        for file_schema in self.files:
            file_path = dir_path / file_schema.get_filename(identifier)
            success, file_errors = file_schema.validate_file(file_path)
            if not success:
                for error in file_errors:
                    errors.append(f"Directory {self.folder_name}: {error}")

        # Validate subdirectories recursively - collect all errors
        if validated_dir:
            for subdir_schema in self.subdirectories:
                sub_validated_dir, subdir_errors = subdir_schema.validate_directory(
                    dir_path, identifier
                )
                if subdir_errors:
                    errors.extend(subdir_errors)
                if sub_validated_dir:
                    validated_dir.files.append(sub_validated_dir)

        return validated_dir, errors


class PipelineResultSchema(BaseModel):
    """
    Top-level schema for complete pipeline result validation and import.

    This class coordinates the validation and import of entire pipeline
    output directories.
    """

    pipeline_name: str = Field(..., description="Name of the pipeline")
    pipeline_version: str = Field(..., description="Version of the pipeline")
    directories: List[PipelineDirectorySchema] = Field(
        default_factory=list, description="Directories in the pipeline output"
    )

    def __str__(self):
        return self.__class__.__name__

    def validate_results(self, base_path: Path, identifier: str) -> Directory:
        """
        Validate the complete pipeline results structure.

        :param base_path: Base path containing the pipeline results
        :param identifier: Pipeline run identifier
        :return: Validated Directory object representing the entire structure
        :raises PipelineValidationError: If validation fails with all collected errors
        """
        logger.info(f"Validating {self.pipeline_name} results at {base_path}")

        # Create the main directory structure
        main_dir = Directory(path=base_path / identifier)

        errors = []

        # Validate each directory and collect all errors
        for dir_schema in self.directories:
            validated_subdir, dir_errors = dir_schema.validate_directory(
                main_dir.path, identifier
            )
            if validated_subdir:
                main_dir.files.append(validated_subdir)
            if dir_errors:
                errors.extend(dir_errors)

        if errors:
            # Format errors for better readability
            error_count = len(errors)
            error_summary = "\n  - ".join(errors)
            error_message = (
                f"Pipeline validation failed for {self.pipeline_name} "
                f"with {error_count} error(s):\n  - {error_summary}"
            )
            raise PipelineValidationError(
                error_message,
                pipeline_name=self.pipeline_name,
                failed_rules=[],
            )

        logger.info(f"Successfully validated {self.pipeline_name} results")
        return main_dir


# Allow forward references
PipelineDirectorySchema.model_rebuild()
