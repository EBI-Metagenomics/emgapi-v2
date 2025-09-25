"""
Base schema classes for unified pipeline validation and import system.

This module provides the foundation classes that all pipeline-specific schemas
inherit from, implementing validation, download generation, and import functionality
using composition over inheritance.
"""

import logging
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field
from prefect import get_run_logger

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.file_rules.base_rules import (
    FileRule,
    DirectoryRule,
    GlobRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory
from .exceptions import PipelineValidationError, PipelineImportError
import analyses.models


class DownloadFileMetadata(BaseModel):
    """
    Metadata configuration for generating DownloadFile objects.

    This class separates download metadata from validation logic,
    providing a clean way to configure how pipeline outputs should
    be exposed as downloadable files.
    """

    file_type: DownloadFileType = Field(..., description="Type of the download file")
    download_type: DownloadType = Field(..., description="Category of the download")
    download_group: str = Field(..., description="Group identifier for the download")
    short_description: str = Field(..., description="Brief description of the file")
    long_description: str = Field(..., description="Detailed description of the file")


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
    required: bool = Field(True, description="Whether this file is required to exist")

    def get_filename(self, identifier: str) -> str:
        """
        Get the actual filename for a given identifier.

        :param identifier: The pipeline run identifier (e.g., assembly_id)
        :return: The actual filename
        """
        return self.filename_template.format(identifier=identifier)

    def validate_file(self, path: Path) -> bool:
        """
        Validate this file using its validation rules.

        :param path: Path to the file to validate
        :return: True if validation passes
        :raises PipelineValidationError: If validation fails
        """
        if not path.exists() and self.required:
            raise PipelineValidationError(
                f"Required file {path} does not exist", failed_rules=["FileExists"]
            )

        if not path.exists() and not self.required:
            return True

        failed_rules = []
        for rule in self.validation_rules:
            try:
                if not rule.test(path):
                    failed_rules.append(rule.rule_name)
            except Exception as e:
                logging.error(f"Error applying rule {rule.rule_name} to {path}: {e}")
                failed_rules.append(rule.rule_name)

        if failed_rules:
            raise PipelineValidationError(
                f"Validation failed for {path}", failed_rules=failed_rules
            )

        return True


class PipelineDirectorySchema(BaseModel):
    """
    Unified schema for directories within pipeline output.

    This class handles directory-level validation and manages
    the files and subdirectories within pipeline output directories.
    """

    folder_name: str = Field(..., description="Name of the folder")
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
    required: bool = Field(True, description="Whether directory is required to exist")

    def validate_directory(self, base_path: Path, identifier: str) -> Directory:
        """
        Validate this directory and return a validated Directory object.

        :param base_path: Base path containing the pipeline results
        :param identifier: Pipeline run identifier
        :return: Validated Directory object
        :raises PipelineValidationError: If validation fails
        """
        dir_path = base_path / self.folder_name

        if not dir_path.exists() and self.required:
            raise PipelineValidationError(
                f"Required directory {dir_path} does not exist"
            )

        if not dir_path.exists() and not self.required:
            # Return empty directory for optional missing directories
            return Directory(path=dir_path)

        # Prepare files for validation
        files_to_validate = []
        for file_schema in self.files:
            filename = file_schema.get_filename(identifier)
            files_to_validate.append((filename, file_schema.validation_rules))

        # Create Directory object with validation
        try:
            validated_dir = Directory(
                path=dir_path,
                files=files_to_validate,
                rules=self.validation_rules,
                glob_rules=self.glob_rules,
            )
        except ValueError as e:
            raise PipelineValidationError(
                f"Directory validation failed for {dir_path}: {e}"
            )

        # Validate subdirectories recursively
        for subdir_schema in self.subdirectories:
            sub_validated_dir = subdir_schema.validate_directory(dir_path, identifier)
            validated_dir.files.append(sub_validated_dir)

        return validated_dir

    def import_directory_results(
        self, analysis: analyses.models.Analysis, base_path: Path
    ) -> None:
        """
        Import all results from this directory.

        :param analysis: The analysis object to update
        :param base_path: Base path containing the pipeline results
        """
        dir_path = base_path / self.folder_name

        if not dir_path.exists():
            if self.required:
                from .exceptions import PipelineImportError

                raise PipelineImportError(
                    f"Required directory {dir_path} not found for import"
                )
            return

        # Import files in this directory
        for file_schema in self.files:
            identifier = analysis.assembly.first_accession
            file_path = dir_path / file_schema.get_filename(identifier)
            analysis.import_from_pipeline_file_schema(file_schema, file_path)

        # Import from subdirectories recursively
        for subdir_schema in self.subdirectories:
            subdir_schema.import_directory_results(analysis, dir_path)


class PipelineResultSchema(BaseModel):
    """
    Top-level schema for complete pipeline result validation and import.

    This class coordinates the validation and import of entire pipeline
    output directories, providing a unified interface for all pipeline types.
    """

    pipeline_name: str = Field(..., description="Name of the pipeline")
    pipeline_version: str = Field(..., description="Version of the pipeline")
    directories: List[PipelineDirectorySchema] = Field(
        default_factory=list, description="Directories in the pipeline output"
    )

    def validate_results(self, base_path: Path, identifier: str) -> Directory:
        """
        Validate the complete pipeline results structure.

        :param base_path: Base path containing the pipeline results
        :param identifier: Pipeline run identifier
        :return: Validated Directory object representing the entire structure
        :raises PipelineValidationError: If validation fails
        """
        logger = get_run_logger() or logging.getLogger(__name__)
        logger.info(f"Validating {self.pipeline_name} results at {base_path}")

        # Create main directory structure
        main_dir = Directory(path=base_path / identifier)

        failed_validations = []

        # Validate each directory
        for dir_schema in self.directories:
            try:
                validated_subdir = dir_schema.validate_directory(
                    main_dir.path, identifier
                )
                main_dir.files.append(validated_subdir)
            except PipelineValidationError as e:
                failed_validations.append(f"{dir_schema.folder_name}: {e}")

        if failed_validations:
            raise PipelineValidationError(
                f"Pipeline validation failed for {self.pipeline_name}: "
                + "; ".join(failed_validations),
                pipeline_name=self.pipeline_name,
            )

        logger.info(f"Successfully validated {self.pipeline_name} results")
        return main_dir

    def import_results(
        self, analysis: analyses.models.Analysis, base_path: Path
    ) -> None:
        """
        Import all pipeline results into the analysis.

        :param analysis: The analysis object to update
        :param base_path: Base path containing the pipeline results
        """
        logger = get_run_logger() or logging.getLogger(__name__)
        logger.info(f"Importing {self.pipeline_name} results from {base_path}")

        identifier = analysis.assembly.first_accession

        # Import from each directory
        for dir_schema in self.directories:
            try:
                dir_schema.import_directory_results(analysis, base_path / identifier)
            except PipelineImportError as e:
                logger.error(f"Failed to import from {dir_schema.folder_name}: {e}")
                # Continue with other directories even if one fails

        logger.info(f"Completed importing {self.pipeline_name} results")

    def generate_downloads(
        self, analysis: analyses.models.Analysis, base_path: Path
    ) -> List[DownloadFile]:
        """
        Generate all DownloadFile objects for this pipeline's results.

        :param analysis: The analysis object
        :param base_path: Base path containing the pipeline results
        :return: List of DownloadFile objects
        """
        downloads = []
        identifier = analysis.assembly.first_accession

        for dir_schema in self.directories:
            downloads.extend(
                self._generate_directory_downloads(
                    analysis, base_path / identifier, dir_schema
                )
            )

        return downloads

    def _generate_directory_downloads(
        self,
        analysis: analyses.models.Analysis,
        base_path: Path,
        dir_schema: PipelineDirectorySchema,
    ) -> List[DownloadFile]:
        """
        Generate downloads for a specific directory.

        :param analysis: The analysis object
        :param base_path: Base path containing the pipeline results
        :param dir_schema: Directory schema to process
        :return: List of DownloadFile objects
        """
        downloads = []
        dir_path = base_path / dir_schema.folder_name

        if not dir_path.exists():
            return downloads

        # Generate downloads for files in this directory
        for file_schema in dir_schema.files:
            download = DownloadFile.from_pipeline_file_schema(
                file_schema, analysis, dir_path.parent
            )
            if download:
                downloads.append(download)

        # Generate downloads for subdirectories
        for subdir_schema in dir_schema.subdirectories:
            downloads.extend(
                self._generate_directory_downloads(analysis, dir_path, subdir_schema)
            )

        return downloads


# Allow forward references
PipelineDirectorySchema.model_rebuild()
