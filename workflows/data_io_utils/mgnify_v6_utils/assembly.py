import logging
from pathlib import Path

import pandas as pd

import analyses.models
from analyses.base_models.with_downloads_models import DownloadFile
from workflows.data_io_utils.csv.csv_comment_handler import CSVDelimiter
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
)

logger = logging.getLogger(__name__)


class BasePipelineResultImporter:
    """
    Base class for importing pipeline results into Analysis models.

    This class provides a common interface for pipeline result importers,
    separating filesystem operations from Django model logic.

    This base class still needs some work to be useful for amplicon and rawreads.
    I've just created
    """

    def __init__(self, analysis: analyses.models.Analysis):
        """
        Initialize the importer with an Analysis instance.

        :param analysis: The Analysis model instance to import results into
        :type analysis: analyses.models.Analysis
        """
        self.analysis = analysis

    def import_results(
        self, schema, base_path: Path, validate_first: bool = True
    ) -> int:
        """
        Import complete pipeline results into the analysis.

        This method must be implemented by subclasses to handle
        pipeline-specific import logic.

        :param schema: Pipeline result schema instance (PipelineResultSchema)
        :param base_path: Path to pipeline results directory
        :type base_path: Path
        :param validate_first: Whether to validate structure before import
        :type validate_first: bool
        :return: Number of downloads generated
        :rtype: int
        :raises NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement import_results method")


class AssemblyResultImporter(BasePipelineResultImporter):
    """
    Handles importing assembly analysis results into Analysis models.

    This includes ASA, MAP and VIRify.

    This importer coordinates validation, annotation import, and download
    generation for assembly analysis pipelines.
    """

    def import_results(
        self, schema, base_path: Path, validate_first: bool = True
    ) -> int:
        """
        Complete assembly analysis results importer.

        This is the central method that coordinates:
        - Structure validation (optional)
        - Annotation import
        - Download generation

        :param schema: Pipeline result schema instance (PipelineResultSchema)
        :param base_path: Path to pipeline results directory
        :type base_path: Path
        :param validate_first: Whether to validate structure before import
        :type validate_first: bool
        :return: Number of downloads generated
        :rtype: int
        """
        logger.info(f"Importing pipeline results from {base_path}")

        # Import annotations - raises exception on failure
        identifier = self.analysis.assembly.first_accession

        # Validation (optional) - raises exception on failure
        if validate_first:
            schema.validate_results(base_path, identifier)

        # Walk through schema directories and import files directly
        for dir_schema in schema.directories:
            self._import_from_directory(dir_schema, base_path / identifier, identifier)

        # Generate downloads - count successful ones
        downloads_generated = self._generate_downloads_from_schema(
            schema, base_path, identifier
        )

        self.analysis.save()

        logger.info(
            f"Successfully imported pipeline results with {downloads_generated} downloads"
        )
        return downloads_generated

    def import_from_pipeline_file_schema(
        self,
        schema,
        file_path: Path,
    ) -> None:
        """
        Import data from a pipeline file schema into this analysis.

        :param schema: The pipeline file schema with import configuration (PipelineFileSchema)
        :param file_path: Path to the file to import
        :type file_path: Path
        """
        df = pd.read_csv(file_path, sep=CSVDelimiter.TAB)

        if schema.import_config.import_column:
            data = df[schema.import_config.import_column].to_list()
        else:
            if schema.import_config.import_as_records:
                data = df.to_dict(orient="records")
            else:
                data = df.to_dict()

        self.analysis.annotations[schema.import_config.annotations_key] = data
        self.analysis.save()

    def _import_from_directory(self, dir_schema, base_path: Path, identifier: str):
        """
        Import annotations from a directory using its schema.

        :param dir_schema: PipelineDirectorySchema defining the directory structure
        :param base_path: Base path containing the directory
        :type base_path: Path
        :param identifier: Pipeline run identifier
        :type identifier: str
        :raises ValueError: If required files are missing
        """
        dir_path = base_path / dir_schema.folder_name

        if not dir_path.exists():
            # Check if a directory is required based on validation rules
            # TODO: this should be part of the validation itself and not this conditional
            if DirectoryExistsRule in dir_schema.validation_rules:
                # TODO: Should we define custom exception?
                raise ValueError(f"Required directory {dir_path} not found")
            return

        # Import files directly from the schema
        for file_schema in dir_schema.files:
            if file_schema.import_config:  # Only import files that have import config
                file_path = dir_path / file_schema.get_filename(identifier)
                # TODO: this should be part of the validation itself and not this conditional
                if file_path.exists():
                    self.import_from_pipeline_file_schema(file_schema, file_path)
                else:
                    # Check if a file is required based on validation rules
                    if FileExistsRule in file_schema.validation_rules:
                        raise ValueError(f"Required file {file_path} not found")

        # Recurse into subdirectories
        for subdir_schema in dir_schema.subdirectories:
            self._import_from_directory(subdir_schema, dir_path, identifier)

    def _generate_downloads_from_schema(
        self, schema, base_path: Path, identifier: str
    ) -> int:
        """
        Generate downloads from the pipeline schema.

        :param schema: PipelineResultSchema defining the pipeline results
        :param base_path: Base path containing the pipeline results
        :type base_path: Path
        :param identifier: Pipeline run identifier
        :type identifier: str
        :return: Number of downloads generated
        :rtype: int
        """
        downloads_count = 0

        # Files are stored under base_path/identifier/qc/... etc.
        # But we want the relative path to start from base_path/identifier (not base_path)
        # so the path becomes qc/... instead of identifier/qc/...
        for dir_schema in schema.directories:
            downloads_count += self._generate_downloads_from_directory(
                dir_schema,
                base_path / identifier,
                identifier,
                results_base_path=base_path / identifier,
            )

        return downloads_count

    def _generate_downloads_from_directory(
        self,
        dir_schema,
        base_path: Path,
        identifier: str,
        results_base_path: Path,
    ) -> int:
        """
        Generate downloads from a directory schema.

        :param dir_schema: PipelineDirectorySchema defining the directory structure
        :param base_path: Base path containing the directory
        :type base_path: Path
        :param identifier: Pipeline run identifier
        :type identifier: str
        :param results_base_path: Root base path for computing relative download paths
        :type results_base_path: Path
        :return: Number of downloads generated
        :rtype: int
        """
        downloads_count = 0
        dir_path = base_path / dir_schema.folder_name

        if not dir_path.exists():
            return 0

        for file_schema in dir_schema.files:
            download_file = DownloadFile.from_pipeline_file_schema(
                file_schema, self.analysis, dir_path, results_base_path
            )
            if download_file:
                self.analysis.add_download(download_file)
                downloads_count += 1

        # Now go into the directory structure
        for subdir_schema in dir_schema.subdirectories:
            downloads_count += self._generate_downloads_from_directory(
                subdir_schema, dir_path, identifier, results_base_path
            )

        return downloads_count
