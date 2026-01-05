import pandas as pd
import pytest
import pandera.pandas as pa
from pandera.typing import Series

from workflows.data_io_utils.file_rules.common_rules import (
    FileExistsRule,
    FileIfExistsIsNotEmptyRule,
)
from workflows.data_io_utils.schemas.base import (
    PipelineFileSchema,
    PipelineDirectorySchema,
    PipelineResultSchema,
)
from workflows.data_io_utils.schemas.exceptions import PipelineValidationError
from analyses.base_models.with_downloads_models import (
    DownloadFileMetadata,
    DownloadFileType,
    DownloadType,
)


class SimpleContentValidator(pa.DataFrameModel):
    """Simple validator for testing content validation."""

    id: Series[str]
    value: Series[int] = pa.Field(ge=0)


def create_test_metadata(description: str) -> DownloadFileMetadata:
    """Helper to create DownloadFileMetadata for tests."""
    return DownloadFileMetadata(
        file_type=DownloadFileType.TSV,
        download_type=DownloadType.OTHER,
        short_description=description,
        long_description=f"Test file: {description}",
    )


@pytest.fixture
def temp_pipeline_structure(tmp_path):
    """
    Create a temporary pipeline output structure with some invalid files.

    Structure:
        base/
            identifier123/
                results/
                    identifier123_output.tsv (exists, valid)
                    identifier123_missing.tsv (missing - should fail)
                    identifier123_empty.tsv (empty - should fail)
                    identifier123_invalid_content.tsv (invalid content - should fail)
    """
    base_path = tmp_path
    identifier = "identifier123"
    results_dir = base_path / identifier / "results"
    results_dir.mkdir(parents=True)

    # Valid file
    valid_file = results_dir / f"{identifier}_output.tsv"
    df_valid = pd.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    df_valid.to_csv(valid_file, sep="\t", index=False)

    # Empty file (should fail FileIfExistsIsNotEmptyRule)
    empty_file = results_dir / f"{identifier}_empty.tsv"
    empty_file.touch()

    # Invalid content file (should fail content validation)
    invalid_file = results_dir / f"{identifier}_invalid_content.tsv"
    df_invalid = pd.DataFrame({"id": ["a", "b"], "value": [-1, -2]})  # negative values
    df_invalid.to_csv(invalid_file, sep="\t", index=False)

    # Missing file (identifier123_missing.tsv) - intentionally not created

    return base_path, identifier


def test_file_validation_collects_multiple_errors(temp_pipeline_structure):
    """
    Test that file validation collects both rule failures and content failures.
    """
    base_path, identifier = temp_pipeline_structure
    results_dir = base_path / identifier / "results"

    # Create schema for file with both rule and content validation
    file_schema = PipelineFileSchema(
        filename_template=f"{identifier}_invalid_content.tsv",
        validation_rules=[
            FileExistsRule,
            FileIfExistsIsNotEmptyRule,
        ],
        download_metadata=create_test_metadata("Test file"),
        content_validator=SimpleContentValidator,
    )

    file_path = results_dir / f"{identifier}_invalid_content.tsv"

    # Validate - should collect both empty file error (if added) and content error
    success, errors = file_schema.validate_file(file_path)

    # Should have content validation error (negative values)
    assert not success
    assert len(errors) > 0
    assert any("Content validation failed" in error for error in errors)


def test_directory_validation_collects_all_file_errors(temp_pipeline_structure):
    """
    Test that directory validation collects errors from all files.
    """
    base_path, identifier = temp_pipeline_structure

    # Create directory schema with multiple files (some will fail)
    dir_schema = PipelineDirectorySchema(
        folder_name="results",
        files=[
            PipelineFileSchema(
                filename_template=f"{identifier}_output.tsv",
                validation_rules=[FileExistsRule],
                download_metadata=create_test_metadata("Valid file"),
            ),
            PipelineFileSchema(
                filename_template=f"{identifier}_missing.tsv",
                validation_rules=[FileExistsRule],  # Will fail - file doesn't exist
                download_metadata=create_test_metadata("Missing file"),
            ),
            PipelineFileSchema(
                filename_template=f"{identifier}_empty.tsv",
                validation_rules=[
                    FileIfExistsIsNotEmptyRule
                ],  # Will fail - file is empty
                download_metadata=create_test_metadata("Empty file"),
            ),
        ],
    )

    # Validate directory
    validated_dir, errors = dir_schema.validate_directory(
        base_path / identifier, identifier
    )

    # Should have collected errors from multiple files
    assert len(errors) >= 2  # At least missing file and empty file
    assert any("missing" in error.lower() for error in errors)
    assert any("empty" in error.lower() or "size" in error.lower() for error in errors)


def test_pipeline_validation_collects_all_errors(temp_pipeline_structure):
    """
    Test that top-level pipeline validation collects errors from all directories and files.
    """
    base_path, identifier = temp_pipeline_structure

    # Create complete pipeline schema with multiple failing components
    pipeline_schema = PipelineResultSchema(
        pipeline_name="Test Pipeline",
        pipeline_version="1.0",
        directories=[
            PipelineDirectorySchema(
                folder_name="results",
                files=[
                    PipelineFileSchema(
                        filename_template=f"{identifier}_missing.tsv",
                        validation_rules=[FileExistsRule],
                        download_metadata=create_test_metadata("Missing file"),
                    ),
                    PipelineFileSchema(
                        filename_template=f"{identifier}_empty.tsv",
                        validation_rules=[FileIfExistsIsNotEmptyRule],
                        download_metadata=create_test_metadata("Empty file"),
                    ),
                    PipelineFileSchema(
                        filename_template=f"{identifier}_invalid_content.tsv",
                        validation_rules=[FileExistsRule],
                        download_metadata=create_test_metadata("Invalid content"),
                        content_validator=SimpleContentValidator,
                    ),
                ],
            ),
        ],
    )

    # Validation should fail with ALL errors collected
    with pytest.raises(PipelineValidationError) as exc_info:
        pipeline_schema.validate_results(base_path, identifier)

    error_message = str(exc_info.value)

    # Check that error message contains count
    assert "error(s)" in error_message.lower()

    # Check that multiple errors are present in the message
    assert "missing" in error_message.lower()
    assert "empty" in error_message.lower() or "size" in error_message.lower()
    assert "content validation failed" in error_message.lower()

    # Should have at least 3 errors (missing file, empty file, invalid content)
    assert error_message.count(" - ") >= 3


def test_validation_success_when_all_files_valid(temp_pipeline_structure):
    """
    Test that validation succeeds when all files pass validation.
    """
    base_path, identifier = temp_pipeline_structure

    # Create schema with only valid files
    pipeline_schema = PipelineResultSchema(
        pipeline_name="Test Pipeline",
        pipeline_version="1.0",
        directories=[
            PipelineDirectorySchema(
                folder_name="results",
                files=[
                    PipelineFileSchema(
                        filename_template=f"{identifier}_output.tsv",
                        validation_rules=[FileExistsRule],
                        download_metadata=create_test_metadata("Valid file"),
                        content_validator=SimpleContentValidator,
                    ),
                ],
            ),
        ],
    )

    # Should not raise exception
    validated_dir = pipeline_schema.validate_results(base_path, identifier)
    assert validated_dir is not None
