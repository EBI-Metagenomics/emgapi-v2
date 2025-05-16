"""
Custom exceptions for the pipeline schema validation system.
"""


class PipelineValidationError(Exception):
    """
    Raised when pipeline output validation fails.

    :param message: Error message describing the validation failure
    :param pipeline_name: Name of the pipeline that failed validation
    :param failed_rules: List of validation rules that failed
    """

    def __init__(
        self, message: str, pipeline_name: str = None, failed_rules: list = None
    ):
        """
        Initialize the validation error.

        :param message: Error message
        :param pipeline_name: Name of the pipeline
        :param failed_rules: List of failed validation rules
        """
        super().__init__(message)
        self.pipeline_name = pipeline_name
        self.failed_rules = failed_rules or []


class PipelineImportError(Exception):
    """
    Raised when pipeline result import fails.

    :param message: Error message describing the import failure
    :param pipeline_name: Name of the pipeline that failed import
    :param file_path: Path to the file that failed to import
    """

    def __init__(self, message: str, pipeline_name: str = None, file_path: str = None):
        """
        Initialize the import error.

        :param message: Error message
        :param pipeline_name: Name of the pipeline
        :param file_path: Path to the failed file
        """
        super().__init__(message)
        self.pipeline_name = pipeline_name
        self.file_path = file_path
