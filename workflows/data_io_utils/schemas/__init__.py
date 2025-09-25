"""
Unified pipeline schema validation system.

This module provides a unified approach to validate and import results from
different MGnify analysis pipelines (Assembly, VIRify, MAP).
"""

from .assembly import AssemblyResultSchema
from .base import (
    PipelineFileSchema,
    PipelineDirectorySchema,
    PipelineResultSchema,
    DownloadFileMetadata,
    ImportConfig,
)
from .exceptions import (
    PipelineValidationError,
    PipelineImportError,
)
from .map import MapResultSchema
from .virify import VirifyResultSchema

__all__ = [
    "AssemblyResultSchema",
    "VirifyResultSchema",
    "MapResultSchema",
    "PipelineFileSchema",
    "PipelineDirectorySchema",
    "PipelineResultSchema",
    "DownloadFileMetadata",
    "ImportConfig",
    "PipelineValidationError",
    "PipelineImportError",
]
