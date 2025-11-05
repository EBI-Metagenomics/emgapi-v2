"""
Admin utilities for workflows app.

This module contains shared constants and helpers used across workflow admin interfaces.
"""

from workflows.models import AssemblyAnalysisPipelineStatus

# Status label mappings for Unfold admin badges
STATUS_LABELS = {
    AssemblyAnalysisPipelineStatus.PENDING: "info",
    AssemblyAnalysisPipelineStatus.RUNNING: "warning",
    AssemblyAnalysisPipelineStatus.COMPLETED: "success",
    AssemblyAnalysisPipelineStatus.FAILED: "danger",
}
