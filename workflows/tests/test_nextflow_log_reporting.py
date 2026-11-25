"""Tests for optional Nextflow log diagnostics."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from workflows.nextflow_utils.logs import log_nextflow_log


@pytest.mark.parametrize(
    ("content", "level"),
    [
        ("DEBUG starting\nINFO done\n", logging.INFO),
        ("WARN retrying\nINFO done\n", logging.WARNING),
        ("WARN retrying\nERROR pipeline failed\n", logging.ERROR),
        ("ERROR failed\nFATAL stopped\n", logging.CRITICAL),
        ("INFO NO_ERROR errorless\n", logging.INFO),
    ],
)
def test_log_path_uses_highest_level(tmp_path, content, level):
    """Report the path at the highest severity without copying log contents."""
    log_path = tmp_path / ".nextflow.log"
    log_path.write_text(content)
    with patch("workflows.nextflow_utils.logs.get_run_logger") as get_logger:
        log_nextflow_log(tmp_path, "nextflow run pipeline")
    get_logger.return_value.log.assert_called_once_with(
        level, "Nextflow log: %s", log_path
    )


@pytest.mark.parametrize("command", ["nextflow run pipeline", "malformed '"])
def test_missing_or_malformed_log_is_optional(tmp_path, command):
    """Ignore missing files and malformed shell commands."""
    with patch("workflows.nextflow_utils.logs.get_run_logger") as get_logger:
        log_nextflow_log(tmp_path, command)
    get_logger.return_value.log.assert_not_called()


def test_unreadable_log_still_reports_path(tmp_path):
    """Ignore read failures and report the known path."""
    log_path = tmp_path / ".nextflow.log"
    log_path.touch()
    with (
        patch("workflows.nextflow_utils.logs.get_run_logger") as get_logger,
        patch.object(Path, "open", side_effect=PermissionError),
    ):
        log_nextflow_log(tmp_path, "nextflow run pipeline")
    get_logger.return_value.log.assert_called_once_with(
        logging.INFO, "Nextflow log: %s", log_path
    )


def test_non_nextflow_command_does_not_report_stale_log(tmp_path):
    """Ignore Nextflow logs left by earlier jobs."""
    (tmp_path / ".nextflow.log").write_text("ERROR earlier run")
    with patch("workflows.nextflow_utils.logs.get_run_logger") as get_logger:
        log_nextflow_log(tmp_path, "java -jar uploader.jar")
    get_logger.assert_not_called()


def test_unexpected_diagnostic_error_is_optional(tmp_path):
    """Ignore diagnostic errors without affecting the caller."""
    with patch(
        "workflows.nextflow_utils.logs.get_run_logger", side_effect=RuntimeError
    ):
        log_nextflow_log(tmp_path, "nextflow run pipeline")
