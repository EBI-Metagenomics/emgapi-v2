"""Subprocess fixtures for flows within a SlurmWorker allocation."""

from itertools import count
from unittest.mock import MagicMock, Mock, patch

import pytest


@pytest.fixture
def mock_process_completion():
    """Allow tests to create pipeline outputs when a process finishes."""
    return Mock(return_value=("", ""))


@pytest.fixture
def mock_job_process(mock_process_completion):
    """Replace command execution with successful process objects."""
    pids = count(10000)

    def make_process(*args, **kwargs):
        process = MagicMock(pid=next(pids), returncode=0)
        process.communicate = mock_process_completion
        return process

    with patch(
        "workflows.prefect_utils.slurm_flow.subprocess.Popen", side_effect=make_process
    ) as mock_popen:
        yield mock_popen
