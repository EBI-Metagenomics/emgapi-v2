"""Report Nextflow log paths without affecting job results."""

import logging
import re
import shlex
from pathlib import Path

from prefect import get_run_logger

LEVELS = {
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}
LEVEL_PATTERN = re.compile(r"\b(WARN|WARNING|ERROR|FATAL|CRITICAL)\b")


def log_nextflow_log(workdir: Path, command: str) -> None:
    """Report an existing Nextflow log at its highest warning level.

    Missing or unreadable logs do not affect the pipeline result.

    :param workdir: The job working directory, where Nextflow writes its log.
    :param command: The job command, used to identify Nextflow runs.
    :return: None.
    """
    try:
        tokens = shlex.split(command)
        if not any(Path(token).name == "nextflow" for token in tokens):
            return
        logger = get_run_logger()
        log_path = (Path(workdir) / ".nextflow.log").absolute()
        if not log_path.is_file():
            return
        level = logging.INFO
        try:
            with log_path.open(encoding="utf-8", errors="replace") as log_file:
                for line in log_file:
                    for match in LEVEL_PATTERN.finditer(line):
                        level = max(level, LEVELS[match.group()])
        finally:
            # Report the path even when reading fails.
            logger.log(level, "Nextflow log: %s", log_path)
    except Exception:
        # This diagnostic must never change the job state or hide its exception.
        return
