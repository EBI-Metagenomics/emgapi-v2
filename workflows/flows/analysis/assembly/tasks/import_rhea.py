"""Prefect entry point for the ASAv6 molecule index."""

from pathlib import Path

import activate_django_first  # noqa

from analyses.utils.rhea import import_rhea
from workflows.prefect_utils.flows_utils import django_db_task as task


@task(retries=2, retry_delay_seconds=30)
def import_rhea_annotations(analysis_id: int, path: Path) -> int:
    """Import a validated assembly's Rhea table.

    :param analysis_id: The analysis database identifier.
    :param path: The proteins2rhea table on the worker filesystem.
    :return: The number of unique indexed molecules.
    """
    return import_rhea(analysis_id, path)
