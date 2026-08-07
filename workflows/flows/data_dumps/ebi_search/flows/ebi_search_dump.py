from __future__ import annotations

import tempfile
from pathlib import Path

from django.utils import timezone
from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from workflows.flows.data_dumps.ebi_search.tasks.dump_analyses import dump_analyses
from workflows.flows.data_dumps.ebi_search.tasks.dump_projects import dump_projects
from workflows.flows.data_dumps.ebi_search.tasks.publish_dump import publish_dump
from workflows.flows.data_dumps.ebi_search.utils.checkpoints import (
    read_last_dump_date,
)
from workflows.prefect_utils.flows_utils import django_db_flow


@django_db_flow(name="EBI Search dump", log_prints=True)
def ebi_search_dump_flow(initial: bool = False) -> dict:
    """Build and publish the MGnify projects and analyses EBI Search XML dumps.

    Incremental runs resume from the last successfully published dump date stored in
    the database. An initial dump creates that state.
    """
    until = timezone.now()
    output_root = Path(EMG_CONFIG.ebi_search.output_dir)
    since = None
    if not initial:
        since = read_last_dump_date()
        if since is None:
            raise ValueError(
                "No successful EBI Search dump date exists; run an initial dump first."
            )

    output_root.parent.mkdir(parents=True, exist_ok=True)
    run_logger = get_run_logger()
    run_logger.info(
        "Building %s EBI Search dump for %s%s",
        "initial" if initial else "incremental",
        until.isoformat(),
        "" if initial else f" from {since.isoformat()}",
    )

    with tempfile.TemporaryDirectory(
        prefix=".ebi-search-", dir=output_root.parent
    ) as temp_dir:
        staging_root = Path(temp_dir)
        if initial:
            source = staging_root / "initial"
            projects_dir = source / "projects" / "latest"
            runs_dir = source / "runs" / "latest"
            destination = output_root / "initial"
        else:
            date_directory = until.date().isoformat()
            source = staging_root / "incremental" / date_directory
            projects_dir = source / "projects"
            runs_dir = source / "runs"
            destination = output_root / "incremental" / date_directory

        project_counts = dump_projects(initial, projects_dir, since, until)
        analysis_counts = dump_analyses(
            initial,
            runs_dir,
            since,
            until,
            EMG_CONFIG.ebi_search.analysis_chunk_size,
            EMG_CONFIG.service_urls.transfer_services_url_root,
        )
        published_to = publish_dump(source, destination, until)

    return {
        "published_to": published_to,
        "since": since.isoformat() if since else None,
        "until": until.isoformat(),
        "projects": project_counts,
        "analyses": analysis_counts,
    }


if __name__ == "__main__":
    ebi_search_dump_flow()
