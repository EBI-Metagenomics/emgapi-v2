from datetime import datetime
from pathlib import Path

from prefect import get_run_logger

from workflows.flows.data_dumps.ebi_search.utils.documents import (
    biome_lineages,
    project_entry,
)
from workflows.flows.data_dumps.ebi_search.utils.querysets import (
    changed_studies,
    study_additions,
)
from workflows.flows.data_dumps.ebi_search.utils.xml import (
    PROJECT_DATABASE_NAME,
    database,
    write_deletions,
    write_xml,
)
from workflows.prefect_utils.flows_utils import django_db_task


@django_db_task(name="Dump EBI Search projects")
def dump_projects(
    initial: bool,
    output_dir: Path,
    since: datetime | None,
    until: datetime,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    additions = study_additions(initial, since, until)
    lineages = biome_lineages()
    deletion_ids: list[str] = []
    if not initial:
        deletion_ids = list(
            changed_studies(since, until)
            .filter(created_at__lt=since)
            .order_by("accession")
            .values_list("accession", flat=True)
        )
        write_deletions(
            output_dir / "projects-deletes.xml", PROJECT_DATABASE_NAME, deletion_ids
        )

    database_ = database(
        PROJECT_DATABASE_NAME,
        "EMG Projects – studies analysed by MGnify",
        until.date().isoformat(),
    )
    run_logger = get_run_logger()
    failures = []
    for study in additions.iterator(chunk_size=500):
        try:
            database_.entries.append(project_entry(study, lineages))
        except Exception as error:
            reason = " ".join(str(error).split())[:300] or type(error).__name__
            run_logger.warning(
                "Skipping EBI Search study %s: %s",
                study.accession,
                reason,
            )
            failures.append((study.accession, study.accession, reason))

    database_.entry_count = len(database_.entries)
    write_xml(output_dir / "projects.xml", database_)
    return {
        "additions": len(database_.entries),
        "deletions": len(deletion_ids),
        "failures": failures,
    }
