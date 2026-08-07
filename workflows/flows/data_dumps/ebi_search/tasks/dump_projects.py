from datetime import datetime
from pathlib import Path

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
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    additions = study_additions(initial, since, until)
    addition_count = additions.count()
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
        addition_count,
    )
    for study in additions.iterator(chunk_size=500):
        database_.entries.append(project_entry(study, lineages))
    write_xml(output_dir / "projects.xml", database_)
    return {"additions": addition_count, "deletions": len(deletion_ids)}
