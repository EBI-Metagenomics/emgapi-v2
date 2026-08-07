from datetime import datetime
from pathlib import Path

from django.core.paginator import Paginator
from prefect import get_run_logger

from workflows.flows.data_dumps.ebi_search.utils.documents import (
    analysis_document_id,
    analysis_entry,
    biome_lineages,
)
from workflows.flows.data_dumps.ebi_search.utils.functional_annotations import (
    _http_client,
)
from workflows.flows.data_dumps.ebi_search.utils.querysets import (
    analyses_to_delete,
    analysis_additions,
)
from workflows.flows.data_dumps.ebi_search.utils.xml import (
    RUN_DATABASE_NAME,
    database,
    write_deletions,
    write_xml,
)
from workflows.prefect_utils.flows_utils import django_db_task


@django_db_task(name="Dump EBI Search runs")
def dump_analyses(
    initial: bool,
    output_dir: Path,
    since: datetime | None,
    until: datetime,
    chunk_size: int,
    transfer_services_url_root: str,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    additions = analysis_additions(initial, since, until)
    deletion_ids: list[str] = []
    if not initial:
        deletion_ids = [
            analysis_document_id(analysis)
            for analysis in analyses_to_delete(since, until)
            .filter(created_at__lt=since)
            .order_by("accession", "pipeline_version")
            .only("accession", "pipeline_version")
        ]
        write_deletions(
            output_dir / "analyses-deletes.xml", RUN_DATABASE_NAME, deletion_ids
        )

    run_logger = get_run_logger()
    client = _http_client()
    lineages = biome_lineages()
    paginator = Paginator(additions, chunk_size)
    addition_count = 0
    try:
        for page in paginator:
            page_analyses = list(page.object_list)
            addition_count += len(page_analyses)
            output_file = output_dir / f"analyses_{page.number:04d}.xml"
            run_logger.info(
                "Writing analysis page %s/%s to %s",
                page.number,
                paginator.num_pages,
                output_file,
            )
            database_ = database(
                RUN_DATABASE_NAME,
                "EMG Analysis runs – samples analysed by MGnify pipelines",
                until.date().isoformat(),
                len(page_analyses),
            )
            for analysis in page_analyses:
                database_.entries.append(
                    analysis_entry(
                        analysis,
                        transfer_services_url_root,
                        client,
                        run_logger,
                        lineages,
                    )
                )
            write_xml(output_file, database_)
    finally:
        client.close()
    return {"additions": addition_count, "deletions": len(deletion_ids)}
