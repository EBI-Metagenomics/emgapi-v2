from prefect import get_run_logger
from prefect.deployments import run_deployment

from activate_django_first import EMG_CONFIG

from proteins.ingestion import analyses
from proteins.ingestion.analyses import IngestionResult
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task

CHUNK_DEPLOYMENT = (
    "ingest-assembly-proteins-chunk/ingest_assembly_proteins_chunk_deployment"
)


class IngestionFailed(Exception):
    """At least one analysis of a chunk failed."""


@task(task_run_name="Ingest the proteins of {analysis_accession}")
def ingest_analysis_task(analysis_accession: str) -> IngestionResult:
    return analyses.ingest_analysis(analysis_accession)


@flow(flow_run_name="Ingest assembly proteins")
def ingest_assembly_proteins(study_accession: str | None = None) -> int:
    """Finds the pending assembly analyses, and starts one chunk flow for each chunk. Does not wait.

    A Prefect trigger starts it when the results sync of an assembly analysis batch completes.
    Run it by hand to ingest existing analyses, of all studies or of one.

    :param study_accession: An MGYS accession, to ingest only its analyses.
    :return: The number of chunks started.
    """
    logger = get_run_logger()
    pending = analyses.find_pending_analyses(study_accession)
    chunks = analyses.chunk_by_study(pending, EMG_CONFIG.proteins.ingestion_chunk_size)
    for chunk in chunks:
        run_deployment(
            name=CHUNK_DEPLOYMENT,
            parameters={"analysis_accessions": chunk},
            timeout=0,
        )
    logger.info(f"Started {len(chunks)} chunks for {len(pending)} pending analyses")
    return len(chunks)


@flow(flow_run_name="Ingest a chunk of assembly proteins")
def ingest_assembly_proteins_chunk(
    analysis_accessions: list[str],
) -> list[IngestionResult]:
    """Ingests the proteins of each analysis. A failed analysis does not stop the others.

    :param analysis_accessions: The MGYAs of the chunk.
    :return: What happened to each analysis that did not fail.
    :raises IngestionFailed: At the end, when any analysis failed.
    """
    logger = get_run_logger()
    results, failed = [], []
    for accession in analysis_accessions:
        state = ingest_analysis_task(accession, return_state=True)
        if state.is_completed():
            results.append(state.result())
        else:
            logger.error(f"Protein ingestion of {accession} failed: {state.message}")
            failed.append(accession)
    if failed:
        raise IngestionFailed(
            f"Protein ingestion failed for {len(failed)} of {len(analysis_accessions)}"
            f" analyses: {', '.join(failed)}"
        )
    return results


@flow(flow_run_name="Ingest the proteins of {accession}")
def ingest_analysis_proteins(accession: str) -> list[IngestionResult]:
    """Ingests the proteins of one MGYA, or of every annotated analysis of one ERZ. For use by hand.

    :param accession: An MGYA or an ERZ accession.
    :return: What happened to each analysis.
    """
    return [
        ingest_analysis_task(analysis_accession)
        for analysis_accession in analyses.resolve_accession(accession)
    ]
