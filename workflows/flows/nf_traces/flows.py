from typing import Optional
import pandas as pd
from datetime import datetime
from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
import sqlite3
from activate_django_first import EMG_CONFIG  # noqa

from workflows.flows.nf_traces.tasks import (
    extract_traces_from_the_database,
    transform_traces_task,
    summary_stats,
)


@flow(
    name="Nextflow Traces extraction and transformation flow",
    description="ETL pipeline for the data extraction, transformation and loading of the nextflow traces",
    persist_result=True,
)
def nextflow_trace_etl_flow(
    sqlite_db_path: str,
    batch_size: int = 1000,
    min_created_at: Optional[datetime] = None,
    max_created_at: Optional[datetime] = None,
    only_completed: bool = True,
    exclude_failed: bool = True,
) -> pd.DataFrame:
    """
    Nextflow Trace extraction and transformation flow.

    This flow orchestrates the extraction and transformation of Nextflow trace data
    from OrchestratedClusterJob models.

    :param sqlite_db_path: Path to the SQLite database where the transformed data will be stored
    :param batch_size: Number of database records to process at once
    :param min_created_at: Only process jobs created after this datetime
    :param max_created_at: Only process jobs created before this datetime
    :param only_completed: Only process completed jobs
    :param exclude_failed: Exclude failed jobs
    """
    logger = get_run_logger()

    # Extract
    raw_records: pd.DataFrame = extract_traces_from_the_database(
        batch_size=batch_size,
        min_created_at=min_created_at,
        max_created_at=max_created_at,
        only_completed=only_completed,
        exclude_failed=exclude_failed,
    )

    # Transform
    transformed_data = transform_traces_task(raw_records)

    # Store in an SQLlite db for now
    with sqlite3.connect(f"{sqlite_db_path}/nf_traces.db") as sqlite_conn:
        transformed_data.to_sql("nextflow_traces", sqlite_conn, if_exists="replace")

    # Generate final summary
    summary = summary_stats(transformed_data)

    final_summary = f"""
## Nextflow trace extraction and transformation pipeline results

## SQLite database with traces
SQLite db path: {sqlite_db_path}/nf_traces.db

## Summary:
- Total records: {summary['total_records']}
- Pipelines: {list(summary['pipelines'].keys())}
"""

    create_markdown_artifact(
        final_summary,
        key="etl-pipeline-summary",
        description="Nextflow Traces extraction and transformation flow.",
    )

    logger.info(
        f"SQLite database with the traces: {len(transformed_data)} records stored in {sqlite_db_path}/nf_traces.db"
    )
