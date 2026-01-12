import sys
import json
from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
import pandera.pandas as pa
from pandera.typing import Series
from datetime import datetime
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from activate_django_first import EMG_CONFIG  # noqa

from django.db.models import Q
from workflows.models import OrchestratedClusterJob


def parse_time_to_seconds(time_str: str) -> float:
    """
    Transforms a Nextflow trace time string into its equivalent duration in seconds.
    :param time_str: A string representing a time duration (e.g., "2h 30m 10s 250ms").
    :return: The total duration in seconds.
    """
    if not time_str or pd.isna(time_str):
        return 0.0
    if isinstance(time_str, (int, float)):
        return float(time_str)

    total_seconds = 0.0
    units = {"h": 3600, "m": 60, "s": 1, "ms": 0.001}
    parts = str(time_str).strip().split()

    for part in parts:
        for unit, factor in units.items():
            if part.endswith(unit):
                # Special check for 'ms' vs 's'
                if unit == "s" and part.endswith("ms"):
                    continue
                try:
                    total_seconds += float(part.replace(unit, "")) * factor
                    break
                except ValueError:
                    pass
        else:
            try:
                total_seconds += float(part)
            except ValueError:
                pass

    return total_seconds


# Resource metric mapping from raw Nextflow trace names to schema-friendly names
NEXTFLOW_FIELD_MAP = {
    "%cpu": "cpu_percent",
    "%mem": "mem_percent",
    "peak_rss": "peak_rss",
    "peak_vmem": "peak_vmem",
    "rchar": "rchar",
    "wchar": "wchar",
    "submit": "submit",
    "duration": "duration",
    "realtime": "realtime",
    "exit": "exit_code",
    "status": "status",
    "hash": "hash",
    "task_id": "task_id",
    "native_id": "native_id",
    "name": "task_name",
}


class NextflowTraceSchema(pa.DataFrameModel):
    """
    Pandera schema for Nextflow trace data.
    Provides validation and automated data cleaning for trace records.
    """

    job_id: Series[str] = pa.Field(nullable=False)
    cluster_job_id: Series[str] = pa.Field(nullable=True)
    flow_run_id: Series[str] = pa.Field(nullable=True)
    task_index: Series[int] = pa.Field(nullable=True)
    task_name: Series[str] = pa.Field(nullable=True)
    status: Series[str] = pa.Field(nullable=True)
    exit_code: Series[float] = pa.Field(nullable=True, coerce=True)

    # Resource metrics - strings in raw data, converted to floats/MB
    cpu_percent: Series[float] = pa.Field(nullable=True, coerce=True)
    mem_percent: Series[float] = pa.Field(nullable=True, coerce=True)
    peak_rss: Series[float] = pa.Field(nullable=True)
    peak_vmem: Series[float] = pa.Field(nullable=True)
    rchar: Series[float] = pa.Field(nullable=True, coerce=True)
    wchar: Series[float] = pa.Field(nullable=True, coerce=True)

    # Timing fields
    duration: Series[float] = pa.Field(nullable=True)
    realtime: Series[float] = pa.Field(nullable=True)

    @pa.parser("duration", "realtime")
    @classmethod
    def parse_time(cls, series: Series[Any]) -> Series[float]:
        return series.apply(parse_time_to_seconds)

    @pa.parser("peak_rss", "peak_vmem")
    @classmethod
    def parse_memory(cls, series: Series[Any]) -> Series[float]:
        return series.apply(parse_memory_to_mb)

    class Config:
        coerce = True
        strict = False


def load_traces_from_json(json_path: str) -> pd.DataFrame:
    """
    Load Nextflow trace data directly from a JSON file and validate it using Pandera.

    :param json_path: Path to the JSON file containing trace data
    :type json_path: str
    :return: Validated and transformed DataFrame
    :rtype: pd.DataFrame
    """
    with open(json_path, "r") as f:
        data = json.load(f)

    # Handle both list of records and dict-of-dicts (Nextflow format)
    if isinstance(data, dict):
        # If it's the dictionary format indexed by task index, flatten it
        df = pd.DataFrame.from_dict(data, orient="index")
        df.index.name = "task_index"
        df = df.reset_index()
    else:
        df = pd.DataFrame(data)

    # Map fields to schema names
    df = df.rename(columns=NEXTFLOW_FIELD_MAP)

    # Clean '-' and empty strings
    df = df.replace(["-", ""], np.nan)

    # Validate and transform using the schema
    return NextflowTraceSchema.validate(df)


# Function to parse memory strings like "99.7 MB", "1.3 GB", or "1.1 TB" into MB
def parse_memory_to_mb(memory_val: Any) -> float:
    """
    Converts a memory string into its equivalent value in Megabytes (MB).
    :param memory_val: A string representation of memory (e.g., "1.3 GB", "1024 KB").
    :return: The memory value in MB.
    """
    if pd.isna(memory_val):
        return 0.0
    if isinstance(memory_val, (int, float)):
        return float(memory_val)

    units = {"KB": 1 / 1024, "MB": 1, "GB": 1024, "TB": 1024 * 1024}
    memory_str = str(memory_val).strip().upper()

    for unit, factor in units.items():
        if unit in memory_str:
            try:
                return float(memory_str.replace(unit, "").strip()) * factor
            except ValueError:
                break
    try:
        return float(memory_str)
    except ValueError:
        return 0.0


@task(
    name="Extract Nextflow Traces",
    retry_attempts=3,
    retry_delay_seconds=60,
    description="Extract trace records from OrchestratedClusterJob database",
)
def extract_traces_task(
    batch_size: int = 1000,
    min_created_at: Optional[datetime] = None,
    max_created_at: Optional[datetime] = None,
    only_completed: bool = True,
    exclude_failed: bool = True,
) -> pd.DataFrame:
    """
    Prefect task for extracting Nextflow trace records from database.

    This task wraps the extract_trace_records_from_database function with
    Prefect features like retries, logging, and monitoring.

    :param batch_size: Number of jobs to process at once
    :type batch_size: int
    :param min_created_at: Only process jobs created after this datetime
    :type min_created_at: datetime, optional
    :param max_created_at: Only process jobs created before this datetime
    :type max_created_at: datetime, optional
    :param only_completed: Only process jobs with completed status
    :type only_completed: bool
    :param exclude_failed: Exclude jobs with failed status
    :type exclude_failed: bool
    :return: DataFrame of trace records
    :rtype: pd.DataFrame
    """
    logger = get_run_logger()
    logger.info("Starting trace extraction task")

    try:
        records = extract_trace_records_from_database(
            batch_size=batch_size,
            min_created_at=min_created_at,
            max_created_at=max_created_at,
            only_completed=only_completed,
            exclude_failed=exclude_failed,
            logger=logger,
        )

        logger.info(f"Successfully extracted {len(records)} trace records")
        return records

    except Exception as e:
        logger.error(f"Trace extraction failed: {e}")
        raise


def extract_trace_records_from_database(
    batch_size: int = 1000,
    min_created_at: Optional[datetime] = None,
    max_created_at: Optional[datetime] = None,
    only_completed: bool = True,
    exclude_failed: bool = True,
    logger=None,
) -> pd.DataFrame:
    """
    Extract Nextflow trace records directly from the database by querying OrchestratedClusterJob models.

    :param batch_size: Number of jobs to process at once (default: 1000)
    :type batch_size: int
    :param min_created_at: Only process jobs created after this datetime (optional)
    :type min_created_at: datetime, optional
    :param max_created_at: Only process jobs created before this datetime (optional)
    :type max_created_at: datetime, optional
    :param only_completed: Only process jobs with completed status (default: True)
    :type only_completed: bool
    :param exclude_failed: Exclude jobs with failed status (default: True)
    :type exclude_failed: bool
    :param logger: Logger to use (optional)
    :type logger: logging.Logger, optional
    :return: DataFrame of trace records
    :rtype: pd.DataFrame
    """
    if logger:
        logger.info("Extracting trace records from database...")
    else:
        print("Extracting trace records from database...", file=sys.stderr)

    # Build the base query
    query = Q()

    # Apply time filters
    if min_created_at:
        query &= Q(created_at__gte=min_created_at)
    if max_created_at:
        query &= Q(created_at__lte=max_created_at)

    # Apply status filters
    if only_completed:
        query &= Q(last_known_state="COMPLETED")
    if exclude_failed:
        query &= ~Q(last_known_state="FAILED")

    # Filter for jobs that have trace data
    query &= ~Q(nextflow_trace=None)
    query &= ~Q(nextflow_trace={})
    query &= ~Q(nextflow_trace=[])

    # Get total count for progress reporting
    total_jobs = OrchestratedClusterJob.objects.filter(query).count()
    if logger:
        logger.info(f"Found {total_jobs} jobs matching criteria")
    else:
        print(f"Found {total_jobs} jobs matching criteria", file=sys.stderr)

    all_dfs = []
    jobs_processed = 0
    jobs_with_trace = 0

    # Process in batches to avoid memory issues
    for job in OrchestratedClusterJob.objects.filter(query).iterator(
        chunk_size=batch_size
    ):
        jobs_processed += 1

        nextflow_trace = job.nextflow_trace
        if not nextflow_trace:
            continue

        # Convert the trace to a DataFrame
        if isinstance(nextflow_trace, dict):
            # Orient 'index' because keys are task indices
            df_job = pd.DataFrame.from_dict(nextflow_trace, orient="index")
            df_job.index.name = "task_index"
            df_job = df_job.reset_index()
        elif isinstance(nextflow_trace, list):
            df_job = pd.DataFrame(nextflow_trace)
        else:
            continue

        if df_job.empty:
            continue

        # Filter out failed tasks if requested
        if exclude_failed and "status" in df_job.columns:
            df_job = df_job[df_job["status"] != "FAILED"]
            if df_job.empty:
                continue

        # Add metadata from the job
        df_job["job_id"] = str(job.id)
        df_job["cluster_job_id"] = job.cluster_job_id
        df_job["flow_run_id"] = str(job.flow_run_id)
        df_job["job_created_at"] = (
            job.created_at.isoformat() if job.created_at else None
        )
        df_job["job_updated_at"] = (
            job.updated_at.isoformat() if job.updated_at else None
        )
        df_job["job_last_known_state"] = job.last_known_state

        # Map fields to schema names
        df_job = df_job.rename(columns=NEXTFLOW_FIELD_MAP)

        all_dfs.append(df_job)
        jobs_with_trace += 1

        # Progress reporting
        if jobs_processed % 100 == 0:
            if logger:
                logger.info(f"Processed {jobs_processed}/{total_jobs} jobs...")
            else:
                print(
                    f"Processed {jobs_processed}/{total_jobs} jobs...", file=sys.stderr
                )

    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)

    if logger:
        logger.info(f"Processed {jobs_processed} jobs total")
        logger.info(f"Found {jobs_with_trace} jobs with trace data")
        logger.info(f"Extracted {len(final_df)} trace records")
    else:
        print(f"Processed {jobs_processed} jobs total", file=sys.stderr)
        print(f"Found {jobs_with_trace} jobs with trace data", file=sys.stderr)
        print(f"Extracted {len(final_df)} trace records", file=sys.stderr)

    return final_df


def transform_trace_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw trace records into a structured DataFrame with computed metrics.
    :param df: DataFrame of raw trace records
    :return: Transformed DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    df = df.replace(["-", ""], np.nan)
    df = NextflowTraceSchema.validate(df)

    # Rename to more descriptive columns
    df = df.rename(
        columns={
            "duration": "duration_seconds",
            "realtime": "realtime_seconds",
            "peak_rss": "peak_rss_mb",
            "peak_vmem": "peak_vmem_mb",
        }
    )

    # Task Name Parsing
    if "task_name" in df.columns:
        df["accession"] = (
            df["task_name"].str.extract(r"\(([^)]+)\)\s*$", expand=False).fillna("")
        )
        workflow_path = df["task_name"].str.replace(r"\s*\([^)]+\)\s*$", "", regex=True)
        split_path = workflow_path.str.split(":", expand=True)

        if not split_path.empty:
            df["pipeline"] = split_path[0].fillna("")
            if len(split_path.columns) > 2:
                df["subworkflow"] = (
                    split_path.iloc[:, 1:-1]
                    .fillna("")
                    .apply(lambda x: ":".join(x[x != ""]), axis=1)
                )
                df["process"] = split_path.iloc[:, -1].fillna("")
            elif len(split_path.columns) == 2:
                df["subworkflow"] = split_path[1].fillna("")
                df["process"] = split_path[1].fillna("")
            else:
                df["subworkflow"] = ""
                df["process"] = split_path[0].fillna("")

    df["pipeline_category"] = categorize_pipeline_vectorized(
        df.get("pipeline", pd.Series("unknown", index=df.index))
    )

    if "duration_seconds" in df.columns and "realtime_seconds" in df.columns:
        df["cpu_efficiency"] = df["duration_seconds"] / df["realtime_seconds"].replace(
            0, np.nan
        )

    return df


def categorize_pipeline_vectorized(pipeline_series: pd.Series) -> pd.Series:
    """
    Categorize pipeline names into broader categories using vectorized operations.

    :param pipeline_series: Pandas Series containing pipeline names
    :type pipeline_series: pd.Series
    :return: Series with category strings
    :rtype: pd.Series
    """
    pipeline_upper = pipeline_series.str.upper()
    conditions = [
        pipeline_upper.str.contains("AMPLICON", na=False),
        pipeline_upper.str.contains("ASA|ASSEMBLY", na=False),
        pipeline_upper.str.contains("VIRIFY", na=False),
        pipeline_upper.str.contains("MAP", na=False),
        pipeline_upper.str.contains("RAWREADS|RAW_READS", na=False),
    ]
    choices = ["amplicon", "assembly", "virify", "map", "raw_reads"]
    return pd.Series(
        np.select(conditions, choices, default="other"), index=pipeline_series.index
    )


def get_etl_summary_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics for the ETL output.
    """
    if df.empty:
        return {
            "total_records": 0,
            "pipelines": {},
            "time_range": None,
            "resource_stats": None,
        }

    summary = {
        "total_records": len(df),
        "pipelines": (
            df["pipeline"].value_counts().to_dict() if "pipeline" in df.columns else {}
        ),
        "pipeline_categories": (
            df["pipeline_category"].value_counts().to_dict()
            if "pipeline_category" in df.columns
            else {}
        ),
        "status_distribution": (
            df["status"].value_counts().to_dict() if "status" in df.columns else {}
        ),
    }

    metrics = {
        "duration_seconds": ("min_duration", "max_duration", "avg_duration"),
        "cpu_percent": ("avg_cpu_percent",),
        "peak_rss_mb": ("avg_peak_rss_mb",),
        "peak_vmem_mb": ("avg_peak_vmem_mb",),
    }

    summary["time_range"] = {}
    summary["resource_stats"] = {}

    for col, labels in metrics.items():
        if col in df.columns:
            stats = df[col].agg(["min", "max", "mean"])
            if col == "duration_seconds":
                summary["time_range"] = dict(zip(labels, stats))
            else:
                summary["resource_stats"][labels[0]] = stats["mean"]

    return summary


# ===== PREFECT FLOW IMPLEMENTATION =====


@task(
    name="Transform Traces",
    retry_attempts=2,
    retry_delay_seconds=30,
    description="Transform raw trace records into structured DataFrame",
)
def transform_traces_task(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prefect task for transforming trace records.

    Includes data quality validation and monitoring artifacts.

    :param df: DataFrame of raw trace records
    :type df: pd.DataFrame
    :return: Transformed DataFrame
    :rtype: pd.DataFrame
    """
    logger = get_run_logger()
    logger.info(f"Starting transformation of {len(df)} records")

    # Data quality validation
    if df.empty:
        raise ValueError("No trace records provided for transformation")

    try:
        # Perform transformation
        df = transform_trace_records(df)

        # Create monitoring artifact
        summary_text = f"""
### Transformation Summary
- **Records Processed**: {len(df)}
- **Output Records**: {len(df)}
- **Columns Generated**: {len(df.columns)}
- **Memory Usage**: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB
"""

        create_markdown_artifact(
            summary_text,
            key="trace-transformation-summary",
            description="Nextflow trace transformation metrics",
        )

        logger.info(
            f"Successfully transformed {len(df)} records into {len(df.columns)} columns"
        )
        return df

    except Exception as e:
        logger.error(f"Trace transformation failed: {e}")
        raise


@flow(
    name="Nextflow Trace ETL Pipeline",
    description="ETL pipeline for Nextflow trace data extraction and transformation (Load phase handled separately)",
    persist_result=True,
    timeout_seconds=3600,
    log_prints=True,
)
def nextflow_trace_etl_flow(
    batch_size: int = 1000,
    min_created_at: Optional[datetime] = None,
    max_created_at: Optional[datetime] = None,
    only_completed: bool = True,
    exclude_failed: bool = True,
) -> pd.DataFrame:
    """
    Nextflow Trace ETL pipeline as a Prefect flow (Extract and Transform only).

    This flow orchestrates the extraction and transformation of Nextflow trace data
    from OrchestratedClusterJob models. The Load phase will be handled separately
    when database storage is implemented.

    Features:
    - Automatic retries for transient failures
    - Comprehensive logging and monitoring
    - Data quality validation
    - Progress tracking via Prefect UI
    - Artifact generation for key metrics
    - Returns transformed DataFrame for further processing

    :param batch_size: Number of database records to process at once
    :type batch_size: int
    :param min_created_at: Only process jobs created after this datetime
    :type min_created_at: datetime, optional
    :param max_created_at: Only process jobs created before this datetime
    :type max_created_at: datetime, optional
    :param only_completed: Only process completed jobs
    :type only_completed: bool
    :param exclude_failed: Exclude failed jobs
    :type exclude_failed: bool
    :return: Transformed DataFrame with trace records (ready for database loading)
    :rtype: pd.DataFrame
    """
    logger = get_run_logger()
    logger.info("=== Starting Nextflow Trace ETL Pipeline ===")

    # Extract phase
    raw_records = extract_traces_task(
        batch_size=batch_size,
        min_created_at=min_created_at,
        max_created_at=max_created_at,
        only_completed=only_completed,
        exclude_failed=exclude_failed,
    )

    # Transform phase
    transformed_data = transform_traces_task(raw_records)

    # Note: Load phase will be implemented separately for database storage

    # Generate final summary
    summary = get_etl_summary_stats(transformed_data)

    final_summary = f"""
### ETL Pipeline Complete (Extract & Transform)

**Extraction**:
- Processed {len(raw_records)} raw trace records

**Transformation**:
- Generated {len(transformed_data)} structured records
- Created {len(transformed_data.columns)} data columns
- DataFrame ready for database loading

**Summary Statistics**:
- Total Records: {summary['total_records']}
- Pipelines: {list(summary['pipelines'].keys())}
- Pipeline Categories: {list(summary['pipeline_categories'].keys())}
- Average Duration: {summary['time_range']['avg_duration']:.2f} seconds (if available)

**Next Steps**:
- This DataFrame can now be loaded into your database
- Implement database loading in a separate flow/task
- Use `transformed_data` as input to your database loader
"""

    create_markdown_artifact(
        final_summary,
        key="etl-pipeline-summary",
        description="Nextflow Trace ETL Pipeline Results (Ready for Database Loading)",
    )

    logger.info("\n=== ETL Pipeline Complete ===")
    logger.info(
        f"Final dataset: {len(transformed_data)} records, {len(transformed_data.columns)} columns"
    )
    logger.info(
        "DataFrame is ready for database loading (to be implemented separately)"
    )

    return transformed_data
