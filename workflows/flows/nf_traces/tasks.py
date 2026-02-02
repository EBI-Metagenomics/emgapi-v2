from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from prefect import task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from django.db.models import Q

from activate_django_first import EMG_CONFIG  # noqa

from workflows.flows.nf_traces.utils import (
    NextflowTraceSchema,
    preprocess_trace_dataframe,
)
from workflows.models import OrchestratedClusterJob


@task(
    name="Extract Nextflow Traces",
    description="Extract trace records from OrchestratedClusterJob database",
)
def extract_traces_from_the_database(
    batch_size: int = 1000,
    min_created_at: Optional[datetime] = None,
    max_created_at: Optional[datetime] = None,
    only_completed: bool = True,
    exclude_failed: bool = True,
) -> pd.DataFrame:
    """
    Prefect task for extracting Nextflow trace records from the database.

    :param batch_size: Number of Orchestrated jobs to process at once
    :param min_created_at: Only process jobs created after this datetime
    :param max_created_at: Only process jobs created before this datetime
    :param only_completed: Only process jobs with completed status
    :param exclude_failed: Exclude jobs with failed status
    :return: DataFrame of trace records
    """
    logger = get_run_logger()
    logger.info("Starting trace extraction task")

    query = Q()

    # Filters #
    if min_created_at:
        query &= Q(created_at__gte=min_created_at)
    if max_created_at:
        query &= Q(created_at__lte=max_created_at)
    if only_completed:
        query &= Q(last_known_state="COMPLETED")
    if exclude_failed:
        query &= ~Q(last_known_state="FAILED")

    # Ignore jobs that don't have a nextflow trace
    query &= ~Q(nextflow_trace=None)
    query &= ~Q(nextflow_trace={})
    query &= ~Q(nextflow_trace=[])

    query_set = OrchestratedClusterJob.objects.filter(query)

    total_jobs = query_set.count()
    logger.info(f"Found {total_jobs} jobs matching criteria")

    all_dfs = []
    jobs_processed = 0
    jobs_with_trace = 0

    for job in query_set.iterator(chunk_size=batch_size):
        jobs_processed += 1

        nextflow_trace = job.nextflow_trace
        if not nextflow_trace:
            continue

        # Convert the trace to a DataFrame
        if isinstance(nextflow_trace, dict):
            df_job = pd.DataFrame.from_dict(nextflow_trace, orient="index")
        elif isinstance(nextflow_trace, list):
            df_job = pd.DataFrame(nextflow_trace)
        else:
            logger.error(
                f"Unexpected trace format: {type(nextflow_trace)}, skipping job id={job.pk}"
            )
            continue

        if df_job.empty:
            logger.warning(f"Empty trace for job id={job.pk}, skipping")
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
        df_job["job_created_at"] = job.created_at
        df_job["job_updated_at"] = job.updated_at
        df_job["job_last_known_state"] = job.last_known_state

        # Preprocess (renaming, etc.)
        df_job = preprocess_trace_dataframe(df_job)

        all_dfs.append(df_job)
        jobs_with_trace += 1

        # Progress reporting
        if jobs_processed % 100 == 0:
            logger.info(f"Processed {jobs_processed}/{total_jobs} jobs...")

    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)

    logger.info(f"Processed {jobs_processed} jobs total")
    logger.info(f"Found {jobs_with_trace} jobs with trace data")
    logger.info(f"Extracted {len(final_df)} trace records")

    return final_df


@task(
    name="Transform the trace records",
    description="Validate the trace records and transform into a structured DataFrame with computed metrics.",
)
def transform_trace_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw trace records into a structured DataFrame with computed metrics.
    :param df: DataFrame of raw trace records
    :return: Transformed DataFrame
    """
    logger = get_run_logger()

    if df.empty:
        logger.warning("No trace records provided for transformation")
        return pd.DataFrame()

    df = preprocess_trace_dataframe(df)
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

    # We need to parse the name of the tasks, from something like EBIMETAGENOMICS:AMPLICON:DADA2 to its components
    # as we need the accession of the run/assembly being processed, the pipeline being used and the tool
    if "task_name" in df.columns:
        df["accession"] = (
            df["task_name"].str.extract(r"\(([^)]+)\)\s*$", expand=False).fillna("")
        )
        split_process_name = df["task_name"].str.split(":")
        if not split_process_name.empty:
            # The pipeline name is usually the second element
            df["pipeline"] = split_process_name.str[1].fillna("")
            # The process is the last element before the accession part
            df["process"] = split_process_name.str[-1].str.split().str[0].fillna("")

    if "duration_seconds" in df.columns and "realtime_seconds" in df.columns:
        df["cpu_efficiency"] = df["duration_seconds"] / df["realtime_seconds"].replace(
            0, np.nan
        )

    return df


@task(
    name="Generate Summary Statistics",
    description="Generate summary statistics for the ETL output.",
)
def summary_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics for the dataframe.
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


@task(
    name="Transform Traces",
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
- Records Processed: {len(df)}
- Output Records: {len(df)}
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
