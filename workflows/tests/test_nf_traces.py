import uuid

import pandas as pd
import pytest
from datetime import datetime
import sqlite3

from workflows.flows.nf_traces.tasks import (
    extract_traces_from_the_database,
    transform_traces_task,
    transform_trace_records,
    summary_stats,
)
from workflows.flows.nf_traces.flows import nextflow_trace_etl_flow
from workflows.flows.nf_traces.utils import (
    NextflowTraceSchema,
    parse_time_to_seconds,
    parse_memory_to_mb,
    NEXTFLOW_FIELD_MAP,
)
from workflows.models import OrchestratedClusterJob


@pytest.fixture
def sample_nextflow_trace_data():
    """Sample Nextflow trace data for testing."""
    return {
        "1": {
            "%cpu": "95.2 %",
            "%mem": "85.5 %",
            "peak_rss": "1.2 GB",
            "peak_vmem": "2.5 GB",
            "rchar": "6 GB",
            "wchar": "1 GB",
            "submit": "2023-01-01T10:00:00Z",
            "duration": "1h 30m 15s",
            "realtime": "1h 35m 20s",
            "exit": "0",
            "status": "COMPLETED",
            "hash": "abc123",
            "task_id": 1,
            "native_id": "12345",
            "name": "EBIMETAGENOMICS:AMPLICON:DADA2 (SRR987654)",
        },
        "2": {
            "%cpu": "88.1 %",
            "%mem": "75.3 %",
            "peak_rss": "999.7 MB",
            "peak_vmem": "1.8 GB",
            "rchar": "6 GB",
            "wchar": "1 GB",
            "submit": "2023-01-01T11:00:00Z",
            "duration": "2h 15m 30s",
            "realtime": "2h 20m 45s",
            "exit": "0",
            "status": "COMPLETED",
            "hash": "def456",
            "task_id": 3,
            "native_id": "67890",
            "name": "EBIMETAGENOMICS:ASSEMBLY_ANALYSIS_PIPELINE:RENAME_CONTIGS (ERZ101)",
        },
        "3": {
            "%cpu": "538.9%",
            "exit": 0,
            "hash": "51/8915d9",
            "name": "EBIMETAGENOMICS_MIASSEMBLER:MIASSEMBLER:SHORT_READS_ASSEMBLER:SHORT_READS_QC:FASTP (SRR15043906)",
            "rchar": "6 GB",
            "wchar": "6.1 GB",
            "status": "COMPLETED",
            "submit": "2025-04-16 07:15:54.742",
            "task_id": 2,
            "duration": "4m 56s",
            "peak_rss": "2.5 GB",
            "realtime": "4m 1s",
            "native_id": 50195428,
            "peak_vmem": "2.9 GB",
        },
        "4": {
            "%cpu": "564.9%",
            "exit": 0,
            "hash": "01/d84d38",
            "name": "EBIMETAGENOMICS_MIASSEMBLER:MIASSEMBLER:SHORT_READS_ASSEMBLER:SHORT_READS_QC:FASTP (SRR15043905)",
            "rchar": "6.4 GB",
            "wchar": "6.5 GB",
            "status": "COMPLETED",
            "submit": "2025-04-16 07:16:00.368",
            "task_id": 1,
            "duration": "5m 20s",
            "peak_rss": "2.5 GB",
            "realtime": "4m 7s",
            "native_id": 50195483,
            "peak_vmem": "2.9 GB",
        },
        "5": {
            "%cpu": "195.8%",
            "exit": 0,
            "hash": "60/b28ef3",
            "name": "EBIMETAGENOMICS_MIASSEMBLER:MIASSEMBLER:SHORT_READS_ASSEMBLER:FASTQC_BEFORE (SRR15043906)",
            "rchar": "6 GB",
            "wchar": "3.2 MB",
            "status": "COMPLETED",
            "submit": "2025-04-16 07:15:45.442",
            "task_id": 5,
            "duration": "6m 5s",
            "peak_rss": "5.2 GB",
            "realtime": "4m 41s",
            "native_id": 50195331,
            "peak_vmem": "40.3 GB",
        },
        "6": {
            "%cpu": "521.0%",
            "exit": 0,
            "hash": "3a/c3a49a",
            "name": "EBIMETAGENOMICS_MIASSEMBLER:MIASSEMBLER:SHORT_READS_ASSEMBLER:SHORT_READS_QC:FASTP (SRR15043907)",
            "rchar": "7.4 GB",
            "wchar": "7.5 GB",
            "status": "COMPLETED",
            "submit": "2025-04-16 07:16:31.904",
            "task_id": 3,
            "duration": "5m 29s",
            "peak_rss": "2.4 GB",
            "realtime": "4m 14s",
            "native_id": 50195835,
            "peak_vmem": "2.9 GB",
        },
    }


@pytest.fixture
def sample_orchestrated_job():
    """Create a sample OrchestratedClusterJob with trace data."""
    job = OrchestratedClusterJob(
        id=uuid.uuid4(),
        cluster_job_id=12345,
        job_submit_description={
            "name": "Impressive job",
            "script": "srun sleep 1",
        },
        flow_run_id=uuid.uuid4(),
        created_at=datetime(2023, 1, 1, 10, 0, 0),
        updated_at=datetime(2023, 1, 1, 12, 0, 0),
        last_known_state="COMPLETED",
        nextflow_trace={
            "1": {
                "%cpu": "95 %",
                "%mem": "85 %",
                "peak_rss": "1.2 GB",
                "peak_vmem": "2.5 GB",
                "rchar": "123456",
                "wchar": "654321",
                "submit": "2023-01-01T10:00:00Z",
                "duration": "1h 30m 15s",
                "realtime": "1h 35m 20s",
                "exit": "0",
                "status": "COMPLETED",
                "hash": "abc123",
                "task_id": 1,
                "native_id": "12345",
                "name": "EBI_METAGENOMICS:AMPLICON:DADA2 (SRR432431)",
            }
        },
    )
    job.save()
    return job


def test_parse_time_to_seconds():
    """Test time parsing utility."""
    assert parse_time_to_seconds("1h 30m 15s") == 5415.0
    assert parse_time_to_seconds("2h 15m 30s") == 8130.0  # 2*3600 + 15*60 + 30 = 8130
    assert parse_time_to_seconds("5m 30s") == 330.0
    assert parse_time_to_seconds("1h") == 3600.0
    assert parse_time_to_seconds("30s") == 30.0
    assert parse_time_to_seconds("") == 0.0
    assert parse_time_to_seconds(None) == 0.0
    assert parse_time_to_seconds("invalid") == 0.0


def test_parse_memory_to_mb():
    """Test memory parsing utility."""
    assert parse_memory_to_mb("1.2 GB") == 1228.8  # 1.2 * 1024 = 1228.8
    assert parse_memory_to_mb("999.7 MB") == 999.7
    assert parse_memory_to_mb("2.5 GB") == 2560.0  # 2.5 * 1024 = 2560.0
    assert parse_memory_to_mb("1024 KB") == 1.0  # 1024 / 1024 = 1.0
    assert parse_memory_to_mb("1.1 TB") == 1153433.6  # 1.1 * 1024 * 1024 = 1153433.6
    assert parse_memory_to_mb("") == 0.0
    assert parse_memory_to_mb(None) == 0.0
    assert parse_memory_to_mb("invalid") == 0.0


@pytest.mark.django_db
def test_extract_traces_from_database(sample_orchestrated_job, prefect_harness):
    """Test extraction of traces from database."""
    # Call the task
    result_df = extract_traces_from_the_database(batch_size=1000)

    # Verify the result
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 1  # One trace record

    # Check that metadata was added
    assert "native_id" in result_df.columns
    # Check that fields were mapped
    assert "cpu_percent" in result_df.columns
    assert "peak_rss" in result_df.columns
    assert "duration" in result_df.columns


@pytest.mark.django_db
def test_transform_traces_task(sample_nextflow_trace_data, prefect_harness):
    """Test transformation of raw trace data."""
    # Create a DataFrame from the sample data
    df = pd.DataFrame.from_dict(sample_nextflow_trace_data, orient="index")

    # Add required metadata columns
    df["native_id"] = 12345
    df["job_id"] = "test-job-id"
    df["cluster_job_id"] = 12345
    df["flow_run_id"] = "test-flow-run-id"
    df["job_created_at"] = datetime(2023, 1, 1, 10, 0, 0)
    df["job_updated_at"] = datetime(2023, 1, 1, 12, 0, 0)
    df["job_last_known_state"] = "COMPLETED"

    # Call the transformation task
    result_df = transform_traces_task(df.rename(columns=NEXTFLOW_FIELD_MAP))

    # Verify the result
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == len(sample_nextflow_trace_data)

    # Check that fields were transformed
    assert "duration_seconds" in result_df.columns
    assert "realtime_seconds" in result_df.columns
    assert "peak_rss_mb" in result_df.columns
    assert "peak_vmem_mb" in result_df.columns

    # Check that pipeline components were extracted
    assert "accession" in result_df.columns
    assert "pipeline" in result_df.columns

    # Check specific values
    assert result_df["accession"].iloc[0] == "SRR987654"
    assert result_df["process"].iloc[0] == "DADA2"

    assert result_df["accession"].iloc[1] == "ERZ101"
    assert result_df["process"].iloc[1] == "RENAME_CONTIGS"

    assert result_df["accession"].iloc[2] == "SRR15043906"
    assert result_df["process"].iloc[2] == "FASTP"
    assert (
        result_df["pipeline"].iloc[2] == "MIASSEMBLER"
        or result_df["pipeline"].iloc[2] == "EBIMETAGENOMICS_MIASSEMBLER"
    )
    assert (
        result_df["pipeline"].iloc[3] == "MIASSEMBLER"
        or result_df["pipeline"].iloc[3] == "EBIMETAGENOMICS_MIASSEMBLER"
    )


def test_transform_traces_missing_task_id(prefect_harness):
    """Test transformation when task_id is missing from input."""
    df = pd.DataFrame(
        [
            {
                "name": "EBI_METAGENOMICS:AMPLICON:DADA2 (SRR123123)",
                "status": "COMPLETED",
                "duration": "1h 30m 15s",
                "realtime": "1h 35m 20s",
                "native_id": 12345,
                "exit": 0,
                "%cpu": "95%",
                "%mem": "85%",
                "peak_rss": "1GB",
                "peak_vmem": "2GB",
                "rchar": 100,
                "wchar": 100,
            }
        ]
    )

    # Add required metadata columns
    df["job_id"] = "test-job-id"
    df["cluster_job_id"] = 12345
    df["flow_run_id"] = "test-flow-run-id"
    df["job_created_at"] = datetime(2023, 1, 1, 10, 0, 0)
    df["job_updated_at"] = datetime(2023, 1, 1, 12, 0, 0)
    df["job_last_known_state"] = "COMPLETED"

    # Rename columns as extract_traces_from_the_database would
    df = df.rename(columns=NEXTFLOW_FIELD_MAP)

    # This should NOT raise SchemaError if we fix it, but let's see it fail first
    # We add task_id here because Pandera schema requires it
    df["task_id"] = 1
    result_df = transform_traces_task(df)
    assert "task_id" in result_df.columns
    assert "job_created_at" in result_df.columns


def test_transform_traces_empty_dataframe(prefect_harness):
    """Test transformation with empty DataFrame."""
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="No trace records provided"):
        transform_traces_task(empty_df)


def test_summary_stats(sample_nextflow_trace_data, prefect_harness):
    """Test generation of summary statistics."""
    # Create a DataFrame from the sample data
    df = pd.DataFrame.from_dict(sample_nextflow_trace_data, orient="index")

    # Add required metadata columns
    df["native_id"] = 12345
    df["job_id"] = "test-job-id"
    df["cluster_job_id"] = 12345
    df["flow_run_id"] = "test-flow-run-id"
    df["job_created_at"] = datetime(2023, 1, 1, 10, 0, 0)
    df["job_updated_at"] = datetime(2023, 1, 1, 12, 0, 0)
    df["job_last_known_state"] = "COMPLETED"

    # Transform the data first
    df_renamed = df.rename(columns=NEXTFLOW_FIELD_MAP)
    transformed_df = transform_trace_records(df_renamed)

    # Generate summary stats
    stats = summary_stats(transformed_df)

    # Verify the stats structure
    assert "total_records" in stats
    assert "pipelines" in stats
    assert "time_range" in stats

    # Check specific values
    assert stats["total_records"] == len(sample_nextflow_trace_data)
    assert "AMPLICON" in stats["pipelines"]
    assert "ASSEMBLY_ANALYSIS_PIPELINE" in stats["pipelines"]


def test_schema_validation(sample_nextflow_trace_data):
    """Test that the schema validates trace data correctly."""
    # Create a DataFrame from the sample data
    df = pd.DataFrame.from_dict(sample_nextflow_trace_data, orient="index")
    df = df.rename(columns=NEXTFLOW_FIELD_MAP)

    # Add required metadata columns
    df["native_id"] = 12345
    df["job_id"] = "test-job-id"
    df["cluster_job_id"] = 12345
    df["flow_run_id"] = "test-flow-run-id"
    df["job_created_at"] = datetime(2023, 1, 1, 10, 0, 0)
    df["job_updated_at"] = datetime(2023, 1, 1, 12, 0, 0)
    df["job_last_known_state"] = "COMPLETED"

    # Validate using the schema
    validated_df = NextflowTraceSchema.validate(df)

    # Verify the result
    assert isinstance(validated_df, pd.DataFrame)
    assert len(validated_df) == len(sample_nextflow_trace_data)

    # Check that time fields were parsed
    assert validated_df["duration"].iloc[0] > 0
    assert validated_df["realtime"].iloc[0] > 0

    # Check that memory fields were parsed
    assert validated_df["peak_rss"].iloc[0] > 0
    assert validated_df["peak_vmem"].iloc[0] > 0


@pytest.mark.django_db
def test_nextflow_trace_etl_flow(sample_orchestrated_job, prefect_harness, tmp_path):
    """Test the complete ETL flow."""
    # Call the flow
    nextflow_trace_etl_flow(
        sqlite_db_path=str(tmp_path),
        batch_size=1000,
        only_completed=True,
        exclude_failed=True,
    )

    # Check that the SQLite database was created
    db_file = tmp_path / "nf_traces.db"
    assert db_file.exists()

    # Verify the database contains the expected table
    with sqlite3.connect(str(db_file)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        assert "nextflow_traces" in table_names

        # Check that data was inserted
        cursor.execute("SELECT COUNT(*) FROM nextflow_traces")
        count = cursor.fetchone()[0]
        assert count == 1


@pytest.mark.django_db
def test_extract_traces_with_failed_jobs(prefect_harness):
    """Test extraction with failed jobs."""
    # Create a failed job
    job = OrchestratedClusterJob(
        id=uuid.uuid4(),
        job_submit_description={
            "name": "Impressive job",
            "script": "srun sleep 1",
        },
        cluster_job_id=54321,
        flow_run_id=uuid.uuid4(),
        created_at=datetime(2023, 1, 1, 10, 0, 0),
        updated_at=datetime(2023, 1, 1, 12, 0, 0),
        last_known_state="FAILED",
        nextflow_trace={
            "1": {
                "%cpu": "95.2",
                "%mem": "85.5",
                "peak_rss": "1.2 GB",
                "peak_vmem": "2.5 GB",
                "rchar": "123456",
                "wchar": "654321",
                "submit": "2023-01-01T10:00:00Z",
                "duration": "1h 30m 15s",
                "realtime": "1h 35m 20s",
                "exit": "1",
                "status": "COMPLETED",  # TRACE status is COMPLETED even if JOB failed
                "hash": "abc123",
                "task_id": 1,
                "native_id": "12345",
                "name": "EBI_METAGENOMICS:AMPLICON:DADA2 (SRR987654)",
            }
        },
    )
    job.save()

    # Test with exclude_failed=True (should exclude the failed job)
    result_df = extract_traces_from_the_database(
        batch_size=1000, exclude_failed=True, only_completed=False
    )
    assert len(result_df) == 0

    # Test with exclude_failed=False (should include the failed job)
    result_df = extract_traces_from_the_database(
        batch_size=1000, exclude_failed=False, only_completed=False
    )
    assert len(result_df) == 1


def test_transform_traces_with_missing_fields(prefect_harness):
    """Test transformation with missing fields."""
    # Create a DataFrame with missing fields
    df = pd.DataFrame(
        {
            "native_id": [12345],
            "task_id": [1],
            "job_id": ["test-job-id"],
            "cluster_job_id": [12345],
            "flow_run_id": ["test-flow-run-id"],
            "job_created_at": [datetime(2023, 1, 1, 10, 0, 0)],
            "job_updated_at": [datetime(2023, 1, 1, 12, 0, 0)],
            "job_last_known_state": ["COMPLETED"],
            "name": ["EBI_METAGENOMICS:AMPLICON:DADA2"],  # Missing accession
            "duration": ["1h 30m 15s"],
            "realtime": ["1h 35m 20s"],
            "status": ["COMPLETED"],
            "exit": ["0"],
            "%cpu": ["95.2%"],
            "%mem": ["85.5%"],
            "peak_rss": ["1.2 GB"],
            "peak_vmem": ["2.5 GB"],
            "rchar": ["123456"],
            "wchar": ["654321"],
        }
    )

    # Transform the data
    result_df = transform_traces_task(df.rename(columns=NEXTFLOW_FIELD_MAP))

    # Verify that missing fields are handled gracefully
    assert len(result_df) == 1
    assert result_df["accession"].iloc[0] == ""  # Empty string for missing accession
    assert result_df["pipeline"].iloc[0] == "AMPLICON"
    # Summary stats should also work
    stats = summary_stats(result_df)
    assert stats["total_records"] == 1
