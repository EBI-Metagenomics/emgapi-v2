from typing import Any
from datetime import datetime
import pandas as pd
import numpy as np
import pandera.pandas as pa
from pandera.typing import Series
from parse import search


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

MEMORY_UNITS = {
    "B": 1 / (1024 * 1024),
    "KB": 1 / 1024,
    "MB": 1,
    "GB": 1024,
    "TB": 1024 * 1024,
}


class NextflowTraceSchema(pa.DataFrameModel):
    """
    Pandera schema for Nextflow trace data.
    Provides validation and automated data cleaning for trace records.
    """

    native_id: Series[int] = pa.Field(nullable=True, coerce=True)
    task_id: Series[int] = pa.Field(nullable=True, coerce=True)
    task_name: Series[str] = pa.Field(nullable=True, coerce=True)
    status: Series[str] = pa.Field(nullable=True, coerce=True)
    exit_code: Series[float] = pa.Field(nullable=True, coerce=True)

    # Timing fields
    duration: Series[Any] = pa.Field(nullable=True)
    realtime: Series[Any] = pa.Field(nullable=True)

    # Orchestrated job metadata - the actual slurm job (and prefect flow)
    job_id: Series[str] = pa.Field(nullable=True, coerce=True)
    cluster_job_id: Series[int] = pa.Field(nullable=True, coerce=True)
    flow_run_id: Series[str] = pa.Field(nullable=True, coerce=True)
    job_created_at: Series[datetime] = pa.Field(nullable=True, coerce=True)
    job_updated_at: Series[datetime] = pa.Field(nullable=True, coerce=True)
    job_last_known_state: Series[str] = pa.Field(nullable=True, coerce=True)

    @pa.parser("duration", "realtime")
    def parse_time(cls, series: Series[Any]) -> Series[float]:
        # Handle cases where the whole series might be non-string
        return series.apply(
            lambda x: parse_time_to_seconds(str(x)) if not pd.isna(x) else 0.0
        ).astype(float)

    # Resource metrics - strings in raw data, converted to floats/MB
    cpu_percent: Series[float] = pa.Field(nullable=True)
    mem_percent: Series[float] = pa.Field(nullable=True)
    peak_rss: Series[Any] = pa.Field(nullable=True)
    peak_vmem: Series[Any] = pa.Field(nullable=True)

    rchar: Series[float] = pa.Field(nullable=True)
    wchar: Series[float] = pa.Field(nullable=True)

    @pa.parser("cpu_percent", "mem_percent")
    def parse_percentage(cls, series: Series[Any]) -> Series[float]:
        return series.apply(
            lambda x: float(x.strip("%")) if not pd.isna(x) else 0.0
        ).astype(float)

    @pa.parser("rchar", "wchar", "peak_rss", "peak_vmem")
    def parse_memory(cls, series: Series[Any]) -> Series[float]:
        # Handle cases where the whole series might be non-string
        return series.apply(
            lambda x: parse_memory_to_mb(str(x)) if not pd.isna(x) else 0.0
        ).astype(float)

    class Config:
        strict = False


def parse_time_to_seconds(time_str: str) -> float:
    """
    Transforms a Nextflow trace time string into its equivalent duration in seconds.
    TODO: There is room to improve this one using the prase library.
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


def preprocess_trace_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Common preprocessing for Nextflow trace DataFrames:
    - Renames fields using NEXTFLOW_FIELD_MAP
    - Cleans '-' and empty strings to NaN

    :param df: Input DataFrame
    :return: Preprocessed DataFrame
    """
    if df.empty:
        return df

    # Map fields to schema names
    df.rename(columns=NEXTFLOW_FIELD_MAP, inplace=True)

    # Clean '-' and empty strings
    df.replace(["-", ""], np.nan, inplace=True)

    return df


def parse_memory_to_mb(memory_val: Any) -> float:
    """
    Converts a memory string into its equivalent value in Megabytes (MB).
    It is used to parse memory strings like "99.7 MB", "1.3 GB", or "1.1 TB" into MB
    :param memory_val: A string representation of memory (e.g., "1.3 GB", "1024 KB").
    :return: The memory value in MB.
    """
    if pd.isna(memory_val):
        return 0.0
    if isinstance(memory_val, (int, float)):
        return round(float(memory_val) * MEMORY_UNITS["B"], 1)

    # Some defensive code against _stuff_ in the field
    memory_str = str(memory_val).strip()
    if not memory_str or memory_str == "-":
        return 0.0

    # Pattern to match a number followed by an optional space and a unit
    # {val:f} matches a float and {unit} matches the rest
    res = search("{val:g} {unit:w}", memory_str)

    if res:
        val = res["val"]
        unit = res["unit"].upper()
        return val * MEMORY_UNITS.get(unit, 1.0)

    # Try parsing as plain float if no unit matched
    try:
        # If it is only the value, then we assume it is in Bytes
        return round(float(memory_val) * MEMORY_UNITS["B"], 1)
    except ValueError:
        return 0.0
