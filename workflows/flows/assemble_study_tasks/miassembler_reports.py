import json
from pathlib import Path
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series


class AssembledRunsReport(pa.DataFrameModel):
    """
    Schema for miassembler rows describing successfully assembled runs.
    """

    run_accession: Series[str] = pa.Field(nullable=False)
    assembler_name: Series[str] = pa.Field(nullable=False)
    assembler_version: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True


class QcFailedRunsReport(pa.DataFrameModel):
    """
    Schema for miassembler rows describing runs that failed pre-assembly QC.
    """

    run_accession: Series[str] = pa.Field(nullable=False)
    reason: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True


def read_assembled_runs_report(path: Path) -> DataFrame[AssembledRunsReport]:
    """
    Read and validate miassembler's assembled_runs.csv report.
    """
    try:
        return AssembledRunsReport.validate(
            pd.read_csv(
                path,
                header=None,
                names=AssembledRunsReport.to_schema().columns,
                dtype=str,
            )
        )
    except pd.errors.EmptyDataError:
        return AssembledRunsReport.validate(
            pd.DataFrame(columns=AssembledRunsReport.to_schema().columns)
        )


def read_qc_failed_runs_report(path: Path) -> DataFrame[QcFailedRunsReport]:
    """
    Read and validate miassembler's qc_failed_runs.csv report.
    """
    try:
        return QcFailedRunsReport.validate(
            pd.read_csv(
                path,
                header=None,
                names=QcFailedRunsReport.to_schema().columns,
                dtype=str,
            )
        )
    except pd.errors.EmptyDataError:
        return QcFailedRunsReport.validate(
            pd.DataFrame(columns=QcFailedRunsReport.to_schema().columns)
        )


def read_coverage_report(path: Path) -> Any:
    """
    Read miassembler's assembly coverage JSON report.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Expected miassembler coverage report at {path}")

    with path.open() as json_file:
        return json.load(json_file)
