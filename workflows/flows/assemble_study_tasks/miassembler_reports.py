import json
from pathlib import Path
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

from workflows.ena_utils.ena_accession_matching import INSDC_RUN_ACCESSION_REGEX


class AssembledRunsReport(pa.DataFrameModel):
    """
    Schema for miassembler rows describing successfully assembled runs.
    """

    run_accession: Series[str] = pa.Field(
        nullable=False, str_matches=INSDC_RUN_ACCESSION_REGEX.pattern, unique=True
    )
    assembler_name: Series[str] = pa.Field(nullable=False)
    assembler_version: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True


class QcFailedRunsReport(pa.DataFrameModel):
    """
    Schema for miassembler rows describing runs that failed pre-assembly QC.
    """

    run_accession: Series[str] = pa.Field(
        nullable=False, str_matches=INSDC_RUN_ACCESSION_REGEX.pattern, unique=True
    )
    reason: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True


def load_assembled_runs_report(path: Path) -> DataFrame[AssembledRunsReport]:
    """
    Load and validate miassembler's assembled_runs.csv report.
    """
    try:
        report = AssembledRunsReport.validate(
            pd.read_csv(
                path,
                header=None,
                names=AssembledRunsReport.to_schema().columns,
                dtype=str,
            )
        )
    except pd.errors.EmptyDataError:
        report = AssembledRunsReport.validate(
            pd.DataFrame(columns=AssembledRunsReport.to_schema().columns)
        )
    return report


def load_qc_failed_runs_report(path: Path) -> DataFrame[QcFailedRunsReport]:
    """
    Load and validate miassembler's qc_failed_runs.csv report.
    """
    try:
        report = QcFailedRunsReport.validate(
            pd.read_csv(
                path,
                header=None,
                names=QcFailedRunsReport.to_schema().columns,
                dtype=str,
            )
        )
    except pd.errors.EmptyDataError:
        report = QcFailedRunsReport.validate(
            pd.DataFrame(columns=QcFailedRunsReport.to_schema().columns)
        )
    return report


def load_coverage_report(path: Path) -> Any:
    """
    Load miassembler's assembly coverage JSON report.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Expected miassembler coverage report at {path}")

    with path.open() as json_file:
        return json.load(json_file)
