import re
from pathlib import Path

import pandas as pd

from workflows.flows.analysis.summaries.amplicon.shared.schema import (
    ensure_canonical_columns,
)

RAW_COMMON_OUTPUT_COLUMNS = [
    "analysis_method",
    "reference_database",
    "amplified_region",
    "StudyID",
    "SampleID",
    "RunID",
    "decimalLongitude",
    "decimalLatitude",
    "depth",
    "temperature",
    "salinity",
    "collectionDate",
    "seq_meth",
    "country",
    "InstitutionCode",
    "ASVID",
    "ASVCaller",
    "ReferenceDatabase",
    "TaxAnnotationTool",
    "taxonID",
    "MeasurementUnit",
    "MeasurementValue",
    "dbhit",
    "dbhitIdentity",
    "dbhitStart",
    "dbhitEnd",
    "ASVSeq",
    "amplifiedRegion",
]

RAW_TAXONOMY_OUTPUT_COLUMNS = [
    "Superkingdom",
    "Domain",
    "Kingdom",
    "Supergroup",
    "Phylum",
    "Division",
    "Subdivision",
    "Class",
    "Order",
    "Family",
    "Genus",
    "Species",
]

RAW_OUTPUT_COLUMNS = RAW_COMMON_OUTPUT_COLUMNS + RAW_TAXONOMY_OUTPUT_COLUMNS

FIELD_NAME_OVERRIDES = {
    "ASVID": "asv_id",
    "ASVSeq": "asv_seq",
}


def normalise_field_name(field_name: str) -> str:
    """Convert a raw field name into snake_case.

    :param field_name: Raw output field name.
    :return: Normalized snake_case field name.
    """
    if field_name in FIELD_NAME_OVERRIDES:
        return FIELD_NAME_OVERRIDES[field_name]

    normalised = re.sub(r"[^0-9A-Za-z]+", "_", field_name)
    normalised = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", normalised)
    normalised = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalised)
    return normalised.strip("_").lower()


OUTPUT_COLUMNS = list(
    dict.fromkeys(normalise_field_name(column) for column in RAW_OUTPUT_COLUMNS)
)


def ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return v5/v6 study-summary parquet columns in stable output order.

    :param df: Input dataframe with raw or already-normalized column names.
    :return: Dataframe with stable v5/v6 parquet output columns.
    """
    standardised = df.copy()
    for column in RAW_OUTPUT_COLUMNS:
        if column not in standardised.columns:
            standardised[column] = pd.NA

    output = pd.DataFrame(index=standardised.index)
    for output_column in OUTPUT_COLUMNS:
        candidate_columns = [
            column
            for column in RAW_OUTPUT_COLUMNS
            if normalise_field_name(column) == output_column
        ]
        output[output_column] = standardised[candidate_columns].bfill(axis=1).iloc[:, 0]
    return output[OUTPUT_COLUMNS]


def write_parquet_summary(df: pd.DataFrame, output_prefix: str) -> Path:
    """Write the v5/v6 study-summary parquet file.

    :param df: Input dataframe to normalize and write.
    :param output_prefix: Output path prefix before ``_taxonomic_results.parquet``.
    :return: Path to the written parquet file.
    """
    output_path = Path(f"{output_prefix}_taxonomic_results.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_output_columns(df).to_parquet(
        output_path,
        index=False,
    )
    return output_path


def write_amplicon_taxonomy_summary_parquet(
    df: pd.DataFrame,
    output_path: Path,
) -> Path:
    """Write a canonical amplicon taxonomy dataframe as parquet.

    :param df: Canonical amplicon taxonomy dataframe.
    :param output_path: Destination parquet path.
    :return: Path to the written parquet file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    canonical = ensure_canonical_columns(df)
    canonical.to_parquet(output_path, index=False)
    return output_path
