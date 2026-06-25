from __future__ import annotations

import uuid
from typing import Iterable

import pandas as pd
from pandas.api.types import is_scalar

SUMMARY_SCHEMA_VERSION = 1

ID_NAMESPACE = uuid.UUID("6f4d03fd-4d33-5a5d-8dc3-ff7a42d98b45")

TAXONOMY_COLUMNS = [
    "superkingdom",
    "domain",
    "kingdom",
    "supergroup",
    "phylum",
    "division",
    "subdivision",
    "class",
    "order",
    "family",
    "genus",
    "species",
]

CANONICAL_COLUMNS = [
    "summary_schema_version",
    "study_accession",
    "ena_study_accession",
    "analysis_accession",
    "pipeline_version",
    "experiment_type",
    "sample_accession",
    "run_accession",
    "event_id",
    "event_guid",
    "occurrence_id",
    "analysis_method",
    "reference_database",
    "amplified_region",
    "longitude",
    "latitude",
    "depth",
    "temperature",
    "salinity",
    "collection_date",
    "country",
    "institution_code",
    "sequencing_method",
    "sample_metadata",
    *TAXONOMY_COLUMNS,
    "scientific_name",
    "taxon_id",
    "measurement_value",
    "measurement_unit",
    "asv_id",
    "asv_sequence",
    "asv_caller",
    "dbhit",
    "dbhit_identity",
    "dbhit_start",
    "dbhit_end",
]


def normalise_empty(value: object) -> object:
    """Normalize empty scalar values to pandas nulls.

    :param value: Scalar value from database metadata or pipeline output.
    :return: ``pd.NA`` for empty/null sentinel values, otherwise the original value.
    :raises TypeError: If ``value`` is not scalar.
    """
    if value is None:
        return pd.NA
    if not is_scalar(value):
        raise TypeError(f"Expected scalar value, got {type(value).__name__}")
    if pd.isna(value):
        return pd.NA
    if value == "":
        return pd.NA
    if isinstance(value, str) and value.strip().upper() in {"NA", "NAN", "NONE"}:
        return pd.NA
    return value


def strip_taxon_prefix(value: object) -> object:
    """Strip taxonomy rank prefixes from a taxon label.

    :param value: Taxon value such as ``sk__Bacteria`` or ``g__Bradyrhizobium``.
    :return: Unprefixed taxon value, or ``pd.NA`` when the input is empty.
    """
    value = normalise_empty(value)
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if "__" in text:
        _, _, suffix = text.partition("__")
        return normalise_empty(suffix)
    return normalise_empty(text)


def deepest_taxon_name(row: pd.Series) -> object:
    """Return the deepest populated taxonomic rank for a canonical row.

    :param row: Canonical summary row containing taxonomy rank columns.
    :return: Deepest non-null rank value, or ``pd.NA`` when no ranks are populated.
    """
    for column in reversed(TAXONOMY_COLUMNS):
        value = row.get(column)
        if pd.notna(value) and str(value).strip():
            return value
    return pd.NA


def coerce_taxon_id(value: object) -> int | pd.NA:
    """Coerce a taxon identifier to a numeric NCBI taxid.

    :param value: Candidate taxon identifier from an OTU file or result table.
    :return: Integer taxid when numeric, otherwise ``pd.NA``.
    """
    value = normalise_empty(value)
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if not text.isdigit():
        return pd.NA
    return int(text)


def make_uuid(name_parts: Iterable[object]) -> str:
    """Build a UUIDv5 string from stable identifier components.

    Example:
        ``make_uuid(["event", "MGYS00000001", "SAMEA1", "ERR1", "v6"])``
        always returns the same UUID for the same ordered components.

    Example:
        ``make_uuid(["occurrence", "MGYA00000001", None, pd.NA])`` treats
        ``None`` and ``pd.NA`` as empty UUID-name components.

    :param name_parts: Ordered components that make up the UUID name.
    :return: UUIDv5 string generated in the fixed amplicon summary namespace.
    """
    normalised_parts = [
        "" if part is None or pd.isna(part) else str(part) for part in name_parts
    ]
    return str(uuid.uuid5(ID_NAMESPACE, "|".join(normalised_parts)))


def make_event_id(
    study_accession: str,
    sample_accession: str,
    run_accession: str,
    pipeline_version: str,
) -> str:
    """Build a human-readable event identifier.

    :param study_accession: MGnify study accession.
    :param sample_accession: Sample accession associated with the run.
    :param run_accession: Run accession represented by the event.
    :param pipeline_version: Pipeline version that produced the event.
    :return: Stable composite event identifier.
    """
    return f"{study_accession}:{sample_accession}:{run_accession}:{pipeline_version}"


def ensure_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with all canonical columns in public schema order.

    :param df: Dataframe containing zero or more canonical columns.
    :return: Copy of ``df`` with missing canonical columns filled with ``pd.NA``.
    """
    canonical = df.copy()
    for column in CANONICAL_COLUMNS:
        if column not in canonical.columns:
            canonical[column] = pd.NA
    return canonical[CANONICAL_COLUMNS]
