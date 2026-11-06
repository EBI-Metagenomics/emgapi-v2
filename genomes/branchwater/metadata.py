"""Enrich Branchwater hits with archive and MGnify metadata."""

import logging
from pathlib import Path
from typing import Any, Dict, List

from django.conf import settings

from analyses.models import Run

from .errors import BranchwaterError

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG

BRANCHWATER_METADATA_FIELDS = (
    "bioproject",
    "assay_type",
    "collection_date_sam",
    "geo_loc_name_country_calc",
    "organism",
    "lat_lon",
)


def _join_branchwater_metadata(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not results:
        return []

    import duckdb
    import pandas as pd

    metadata_path = Path(EMG_CONFIG.branchwater.metadata_duckdb)
    if not metadata_path.exists():
        logger.error("Branchwater metadata DuckDB does not exist: %s", metadata_path)
        raise BranchwaterError(
            503, "Branchwater metadata database is unavailable. Please try later."
        )

    result_df = pd.DataFrame(results)
    metadata_columns = ", ".join(["acc", *BRANCHWATER_METADATA_FIELDS])
    query = f"""
        SELECT
            hits.acc,
            hits.containment,
            hits.cANI,
            metadata_subset.* EXCLUDE (acc)
        FROM hits
        LEFT JOIN (
            SELECT {metadata_columns}
            FROM metadata
        ) metadata_subset
        ON hits.acc = metadata_subset.acc
        ORDER BY hits.containment DESC
    """

    try:
        with duckdb.connect(database=str(metadata_path), read_only=True) as connection:
            connection.register("hits", result_df)
            joined = connection.execute(query).fetchdf()
    except Exception as exc:
        logger.exception("Failed to query Branchwater metadata DuckDB")
        raise BranchwaterError(
            503, "Branchwater metadata database is unavailable. Please try later."
        ) from exc

    rows = joined.to_dict(orient="records")
    for row in rows:
        for field in BRANCHWATER_METADATA_FIELDS:
            if pd.isna(row.get(field)):
                row[field] = "NP"
    return rows


def _add_mgnify_run_flags(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    accessions = {row.get("acc") for row in results if row.get("acc")}
    if not accessions:
        return results

    accession_to_visibility: Dict[str, bool] = {}
    matching_runs = (
        Run.objects.select_related(None)
        .filter(ena_accessions__overlap=list(accessions))
        .only("ena_accessions", "is_private")
    )
    for run in matching_runs:
        for accession in run.ena_accessions:
            if accession in accessions and accession not in accession_to_visibility:
                accession_to_visibility[accession] = run.is_private

    for row in results:
        accession = row.get("acc")
        if accession in accession_to_visibility:
            row["exists_on_mgnify"] = True
            row["is_private"] = accession_to_visibility[accession]
        else:
            row["exists_on_mgnify"] = False
            row["is_private"] = None
    return results
