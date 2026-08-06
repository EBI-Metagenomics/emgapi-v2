import csv
import gzip
import io
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import httpx
from django.conf import settings
from ninja import Query, Schema
from ninja.errors import HttpError
from ninja_extra import api_controller, http_get, http_post

from analyses.models import Run
from emgapiv2.api.schema_utils import ApiSections

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


class BranchwaterError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class BranchwaterSearchIn(Schema):
    signatures: str


class BranchwaterHealthOut(Schema):
    status: str


def _branchwater_base_url() -> str:
    return EMG_CONFIG.branchwater.index_server.rstrip("/")


def _compress_branchwater_query_payload(
    signatures: Any, *, use_precomputed_sketches: bool = False
) -> bytes:
    if use_precomputed_sketches:
        json_bytes = json.dumps(signatures).encode("utf-8")
    else:
        sig_str = str(signatures).translate({ord(c): None for c in "\t\n\r "})
        json_bytes = f"[{sig_str}]".encode("utf-8")

    buffer = io.BytesIO()
    with gzip.open(buffer, "wb") as compressed:
        compressed.write(json_bytes)
    return buffer.getvalue()


def _parse_branchwater_csv_results(csv_text: str) -> List[Dict[str, Any]]:
    threshold = EMG_CONFIG.branchwater.threshold
    ksize = EMG_CONFIG.branchwater.ksize
    results: List[Dict[str, Any]] = []

    for row in csv.DictReader(io.StringIO(csv_text)):
        accession = row.get("SRA_accession") or row.get("acc")
        containment_raw = row.get("containment")
        if not accession or containment_raw in (None, ""):
            continue

        containment = float(containment_raw)
        if containment < threshold:
            continue

        results.append(
            {
                "acc": accession,
                "containment": containment,
                "cANI": containment ** (1.0 / ksize),
            }
        )

    return results


def _search_branchwater(
    signatures: Any, *, use_precomputed_sketches: bool = False
) -> List[Dict[str, Any]]:
    url = f"{_branchwater_base_url()}/search"
    compressed_payload = _compress_branchwater_query_payload(
        signatures, use_precomputed_sketches=use_precomputed_sketches
    )

    try:
        response = httpx.post(
            url,
            content=compressed_payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        logger.error(
            "Branchwater backend returned %s: %s", status, exc.response.text[:500]
        )
        if status >= 500:
            raise BranchwaterError(
                502, "Branchwater backend is unavailable. Please try later."
            ) from exc
        raise BranchwaterError(
            400, "Branchwater request was rejected by the backend."
        ) from exc
    except httpx.RequestError as exc:
        logger.exception("Failed to reach Branchwater backend at %s", url)
        raise BranchwaterError(
            503, "Branchwater is temporarily unavailable. Please try later."
        ) from exc

    return _parse_branchwater_csv_results(response.text)


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
            if row.get(field) is None:
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


def _load_branchwater_mag_signature(accession: str, catalogue: str) -> Any:
    signature_path = (
        Path(EMG_CONFIG.branchwater.signatures_root)
        / catalogue
        / f"{accession}.fna.sig"
    )
    if not signature_path.exists():
        raise HttpError(
            404,
            f"No .sig file found for accession {accession} in catalogue {catalogue}.",
        )

    try:
        with signature_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.exception(
            "Failed to load Branchwater MAG signature from %s", signature_path
        )
        raise HttpError(400, "Could not read or parse the signature file.") from exc


def _check_branchwater_health() -> None:
    url = f"{_branchwater_base_url()}/health"
    try:
        response = httpx.get(
            url,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
    except httpx.RequestError as exc:
        logger.exception("Failed to reach Branchwater health endpoint at %s", url)
        raise BranchwaterError(
            503, "Branchwater healthcheck is temporarily unavailable."
        ) from exc
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Branchwater health endpoint returned %s: %s",
            exc.response.status_code,
            exc.response.text[:500],
        )
        raise BranchwaterError(
            503, "Branchwater healthcheck is temporarily unavailable."
        ) from exc


@api_controller("services/branchwater", tags=[ApiSections.GENOMES])
class BranchwaterController:
    @http_post(
        "/",
        response=List[Dict[str, Any]],
        summary="Search public metagenomes with Branchwater using a precomputed signature",
        operation_id="branchwater_search",
    )
    def search(self, payload: BranchwaterSearchIn):
        try:
            results = _search_branchwater(payload.signatures)
            results = _join_branchwater_metadata(results)
            results = _add_mgnify_run_flags(results)
        except BranchwaterError as exc:
            raise HttpError(exc.status_code, exc.message) from exc
        return results

    @http_post(
        "/mags",
        response=List[Dict[str, Any]],
        summary="Search public metagenomes using a precomputed MAG signature",
        operation_id="branchwater_search_mags",
    )
    def mags(
        self,
        accession: str = Query(...),
        catalogue: str = Query(...),
    ):
        signature_payload = _load_branchwater_mag_signature(accession, catalogue)
        try:
            results = _search_branchwater(
                signature_payload, use_precomputed_sketches=True
            )
            results = _join_branchwater_metadata(results)
            results = _add_mgnify_run_flags(results)
        except BranchwaterError as exc:
            raise HttpError(exc.status_code, exc.message) from exc
        return results

    @http_get(
        "/health",
        response=BranchwaterHealthOut,
        summary="Healthcheck for the integrated Branchwater service",
        operation_id="branchwater_health",
    )
    def health(self):
        try:
            _check_branchwater_health()
        except BranchwaterError as exc:
            raise HttpError(exc.status_code, exc.message) from exc
        return BranchwaterHealthOut(status="ok")
