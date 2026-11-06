"""HTTP client and wire-format handling for the Branchwater Rust service."""

import csv
import gzip
import io
import json
import logging
from typing import Any, Dict, List

import httpx
from django.conf import settings

from .errors import BranchwaterError

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


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
        accession = (
            row.get("SRA accession") or row.get("SRA_accession") or row.get("acc")
        )
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
            timeout=EMG_CONFIG.branchwater.search_timeout_seconds,
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
