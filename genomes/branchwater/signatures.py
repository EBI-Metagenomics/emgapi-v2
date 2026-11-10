"""Validation and filesystem handling for Branchwater JSON artifacts."""

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder

from .errors import BranchwaterError

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


def _load_branchwater_mag_signature(accession: str, catalogue: str) -> Any:
    if not all(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value)
        for value in (accession, catalogue)
    ):
        raise BranchwaterError(400, "Invalid MAG accession or catalogue.")
    signature_path = (
        Path(EMG_CONFIG.branchwater.signatures_root)
        / catalogue
        / f"{accession}.fna.sig"
    )
    if not signature_path.exists():
        raise BranchwaterError(
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
        raise BranchwaterError(
            400, "Could not read or parse the signature file."
        ) from exc


def validate_branchwater_signature(signatures: Any) -> list[dict[str, Any]]:
    """Validate a single sourmash signature using Python's exact integer parser."""
    try:
        payload = json.loads(signatures) if isinstance(signatures, str) else signatures
    except (TypeError, ValueError) as exc:
        raise BranchwaterError(400, "Could not parse the signature JSON.") from exc
    signatures_list = payload if isinstance(payload, list) else [payload]
    if len(signatures_list) != 1 or not isinstance(signatures_list[0], dict):
        raise BranchwaterError(400, "Provide exactly one sourmash signature.")
    sketches = signatures_list[0].get("signatures")
    if (
        not isinstance(sketches, list)
        or not sketches
        or not all(isinstance(sketch, dict) for sketch in sketches)
    ):
        raise BranchwaterError(400, "The file is not a valid sourmash signature.")
    compatible = [
        sketch
        for sketch in sketches
        if str(sketch.get("molecule", "")).lower() == "dna"
        and sketch.get("ksize") == EMG_CONFIG.branchwater.ksize
        and sketch.get("seed") == 42
    ]
    if not compatible:
        raise BranchwaterError(
            400,
            f"Provide a DNA signature with k={EMG_CONFIG.branchwater.ksize} and seed=42.",
        )
    for sketch in compatible:
        hashes = sketch.get("mins")
        max_hash = sketch.get("max_hash")
        if (
            not isinstance(hashes, list)
            or not hashes
            or any(
                type(value) is not int or not 0 <= value <= (2**64 - 1)
                for value in hashes
            )
            or type(max_hash) is not int
            or not 0 < max_hash <= (2**64 - 1)
        ):
            raise BranchwaterError(
                400,
                "The signature must contain unsigned 64-bit integer hashes and max_hash.",
            )
    return signatures_list


def write_branchwater_json(path: Path, payload: Any) -> None:
    """Publish a complete JSON file atomically on the shared jobs filesystem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".pending-",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, cls=DjangoJSONEncoder, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
