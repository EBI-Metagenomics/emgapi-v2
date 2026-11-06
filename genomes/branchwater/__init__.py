"""Branchwater search integration.

The package exports the established integration surface while keeping transport,
metadata enrichment, signature handling, and orchestration in focused modules.
"""

from .client import (
    _branchwater_base_url,
    _check_branchwater_health,
    _compress_branchwater_query_payload,
    _parse_branchwater_csv_results,
    _search_branchwater,
)
from .errors import BranchwaterError
from .metadata import (
    BRANCHWATER_METADATA_FIELDS,
    _add_mgnify_run_flags,
    _join_branchwater_metadata,
)
from .service import process_branchwater_search
from .signatures import (
    _load_branchwater_mag_signature,
    validate_branchwater_signature,
    write_branchwater_json,
)

__all__ = [
    "BRANCHWATER_METADATA_FIELDS",
    "BranchwaterError",
    "_add_mgnify_run_flags",
    "_branchwater_base_url",
    "_check_branchwater_health",
    "_compress_branchwater_query_payload",
    "_join_branchwater_metadata",
    "_load_branchwater_mag_signature",
    "_parse_branchwater_csv_results",
    "_search_branchwater",
    "process_branchwater_search",
    "validate_branchwater_signature",
    "write_branchwater_json",
]
