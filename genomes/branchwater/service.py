"""Application-level orchestration for a staged Branchwater search."""

import json
from pathlib import Path
from typing import Any

from .client import _search_branchwater
from .metadata import _add_mgnify_run_flags, _join_branchwater_metadata
from .signatures import write_branchwater_json


def process_branchwater_search(query_path: str, result_path: str) -> dict[str, Any]:
    """Execute and enrich a staged search in a worker, keeping large data off the queue."""
    with Path(query_path).open(encoding="utf-8") as handle:
        signatures = json.load(handle)
    results = _search_branchwater(signatures, use_precomputed_sketches=True)
    results = _join_branchwater_metadata(results)
    results = _add_mgnify_run_flags(results)
    write_branchwater_json(Path(result_path), results)
    return {
        "status": "SUCCESS" if results else "NO_RESULTS",
        "result_path": result_path,
        "result_count": len(results),
        "reason": None,
    }
