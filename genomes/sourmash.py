from __future__ import annotations

import csv
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

RESULT_FIELDNAMES = [
    "intersect_bp",
    "f_orig_query",
    "f_match",
    "f_unique_to_query",
    "f_unique_weighted",
    "average_abund",
    "median_abund",
    "std_abund",
    "name",
    "filename",
    "md5",
    "f_match_orig",
    "unique_intersect_bp",
    "gather_result_rank",
    "remaining_bp",
    "query_filename",
    "query_name",
    "query_md5",
    "query_bp",
]


@lru_cache(maxsize=4)
def _load_name_map(name_map_path: str) -> dict[str, str]:
    if not name_map_path:
        return {}

    path = Path(name_map_path)
    if not path.exists():
        logger.warning("Sourmash name-map file does not exist: %s", path)
        return {}

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _strip_known_suffixes(filename: str) -> str:
    name = Path(filename).name
    known_suffixes = {".sig", ".fa", ".fna", ".gz"}

    while True:
        stem, suffix = os.path.splitext(name)
        if not suffix or suffix.lower() not in known_suffixes or not stem:
            return name
        name = stem


def get_accession_from_filename(filename: str, *, name_map_path: str = "") -> str:
    accession = _strip_known_suffixes(filename)
    if accession.startswith("GUT_"):
        return _load_name_map(name_map_path).get(accession, accession)
    return accession


def run_sourmash_gather(
    *,
    query_path: str,
    original_filename: str,
    catalogue_id: str,
    artifact_path: str,
    result_path: str,
    ksize: int = 31,
    moltype: str = "DNA",
    threshold_bp: float = 5e4,
    ignore_abundance: bool = False,
    name_map_path: str = "",
) -> dict[str, Any]:
    from sourmash.commands import SaveSignaturesToLocation
    from sourmash.search import format_bp, gather_databases
    from sourmash.sourmash_args import (
        FileOutputCSV,
        get_moltype,
        load_dbs_and_sigs,
        load_query_signature,
    )

    # log all received params
    logger.info("Loading sourmash query signature from %s", query_path)
    logger.info("Using original filename %s", original_filename)
    logger.info("Using catalogue %s", catalogue_id)
    logger.info("Using artifact %s", artifact_path)
    logger.info("Using result path %s", result_path)
    logger.info("Using ksize %s", ksize)
    logger.info("Using moltype %s", moltype)
    logger.info("Using threshold_bp %s", threshold_bp)
    logger.info("Using ignore_abundance %s", ignore_abundance)
    logger.info("Using name_map_path %s", name_map_path)

    query_file = Path(query_path)
    if not query_file.exists():
        raise FileNotFoundError(
            f"Sourmash query signature does not exist: {query_file}"
        )

    search_artifact = Path(artifact_path)
    if not search_artifact.exists():
        raise FileNotFoundError(
            f"Sourmash search artifact does not exist: {search_artifact}"
        )

    logger.info("Loading sourmash query signature from %s", query_file)
    query = load_query_signature(
        str(query_file),
        ksize=ksize,
        select_moltype=moltype,
    )
    logger.info(
        "Loaded sourmash query %s (k=%s, moltype=%s)",
        original_filename,
        query.minhash.ksize,
        get_moltype(query),
    )

    if not query.minhash.scaled:
        raise ValueError("Query signature needs to be created with --scaled")

    if not len(query.minhash):
        raise ValueError("The query signature does not contain any hashes")

    databases = load_dbs_and_sigs([str(search_artifact)], query, False, cache_size=None)
    if not databases:
        raise ValueError("No sourmash databases were available to search")

    prefetch_query = query.copy()
    prefetch_query.minhash = prefetch_query.minhash.flatten()
    save_prefetch = SaveSignaturesToLocation(None)
    save_prefetch.open()

    counters = []
    for database in databases:
        counter = database.counter_gather(prefetch_query, threshold_bp)
        save_prefetch.add_many(counter.siglist)
        counters.append(counter)

    logger.info("Prefetch found %s signatures; starting gather", len(save_prefetch))
    save_prefetch.close()

    found = []
    first_match = None
    gather_iter = gather_databases(query, counters, threshold_bp, ignore_abundance)

    for result, _weighted_missed, _next_query in gather_iter:
        pct_query = "{:.1f}%".format(result.f_unique_weighted * 100)
        pct_genome = "{:.1f}%".format(result.f_match * 100)
        match_name = result.match.filename
        if not found:
            first_match = {
                "overlap": format_bp(result.intersect_bp),
                "p_query": pct_query,
                "p_match": pct_genome,
                "match": get_accession_from_filename(
                    match_name, name_map_path=name_map_path
                ),
                "catalog": catalogue_id,
                "query_filename": original_filename,
                "md5_name": query_file.name,
            }
        found.append(result)

    if found:
        output_path = Path(result_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with FileOutputCSV(str(output_path)) as file_pointer:
            writer = csv.DictWriter(
                file_pointer,
                fieldnames=RESULT_FIELDNAMES,
                extrasaction="ignore",
            )
            writer.writeheader()
            for result in found:
                row = dict(result._asdict())
                row.pop("match", None)
                writer.writerow(row)

        first_match["matches"] = len(found)
        return first_match

    return {
        "status": "NO_RESULTS",
        "catalog": catalogue_id,
        "query_filename": original_filename,
        "md5_name": query_file.name,
    }
