from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
import json

# Ensure Django is activated before any model imports
from activate_django_first import EMG_CONFIG  # noqa: F401

from django.db import transaction
from django.db.models import QuerySet
from prefect import flow, get_run_logger

from genomes.models import Genome


def _json_path_for_accession(base_dir: Path, accession: str) -> Path:
    """
    Build the expected JSON path for a genome accession:
    <base_dir>/<accession>/<accession>.json
    """
    return base_dir / accession / f"{accession}.json"


def _read_ncbi_from_json(json_path: Path) -> Optional[str]:
    """Read a JSON file and return the value of the 'ncbi_genome_accession' key if present.

    Returns None if the file cannot be read/parsed or the key is absent/empty.
    """
    try:
        with json_path.open("r") as fh:
            data = json.load(fh)
        val = data.get("ncbi_genome_accession")
        if val is None:
            return None
        # Normalize empty strings
        if isinstance(val, str) and val.strip() == "":
            return None
        return str(val)
    except Exception:
        return None


@flow(name="update_ena_accession_from_json_flow")
def update_ena_accession_from_json_flow(
    base_dir: str,
    read_chunk_size: int = 5000,
    update_batch_size: int = 2000,
) -> Dict[str, int]:
    """
    Traverse per-genome JSON files to update Genome.ena_genome_accession from the
    'ncbi_genome_accession' value found in each file.

    Parameters:
      - base_dir: directory containing one subdirectory per genome accession, each with
        a JSON file named <accession>.json
      - read_chunk_size: Django iterator chunk size when scanning genomes
      - update_batch_size: number of rows to bulk update at once

    Notes:
      - Designed to handle up to ~100k genomes using streaming DB and batched writes.
      - Missing files/keys or JSON parse issues are tracked and logged; processing continues.
    """
    logger = get_run_logger()
    root = Path(base_dir)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Base directory does not exist or is not a directory: {base_dir}")

    # Prepare counters
    total_seen = 0
    total_with_file = 0
    total_missing_file = 0
    total_missing_key_or_parse_error = 0
    total_to_update = 0
    total_updated = 0
    total_skipped_no_change = 0

    # We'll update in batches
    pending_updates: list[Genome] = []

    qs: QuerySet[Genome] = (
        Genome.objects.all()
        .only("genome_id", "accession", "ena_genome_accession")
        .order_by("genome_id")
    )

    logger.info(
        "Starting scan of genomes for JSON-based ena accession update; base_dir=%s",
        base_dir,
    )

    for genome in qs.iterator(chunk_size=read_chunk_size):
        total_seen += 1
        accession = genome.accession
        json_path = _json_path_for_accession(root, accession)

        if not json_path.exists():
            total_missing_file += 1
            continue

        total_with_file += 1
        ncbi = _read_ncbi_from_json(json_path)
        if not ncbi:
            total_missing_key_or_parse_error += 1
            continue

        new_ena = ncbi
        if genome.ena_genome_accession == new_ena:
            total_skipped_no_change += 1
            continue

        genome.ena_genome_accession = new_ena
        pending_updates.append(genome)
        total_to_update += 1

        if len(pending_updates) >= update_batch_size:
            with transaction.atomic():
                Genome.objects.bulk_update(
                    pending_updates, ["ena_genome_accession"], batch_size=update_batch_size
                )
            total_updated += len(pending_updates)
            pending_updates.clear()

    # Flush remainder
    if pending_updates:
        with transaction.atomic():
            Genome.objects.bulk_update(
                pending_updates, ["ena_genome_accession"], batch_size=update_batch_size
            )
        total_updated += len(pending_updates)
        pending_updates.clear()

    summary = {
        "total_genomes_seen": total_seen,
        "total_genomes_with_file": total_with_file,
        "total_missing_file": total_missing_file,
        "total_missing_key_or_parse_error": total_missing_key_or_parse_error,
        "total_marked_for_update": total_to_update,
        "total_updated": total_updated,
        "total_skipped_no_change": total_skipped_no_change,
    }

    logger.info("update_ena_accession_from_json_flow summary: %s", summary)
    return summary


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage: python -m workflows.flows.update_ena_accession_from_json_flow <base_dir> [read_chunk_size] [update_batch_size]"
        )
        sys.exit(1)

    base = sys.argv[1]
    read_cs = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    upd_bs = int(sys.argv[3]) if len(sys.argv) > 3 else 2000

    # Run directly (synchronously)
    result = update_ena_accession_from_json_flow(base, read_cs, upd_bs)
    print(result)
