from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from django.db import transaction
from django.db.models import Q
from prefect import flow, task, get_run_logger

from activate_django_first import EMG_CONFIG

from analyses.models import Assembly, Run
from genomes.models import Genome, AdditionalContainedGenomes
from workflows.data_io_utils.csv.csv_comment_handler import (
    CommentAwareDictReader,
    CSVDelimiter,
)
from workflows.data_io_utils.file_rules.common_rules import (
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import File
import sys

genome_config = EMG_CONFIG.genomes


@task(name="Validate TSV file")
def validate_csv_file(csv_path: str) -> str:
    """
    Validates that the TSV file exists and is readable.
    """
    logger = get_run_logger()
    path = Path(csv_path)

    if not path.exists():
        logger.error(f"TSV file not found: {csv_path}")
        raise FileNotFoundError(f"TSV file not found: {csv_path}")

    if not path.is_file():
        logger.error(f"Path is not a file: {csv_path}")
        raise ValueError(f"Path is not a file: {csv_path}")

    File(
        path=path,
        rules=[
            FileExistsRule,
            FileIsNotEmptyRule,
        ],
    )

    logger.info(f"TSV file validated: {csv_path}")
    return csv_path


REQUIRED_COLUMNS = ["Run", "Genome_Mgnify_accession", "Containment", "cANI"]
NA_VALUES = {None, "", "NA", "N/A", "null", "NULL"}


def _parse_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s in NA_VALUES:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _chunked_iterable(it: Iterable, size: int):
    chunk = []
    for item in it:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


@task(name="Process TSV chunk")
def process_csv_chunk(
    rows: List[Dict[str, str]], batch_size: int = 10000
) -> Tuple[int, int, int]:
    """
    Process a list of rows: resolve Run, Genome, Assemblies and bulk-insert into AdditionalContainedGenomes.
    Returns tuple: (created_count, skipped_rows, run_or_genome_missing)

    Enhanced logging explicitly lists which rows were skipped and why:
      - rows with missing Run or Genome accessions (at parse time)
      - unresolved Run accessions (no matching Run)
      - unresolved Genome accessions (no matching Genome)
      - runs that resolved but had no related Assemblies
    """
    logger = get_run_logger()

    if not rows:
        return 0, 0, 0

    # Normalise and validate minimal fields, group by Run accession
    normalised: List[Tuple[str, str, Optional[float], Optional[float]]] = []
    skipped = 0
    missing_required_examples: List[Tuple[str, str]] = []

    for r in rows:
        run_acc = (r.get("Run") or "").strip()
        genome_acc = (r.get("Genome_Mgnify_accession") or "").strip()
        containment = _parse_float(r.get("Containment"))
        cani = _parse_float(r.get("cANI"))
        if not run_acc or not genome_acc:
            skipped += 1
            if len(missing_required_examples) < 20:
                missing_required_examples.append((run_acc, genome_acc))
            continue
        normalised.append((run_acc, genome_acc, containment, cani))

    if skipped:
        logger.warning(
            f"Rows skipped at parse time due to missing required fields (Run/Genome): count={skipped}, examples={missing_required_examples}"
        )

    if not normalised:
        logger.info(
            f"No valid rows after normalisation in chunk; rows={len(rows)}, skipped_missing_required={skipped}"
        )
        return 0, skipped, 0

    run_accessions = {t[0] for t in normalised}
    genome_accessions = {t[1] for t in normalised}

    # Resolve Runs in batch via ENA accessions helper
    runs_map: Dict[str, Run] = Run.objects.match_accessions_to_instances(run_accessions)

    # Resolve Genomes in batch
    genomes_qs = Genome.objects.filter(accession__in=list(genome_accessions)).only(
        "genome_id", "accession"
    )
    genomes_map: Dict[str, Genome] = {g.accession: g for g in genomes_qs}

    # Identify unresolved accessions for explicit logging
    unresolved_run_accs = sorted(a for a in run_accessions if a not in runs_map)
    unresolved_genome_accs = sorted(
        a for a in genome_accessions if a not in genomes_map
    )
    if unresolved_run_accs:
        logger.warning(
            f"Unresolved Run accessions in chunk: count={len(unresolved_run_accs)}, examples={unresolved_run_accs[:20]}"
        )
    if unresolved_genome_accs:
        logger.warning(
            f"Unresolved Genome accessions in chunk: count={len(unresolved_genome_accs)}, examples={unresolved_genome_accs[:20]}"
        )

    # Group rows by found run id to optimise assembly lookups
    rows_by_run_id: Dict[
        int, List[Tuple[str, str, Optional[float], Optional[float]]]
    ] = {}
    missing_run_or_genome = 0
    missing_due_to_run_examples: List[Tuple[str, str]] = []
    missing_due_to_genome_examples: List[Tuple[str, str]] = []

    for run_acc, genome_acc, containment, cani in normalised:
        run = runs_map.get(run_acc)
        genome = genomes_map.get(genome_acc)
        if run is None or genome is None:
            missing_run_or_genome += 1
            if run is None and len(missing_due_to_run_examples) < 20:
                missing_due_to_run_examples.append((run_acc, genome_acc))
            if genome is None and len(missing_due_to_genome_examples) < 20:
                missing_due_to_genome_examples.append((run_acc, genome_acc))
            continue
        rows_by_run_id.setdefault(run.id, []).append(
            (run_acc, genome_acc, containment, cani)
        )

    if missing_run_or_genome:
        logger.warning(
            "Rows skipped after resolution due to missing Run/Genome: total=%d, examples_missing_run=%s, examples_missing_genome=%s",
            missing_run_or_genome,
            missing_due_to_run_examples,
            missing_due_to_genome_examples,
        )

    if not rows_by_run_id:
        return 0, skipped, missing_run_or_genome

    run_ids = list(rows_by_run_id.keys())

    # Collect all run accessions present in this chunk (for ena_accessions overlap lookup)
    run_acc_to_ids: Dict[str, List[int]] = {}
    for run_acc, run in runs_map.items():
        if run.id in rows_by_run_id:
            for acc in getattr(run, "ena_accessions", []) or []:
                if not acc:
                    continue
                run_acc_to_ids.setdefault(acc, []).append(run.id)

    # Find assemblies linked to any of these runs either by FK or overlapping ena_accessions
    assemblies_qs = Assembly.objects.filter(
        Q(run_id__in=run_ids) | Q(ena_accessions__overlap=list(run_acc_to_ids.keys()))
    ).only("id", "run_id", "ena_accessions")

    # Map run_id -> list of Assembly ids
    assemblies_by_run_id: Dict[int, List[int]] = {rid: [] for rid in run_ids}
    for assembly in assemblies_qs:
        candidate_run_ids = set()
        if assembly.run_id in assemblies_by_run_id:
            candidate_run_ids.add(assembly.run_id)
        for acc in getattr(assembly, "ena_accessions", []) or []:
            for rid in run_acc_to_ids.get(acc, []):
                candidate_run_ids.add(rid)
        for rid in candidate_run_ids:
            assemblies_by_run_id.setdefault(rid, []).append(assembly.id)

    # Prepare rows to insert
    to_create: List[AdditionalContainedGenomes] = []
    runs_without_assemblies_examples: List[str] = []
    runs_without_assemblies_count = 0

    for rid, row_list in rows_by_run_id.items():
        assembly_ids = assemblies_by_run_id.get(rid, [])
        if not assembly_ids:
            runs_without_assemblies_count += 1
            # Pick a representative run accession from the rows associated to this rid
            if len(runs_without_assemblies_examples) < 20:
                example_accs = {row[0] for row in row_list}  # run_acc values
                runs_without_assemblies_examples.append(next(iter(example_accs)))
            continue
        for _run_acc, genome_acc, containment, cani in row_list:
            genome = genomes_map.get(genome_acc)
            if genome is None:
                continue  # already counted above, but double safety
            for aid in assembly_ids:
                to_create.append(
                    AdditionalContainedGenomes(
                        run_id=rid,
                        genome_id=genome.genome_id,
                        assembly_id=aid,
                        containment=containment,
                        cani=cani,
                    )
                )

    if runs_without_assemblies_count:
        logger.info(
            f"Resolved runs with no related assemblies: count={runs_without_assemblies_count}, examples={runs_without_assemblies_examples}"
        )

    created = 0
    # Insert in DB in batches with ignore_conflicts for unique constraint
    for batch in _chunked_iterable(to_create, batch_size):
        with transaction.atomic():
            AdditionalContainedGenomes.objects.bulk_create(
                batch, ignore_conflicts=True, batch_size=batch_size
            )
            created += len(batch)

    logger.info(
        "Chunk processed: rows=%d, valid_rows_for_insert=%d, create_attempts=%d, skipped_missing_required=%d, skipped_missing_after_resolution=%d",
        len(rows),
        len(to_create),
        created,
        skipped,
        missing_run_or_genome,
    )
    return created, skipped, missing_run_or_genome


@flow(name="import_additional_contained_genomes_flow")
def import_additional_contained_genomes_flow(
    csv_path: str, chunk_size: int = 50000, insert_batch_size: int = 10000
) -> Dict[str, int]:
    """
    Imports data from a large TSV file into the AdditionalContainedGenomes model.

    The TSV must contain the following columns:
      - Run
      - Genome_Mgnify_accession
      - Containment
      - cANI

    The flow reads the file in streaming chunks and performs batched DB operations.
    """
    logger = get_run_logger()
    logger.info(f"Starting import of AdditionalContainedGenomes from TSV: {csv_path}")

    validated_path = validate_csv_file(csv_path)

    total_rows = 0
    total_created = 0
    total_skipped = 0
    total_missing = 0

    with open(validated_path, "r") as f:
        reader = CommentAwareDictReader(
            f,
            delimiter=CSVDelimiter.TAB,
            none_values=["", "NA", "N/A", "null", "NULL"],
        )
        # Validate header
        fieldnames = reader.fieldnames or []
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
        if missing_cols:
            raise ValueError(
                f"Missing required columns in TSV: {', '.join(missing_cols)}; found: {', '.join(fieldnames)}"
            )

        # Iterate in chunks to limit memory usage
        rows_buffer: List[Dict[str, str]] = []
        for row in reader:
            rows_buffer.append(row)
            total_rows += 1
            if len(rows_buffer) >= chunk_size:
                created, skipped, missing = process_csv_chunk(
                    rows_buffer, insert_batch_size
                )
                total_created += created
                total_skipped += skipped
                total_missing += missing
                rows_buffer = []
        # Process remainder
        if rows_buffer:
            created, skipped, missing = process_csv_chunk(
                rows_buffer, insert_batch_size
            )
            total_created += created
            total_skipped += skipped
            total_missing += missing

    logger.info(
        f"Import completed. rows={total_rows}, created_attempts={total_created}, skipped_rows={total_skipped}, missing_run_or_genome_rows={total_missing}"
    )

    return {
        "total_rows": total_rows,
        "created_attempts": total_created,
        "skipped_rows": total_skipped,
        "missing_run_or_genome_rows": total_missing,
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: python import_additional_contained_genomes_flow.py <tsv_path> [chunk_size] [insert_batch_size]"
        )
        sys.exit(1)

    _csv_path = sys.argv[1]
    _chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    _insert_batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10000

    result = import_additional_contained_genomes_flow(
        _csv_path, _chunk_size, _insert_batch_size
    )
    print(f"Import completed: {result}")
