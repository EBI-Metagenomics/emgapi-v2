from pathlib import Path
from typing import Dict, List, Optional, Tuple

from django.db import transaction
from prefect import flow, task, get_run_logger

# Import Django models
from analyses.models import Assembly
from genomes.models import Genome, GenomeAssemblyLink
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


@task(name="Validate TSV file")
def validate_tsv_file(tsv_path: str) -> str:
    """
    Validates that the TSV file exists and is readable.

    Args:
        tsv_path: Path to the TSV file

    Returns:
        The validated path to the TSV file

    Raises:
        FileNotFoundError: If the TSV file does not exist
        ValueError: If the path is not a file
    """
    logger = get_run_logger()
    path = Path(tsv_path)

    if not path.exists():
        logger.error(f"TSV file not found: {tsv_path}")
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    if not path.is_file():
        logger.error(f"Path is not a file: {tsv_path}")
        raise ValueError(f"Path is not a file: {tsv_path}")

    # Additional validation using shared rules
    File(
        path=path,
        rules=[
            FileExistsRule,
            FileIsNotEmptyRule,
        ],
    )

    logger.info(f"TSV file validated: {tsv_path}")
    return tsv_path


@task(name="Read TSV file")
def read_tsv_file(tsv_path: str) -> List[Dict[str, str]]:
    """
    Reads the TSV file and returns a list of dictionaries, one for each row.

    Args:
        tsv_path: Path to the TSV file

    Returns:
        List of dictionaries, one for each row in the TSV file
    """
    logger = get_run_logger()
    records = []

    with open(tsv_path, "r") as f:
        reader = CommentAwareDictReader(
            f, delimiter=CSVDelimiter.TAB, none_values=["", "NA", "N/A", "null", "NULL"]
        )

        # Check if required columns are present
        required_columns = ["primary_assembly", "genome"]
        missing_columns = [
            col for col in required_columns if col not in reader.fieldnames
        ]

        if missing_columns:
            logger.error(
                f"Missing required columns in TSV: {', '.join(missing_columns)}"
            )
            raise ValueError(
                f"Missing required columns in TSV: {', '.join(missing_columns)}"
            )

        for row in reader:
            records.append(row)

    logger.info(f"Read {len(records)} records from TSV file")
    return records


@task(name="Process TSV records")
def process_tsv_records(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Processes the TSV records, validating and cleaning the data.

    Args:
        records: List of dictionaries, one for each row in the TSV file

    Returns:
        List of validated and cleaned records
    """
    logger = get_run_logger()
    validated_records = []

    skipped = 0
    for i, record in enumerate(records, 1):
        if i == 1:
            logger.info(f"Processing record {i}")
            logger.info(record)

        # Safely normalise fields
        primary_assembly = (record.get("primary_assembly") or "").strip()
        genome_raw = record.get("genome")
        genome = (genome_raw or "").strip()

        # Skip records where genome is NA-like; CSV reader maps "" to None
        if genome_raw is None or genome.lower() in {"na", "n/a", "null"}:
            skipped += 1
            continue

        cleaned_record = {
            "primary_assembly": primary_assembly,
            "genome": genome,
            "mag_accession": (
                (record.get("mag_accession") or "").strip()
                if record.get("mag_accession")
                else None
            ),
            "species_rep": (
                (record.get("species_rep") or "").strip()
                if record.get("species_rep")
                else None
            ),
        }

        validated_records.append(cleaned_record)

    if skipped:
        logger.info(f"Skipped {skipped} record(s) due to NA-like genome values")

    logger.info(f"Validated {len(validated_records)} records")
    return validated_records


@task(name="Find Genome and Assembly objects")
def find_objects(
    records: List[Dict[str, str]],
) -> List[Tuple[Dict[str, str], Optional[Genome], Optional[Assembly]]]:
    """
    Find Genome and Assembly objects for each input record using batched lookups
    to reduce database round trips.

    :param records: List of input dictionaries containing 'genome' and/or 'primary_assembly' accessions
    :type records: list[dict[str, str]]
    :returns: Tuples of (record, Genome, Assembly) for each input record. If a
              lookup fails, the corresponding Genome or Assembly will be None.
    :rtype: list[tuple[dict[str, str], Optional[Genome], Optional[Assembly]]]
    """
    logger = get_run_logger()
    results = []

    if not records:
        return results

    genome_accessions = {r["genome"] for r in records if r.get("genome")}
    assembly_accessions = {
        r["primary_assembly"] for r in records if r.get("primary_assembly")
    }

    genomes_map = {
        g.accession: g
        for g in Genome.objects.filter(accession__in=list(genome_accessions)).only(
            "genome_id", "accession"
        )
    }

    assemblies_map: Dict[str, Assembly] = (
        Assembly.objects.match_accessions_to_instances(assembly_accessions)
    )

    for record in records:
        genome = genomes_map.get(record.get("genome"))
        assembly = assemblies_map.get(record.get("primary_assembly"))
        if genome is None:
            logger.warning(f"Genome not found: {record.get('genome')}")
        if assembly is None:
            logger.warning(
                f"Assembly not found or multiple found: {record.get('primary_assembly')}"
            )
        results.append((record, genome, assembly))

    logger.info(f"Found objects for {len(results)} records")
    return results


@task(name="Create GenomeAssemblyLink objects")
def create_links(
    objects: List[Tuple[Dict[str, str], Optional[Genome], Optional[Assembly]]],
    chunk_size: int = 10000,
) -> int:
    """
    Creates or updates GenomeAssemblyLink objects for each valid record using
    chunked bulk operations for scalability.

    Args:
        objects: List of tuples containing the record, Genome object, and Assembly object
        chunk_size: Number of valid rows per DB transaction/batch

    Returns:
        Number of GenomeAssemblyLink objects created or updated
    """
    logger = get_run_logger()

    def chunker(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    created_count = 0
    updated_count = 0
    skipped_count = 0

    # Only keep rows where both FKs resolved
    valid_rows = [t for t in objects if t[1] is not None and t[2] is not None]
    skipped_count += len(objects) - len(valid_rows)

    for batch in chunker(valid_rows, chunk_size):
        # close_old_connections()
        key_to_record = {}
        genome_ids = set()
        assembly_ids = set()
        for record, genome, assembly in batch:
            key = (genome.genome_id, assembly.id)
            key_to_record[key] = record
            genome_ids.add(genome.genome_id)
            assembly_ids.add(assembly.id)

        with transaction.atomic():
            # Fetch existing links for this batch
            existing_links = GenomeAssemblyLink.objects.filter(
                genome_id__in=genome_ids, assembly_id__in=assembly_ids
            ).only("id", "genome_id", "assembly_id", "species_rep", "mag_accession")
            existing_map = {(gl.genome_id, gl.assembly_id): gl for gl in existing_links}

            to_create = []
            to_update = []

            for (genome_id, assembly_id), rec in key_to_record.items():
                existing = existing_map.get((genome_id, assembly_id))
                if existing is None:
                    to_create.append(
                        GenomeAssemblyLink(
                            genome_id=genome_id,
                            assembly_id=assembly_id,
                            species_rep=rec.get("species_rep"),
                            mag_accession=rec.get("mag_accession"),
                        )
                    )
                else:
                    changed = False
                    species_rep = rec.get("species_rep")
                    mag_accession = rec.get("mag_accession")
                    if species_rep and species_rep != existing.species_rep:
                        existing.species_rep = species_rep
                        changed = True
                    if mag_accession and mag_accession != existing.mag_accession:
                        existing.mag_accession = mag_accession
                        changed = True
                    if changed:
                        to_update.append(existing)

            if to_create:
                # ignore_conflicts because unique_together(genome, assembly)
                GenomeAssemblyLink.objects.bulk_create(
                    to_create, ignore_conflicts=True, batch_size=chunk_size
                )
                created_count += len(to_create)
            if to_update:
                GenomeAssemblyLink.objects.bulk_update(
                    to_update, ["species_rep", "mag_accession"], batch_size=chunk_size
                )
                updated_count += len(to_update)

    logger.info(
        f"Created {created_count} links, updated {updated_count} links, skipped {skipped_count} links"
    )
    return created_count + updated_count


"""
Prefect flow for ingesting data into the GenomeAssemblyLink model.
This flow accepts a path to a TSV file with the following columns:
- primary_assembly: the accession of the related assembly
- mag_accession: arbitrary accession for MAG
- genome: the accession of the related genome
- species_rep: arbitrary genome accession for species representative
"""


@flow(name="import_genome_assembly_links_flow")
def import_genome_assembly_links_flow(tsv_path: str) -> Dict[str, int]:
    """
    Imports data from a TSV file into the GenomeAssemblyLink model.

    Args:
        tsv_path: Path to the TSV file

    Returns:
        Dictionary with statistics about the import
    """
    logger = get_run_logger()
    logger.info(f"Starting import of genome assembly links from {tsv_path}")

    # Validate the TSV file
    validated_path = validate_tsv_file(tsv_path)

    # Read the TSV file
    records = read_tsv_file(validated_path)

    # Process the records
    validated_records = process_tsv_records(records)

    # Find the Genome and Assembly objects
    objects = find_objects(validated_records)

    # Create the GenomeAssemblyLink objects
    links_count = create_links(objects)

    logger.info(
        f"Import completed. Processed {len(records)} records, created/updated {links_count} links"
    )

    return {
        "total_records": len(records),
        "validated_records": len(validated_records),
        "links_created_or_updated": links_count,
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_genome_assembly_links_flow.py <tsv_path>")
        sys.exit(1)

    tsv_path = sys.argv[1]
    result = import_genome_assembly_links_flow(tsv_path)
    print(f"Import completed: {result}")
