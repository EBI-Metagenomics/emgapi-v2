"""
Prefect flow for ingesting data into the GenomeAssemblyLink model.
This flow accepts a path to a TSV file with the following columns:
- primary_assembly: the accession of the related assembly
- mag_accession: arbitrary accession for MAG
- genome: the accession of the related genome
- species_rep: arbitrary genome accession for species representative
"""

from pathlib import Path
import csv
from typing import Dict, List, Optional, Tuple

from prefect import flow, task, get_run_logger
from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist

# Import Django models
from activate_django_first import EMG_CONFIG  # Required before any Django model imports
from analyses.models import Assembly
from genomes.models import Genome, GenomeAssemblyLink
from workflows.data_io_utils.csv.csv_comment_handler import CommentAwareDictReader, CSVDelimiter


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
    """
    logger = get_run_logger()
    path = Path(tsv_path)

    if not path.exists():
        logger.error(f"TSV file not found: {tsv_path}")
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    if not path.is_file():
        logger.error(f"Path is not a file: {tsv_path}")
        raise ValueError(f"Path is not a file: {tsv_path}")

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

    with open(tsv_path, 'r') as f:
        reader = CommentAwareDictReader(
            f, 
            delimiter=CSVDelimiter.TAB,
            none_values=["", "NA", "N/A", "null", "NULL"]
        )

        # Check if required columns are present
        required_columns = ["primary_assembly", "genome"]
        missing_columns = [col for col in required_columns if col not in reader.fieldnames]

        if missing_columns:
            logger.error(f"Missing required columns in TSV: {', '.join(missing_columns)}")
            raise ValueError(f"Missing required columns in TSV: {', '.join(missing_columns)}")

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

    for i, record in enumerate(records, 1):
        # Check for required fields
        if not record.get("primary_assembly"):
            logger.warning(f"Row {i}: Missing primary_assembly, skipping")
            continue

        if not record.get("genome"):
            logger.warning(f"Row {i}: Missing genome, skipping")
            continue

        # Clean up the record
        cleaned_record = {
            "primary_assembly": record["primary_assembly"].strip(),
            "genome": record["genome"].strip(),
            "mag_accession": record.get("mag_accession", "").strip() if record.get("mag_accession") else None,
            "species_rep": record.get("species_rep", "").strip() if record.get("species_rep") else None
        }

        validated_records.append(cleaned_record)

    logger.info(f"Validated {len(validated_records)} records")
    return validated_records


@task(name="Find Genome and Assembly objects")
def find_objects(records: List[Dict[str, str]]) -> List[Tuple[Dict[str, str], Optional[Genome], Optional[Assembly]]]:
    """
    Finds the Genome and Assembly objects for each record.

    Args:
        records: List of validated and cleaned records

    Returns:
        List of tuples containing the record, Genome object, and Assembly object
    """
    logger = get_run_logger()
    results = []

    for record in records:
        genome = None
        assembly = None

        # Find the Genome
        try:
            genome = Genome.objects.get(accession=record["genome"])
        except ObjectDoesNotExist:
            logger.warning(f"Genome not found: {record['genome']}")

        # Find the Assembly
        try:
            assembly = Assembly.objects.get_by_accession(record["primary_assembly"])
        except (ObjectDoesNotExist, Assembly.MultipleObjectsReturned):
            logger.warning(f"Assembly not found or multiple found: {record['primary_assembly']}")

        results.append((record, genome, assembly))

    logger.info(f"Found objects for {len(results)} records")
    return results


@task(name="Create GenomeAssemblyLink objects")
def create_links(objects: List[Tuple[Dict[str, str], Optional[Genome], Optional[Assembly]]]) -> int:
    """
    Creates GenomeAssemblyLink objects for each valid record.

    Args:
        objects: List of tuples containing the record, Genome object, and Assembly object

    Returns:
        Number of GenomeAssemblyLink objects created
    """
    logger = get_run_logger()
    created_count = 0
    updated_count = 0
    skipped_count = 0

    with transaction.atomic():
        for record, genome, assembly in objects:
            if not genome or not assembly:
                skipped_count += 1
                continue

            # Check if the link already exists
            link, created = GenomeAssemblyLink.objects.get_or_create(
                genome=genome,
                assembly=assembly,
                defaults={
                    "species_rep": record["species_rep"],
                    "mag_accession": record["mag_accession"]
                }
            )

            if created:
                created_count += 1
                logger.info(f"Created link between {genome.accession} and {assembly.id}")
            else:
                # Update the link if it already exists
                updated = False
                if record["species_rep"] and record["species_rep"] != link.species_rep:
                    link.species_rep = record["species_rep"]
                    updated = True

                if record["mag_accession"] and record["mag_accession"] != link.mag_accession:
                    link.mag_accession = record["mag_accession"]
                    updated = True

                if updated:
                    link.save()
                    updated_count += 1
                    logger.info(f"Updated link between {genome.accession} and {assembly.id}")
                else:
                    skipped_count += 1

    logger.info(f"Created {created_count} links, updated {updated_count} links, skipped {skipped_count} links")
    return created_count + updated_count


@flow(name="Import Genome Assembly Links")
def import_genome_assembly_links(tsv_path: str) -> Dict[str, int]:
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

    logger.info(f"Import completed. Processed {len(records)} records, created/updated {links_count} links")

    return {
        "total_records": len(records),
        "validated_records": len(validated_records),
        "links_created_or_updated": links_count
    }


if __name__ == "__main__":
    # This allows the flow to be run directly for testing
    import sys

    if len(sys.argv) != 2:
        print("Usage: python import_genome_assembly_links.py <tsv_path>")
        sys.exit(1)

    tsv_path = sys.argv[1]
    result = import_genome_assembly_links(tsv_path)
    print(f"Import completed: {result}")
