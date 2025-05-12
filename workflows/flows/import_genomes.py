import os
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
import subprocess
import logging

logger = logging.getLogger(__name__)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
def check_directory_exists(directory: str) -> bool:
    """Verify that the specified directory exists"""
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Directory {directory} does not exist")
    return True


@task
def validate_catalogue_metadata(
        catalogue_name: str,
        catalogue_version: str,
        gold_biome: str,
        catalogue_type: str
) -> dict:
    """Validate the catalogue metadata parameters"""
    valid_types = ["prokaryotes", "eukaryotes", "viruses"]

    if catalogue_type not in valid_types:
        raise ValueError(f"Catalogue type must be one of {valid_types}")

    if not catalogue_name:
        raise ValueError("Catalogue name must not be empty")

    if not catalogue_version:
        raise ValueError("Catalogue version must not be empty")

    if not gold_biome:
        raise ValueError("GOLD biome must not be empty")

    return {
        "catalogue_name": catalogue_name,
        "catalogue_version": catalogue_version,
        "gold_biome": gold_biome,
        "catalogue_type": catalogue_type
    }


@task
def run_upload_command(
        results_directory: str,
        catalogue_directory: str,
        catalogue_name: str,
        catalogue_version: str,
        gold_biome: str,
        pipeline_version: str,
        catalogue_type: str,
        database: str = "default",
        catalogue_biome_label: str = "",
        update_metadata_only: bool = False
) -> str:
    """Run the Django management command to upload genome data"""
    command = [
        "python", "manage.py", "upload_genomes",
        results_directory,
        catalogue_directory,
        catalogue_name,
        catalogue_version,
        gold_biome,
        pipeline_version,
        catalogue_type,
    ]

    if database != "default":
        command.extend(["--database", database])

    if catalogue_biome_label:
        command.extend(["--catalogue_biome_label", catalogue_biome_label])

    if update_metadata_only:
        command.append("--update-metadata-only")

    logger.info(f"Executing command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True
        )
        logger.info(f"Command output: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        raise RuntimeError(f"Upload command failed: {e.stderr}")


@flow(name="Genome Catalogue Upload Flow")
def upload_genome_catalogue(
        results_directory: str,
        catalogue_directory: str,
        catalogue_name: str,
        catalogue_version: str,
        gold_biome: str,
        pipeline_version: str,
        catalogue_type: str,
        database: str = "default",
        catalogue_biome_label: str = "",
        update_metadata_only: bool = False
):
    """
    Prefect flow to run the Django upload_genomes management command for uploading
    genome catalogue data to the database.

    Args:
        results_directory: Base directory containing results
        catalogue_directory: Specific catalogue directory within results_directory
        catalogue_name: Name of the catalogue (e.g. "Human Skin")
        catalogue_version: Version of the catalogue (e.g. "1.0")
        gold_biome: GOLD biome lineage (e.g. "root:Host-Associated:Human:Skin")
        pipeline_version: Pipeline version tag (e.g. "3.0.1")
        catalogue_type: Type of genomes (one of: "prokaryotes", "eukaryotes", "viruses")
        database: Django database alias to use
        catalogue_biome_label: Optional biome label
        update_metadata_only: Only update metadata, not files
    """
    # Validate directories
    results_dir_valid = check_directory_exists(results_directory)
    full_catalogue_path = os.path.join(results_directory, catalogue_directory)
    catalogue_dir_valid = check_directory_exists(full_catalogue_path)

    if results_dir_valid and catalogue_dir_valid:
        # Validate metadata
        metadata_valid = validate_catalogue_metadata(
            catalogue_name,
            catalogue_version,
            gold_biome,
            catalogue_type
        )

        # Run upload command
        result = run_upload_command(
            results_directory=results_directory,
            catalogue_directory=catalogue_directory,
            catalogue_name=catalogue_name,
            catalogue_version=catalogue_version,
            gold_biome=gold_biome,
            pipeline_version=pipeline_version,
            catalogue_type=catalogue_type,
            database=database,
            catalogue_biome_label=catalogue_biome_label,
            update_metadata_only=update_metadata_only
        )

        return result


if __name__ == "__main__":
    # Example usage
    upload_genome_catalogue(
        results_directory="/path/to/results",
        catalogue_directory="genomes/human-skin/1.0",
        catalogue_name="Human Skin",
        catalogue_version="1.0",
        gold_biome="root:Host-Associated:Human:Skin",
        pipeline_version="v3.0.1",
        catalogue_type="prokaryotes",
        # Optional parameters
        database="default",
        catalogue_biome_label="Human Skin",
        update_metadata_only=False
    )