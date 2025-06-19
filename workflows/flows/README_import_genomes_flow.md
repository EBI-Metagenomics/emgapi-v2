# Import Genomes Prefect Flow

This document describes how to use the `import_genomes_flow.py` Prefect flow for importing genomes into the MGnify database.

## Overview

The `import_genomes_flow.py` file contains a Prefect workflow that orchestrates the process of importing genomes into the MGnify database. It is a Prefect-based implementation of the Django management command `import_genomes`.

## Prerequisites

- Prefect installed and configured
- Access to the MGnify database
- Genome data in the expected format and directory structure

## Usage

### Running as a Prefect Flow

To run the flow using Prefect:

```python
from workflows.flows.import_genomes_flow import import_genomes_flow

# Run the flow
import_genomes_flow(
    results_directory="/path/to/results",
    catalogue_directory="genomes/skin/1.0/",
    catalogue_name="Human Skin",
    catalogue_version="1.0",
    gold_biome="root:Host-Associated:Human:Skin",
    pipeline_version="v2.0",
    catalogue_type="prokaryotes",
    update_metadata_only=False,
    database="default",
    catalogue_biome_label="Human Skin"
)
```

### Running as a Script

The flow can also be run directly as a script:

```bash
python -m workflows.flows.import_genomes_flow \
    /path/to/results \
    "genomes/skin/1.0/" \
    "Human Skin" \
    "1.0" \
    "root:Host-Associated:Human:Skin" \
    "v2.0" \
    "prokaryotes" \
    --database default \
    --catalogue_biome_label "Human Skin"
```

Add the `--update-metadata-only` flag if you only want to update the metadata of genomes in an existing catalogue.

## Parameters

- `results_directory`: Path to the results directory
- `catalogue_directory`: The folder within `results_directory` where the results files are (e.g., "genomes/skin/1.0/")
- `catalogue_name`: The name of this catalogue without any version label (e.g., "Human Skin")
- `catalogue_version`: The version label (e.g., "1.0" or "2021-01")
- `gold_biome`: Primary biome for the catalogue, as a GOLD lineage (e.g., "root:Host-Associated:Human:Skin")
- `pipeline_version`: Pipeline version tag that catalogue was produced by (e.g., "v2.0")
- `catalogue_type`: The type of genomes in the catalogue (one of: "prokaryotes", "eukaryotes", "viruses")
- `update_metadata_only`: Only update the metadata of genomes in an existing catalogue (default: False)
- `database`: Database to use (default: "default")
- `catalogue_biome_label`: A catalogue biome label which can be used to group together related catalogues (default: same as `catalogue_name`)

## Flow Structure

The flow is broken down into several tasks:

1. `validate_pipeline_version`: Validates the pipeline version format
2. `parse_options`: Parses and validates command options
3. `get_gold_biome`: Gets the gold biome for a given lineage
4. `get_catalogue`: Gets or creates a genome catalogue
5. `process_genome_directory`: Processes a single genome directory
6. `upload_genome_annotations`: Uploads genome annotations
7. `upload_genome_files`: Uploads genome files
8. `upload_catalogue_files`: Uploads catalogue files

These tasks are orchestrated by the main flow function `import_genomes_flow`.

## Error Handling

The flow includes error handling for common issues:

- Invalid pipeline versions
- Missing results directories
- Missing required files
- Non-existent biomes
- Duplicate download aliases

Errors are logged using Prefect's logging system and can be viewed in the Prefect UI.