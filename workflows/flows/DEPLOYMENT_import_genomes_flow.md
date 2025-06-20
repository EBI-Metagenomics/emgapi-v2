# Deploying the Import Genomes Prefect Flow

This document provides instructions on how to deploy the `import_genomes_flow.py` Prefect flow to various environments.

## Prerequisites

Before deploying the flow, ensure you have:

- Prefect installed and configured
- Access to the MGnify database
- Appropriate permissions to register flows and create deployments
- Genome data in the expected format and directory structure

## Prefect Version Considerations

The `import_genomes_flow.py` file uses Prefect 1.0 syntax with `Flow` objects and `@task` decorators. Prefect 2.0 has a different deployment model, so the instructions below are specific to Prefect 1.0.

If you want to migrate to Prefect 2.0, you would need to refactor the flow to use the `@flow` decorator instead of creating a `Flow` object explicitly.

## Deployment Options

### Option 1: Local Execution

The simplest way to run the flow is locally, as described in the README:

```python
from workflows.flows.import_genomes_flow import flow

# Run the flow with parameters
flow.run(
    parameters={
        "options": {
            "results_directory": "/path/to/results",
            "catalogue_directory": "genomes/skin/1.0/",
            "catalogue_name": "Human Skin",
            "catalogue_version": "1.0",
            "gold_biome": "root:Host-Associated:Human:Skin",
            "pipeline_version": "v2.0",
            "catalogue_type": "prokaryotes",
            "update_metadata_only": False,
            "database": "default",
            "catalogue_biome_label": "Human Skin"
        }
    }
)
```

### Option 2: Register with Prefect Server/Cloud

To deploy the flow to a Prefect Server or Prefect Cloud:

1. Start by registering the flow:

```python
from workflows.flows.import_genomes_flow import flow
from prefect.storage import GitHub
from prefect.run_configs import LocalRun

# Configure storage
storage = GitHub(
    repo="your-org/mgnify-web",
    path="emgapi-v2/workflows/flows/import_genomes_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN"  # Name of the secret in Prefect
)

# Configure run config
run_config = LocalRun(
    env={"PYTHONPATH": "/path/to/mgnify-web/emgapi-v2"}
)

# Register the flow
flow.storage = storage
flow.run_config = run_config
flow.register(project_name="mgnify")
```

2. Create a deployment using the Prefect CLI:

```bash
prefect create-deployment --name "import-genomes" --flow-name "genome-catalogue-import" --project "mgnify"
```

3. Start an agent to execute the flow:

```bash
prefect agent local start
```

### Option 3: Docker Deployment

For containerized deployment:

1. Create a Dockerfile:

```dockerfile
FROM prefecthq/prefect:1.0

WORKDIR /app

# Copy the project files
COPY emgapi-v2 /app/emgapi-v2

# Install dependencies
RUN pip install -r /app/emgapi-v2/requirements.txt

# Set environment variables
ENV PYTHONPATH=/app

# Set the entrypoint
ENTRYPOINT ["prefect", "agent", "local", "start"]
```

2. Build and run the Docker image:

```bash
docker build -t mgnify-prefect .
docker run -e PREFECT__CLOUD__API_KEY=your-api-key mgnify-prefect
```

## Scheduling the Flow

To schedule the flow to run on a regular basis:

```python
from datetime import timedelta
from prefect.schedules import IntervalSchedule

# Create a schedule to run the flow every week
schedule = IntervalSchedule(
    start_date=datetime.utcnow(),
    interval=timedelta(weeks=1)
)

# Update the flow with the schedule
flow.schedule = schedule
flow.register(project_name="mgnify")
```

## Running the Deployed Flow

### Via Prefect UI

1. Navigate to the Prefect UI
2. Find your flow in the "Flows" section
3. Click "Run" and provide the required parameters

### Via Prefect CLI

```bash
prefect run flow --name "genome-catalogue-import" --project "mgnify" --param options.results_directory="/path/to/results" --param options.catalogue_directory="genomes/skin/1.0/" --param options.catalogue_name="Human Skin" --param options.catalogue_version="1.0" --param options.gold_biome="root:Host-Associated:Human:Skin" --param options.pipeline_version="v2.0" --param options.catalogue_type="prokaryotes"
```

### Via Python API

```python
from prefect import Client

client = Client()
flow_run_id = client.create_flow_run(
    flow_name="genome-catalogue-import",
    project_name="mgnify",
    parameters={
        "options": {
            "results_directory": "/path/to/results",
            "catalogue_directory": "genomes/skin/1.0/",
            "catalogue_name": "Human Skin",
            "catalogue_version": "1.0",
            "gold_biome": "root:Host-Associated:Human:Skin",
            "pipeline_version": "v2.0",
            "catalogue_type": "prokaryotes",
            "update_metadata_only": False,
            "database": "default",
            "catalogue_biome_label": "Human Skin"
        }
    }
)
```

## Migrating to Prefect 2.0

If you want to migrate this flow to Prefect 2.0, you would need to:

1. Refactor the flow to use the `@flow` decorator instead of creating a `Flow` object
2. Replace `@task` decorators with the Prefect 2.0 version
3. Use `get_run_logger()` instead of the standard logging module
4. Update the deployment process to use Prefect 2.0 deployments

Example of how the flow might look in Prefect 2.0:

```python
import os
import re
from pathlib import Path
import django
from prefect import flow, task, get_run_logger

django.setup()

@task
def validate_pipeline_version(version: str) -> int:
    logger = get_run_logger()
    match = re.match(r'^v?([1-3])(?:\..*)?(?:[a-zA-Z0-9\-]*)?$', version)
    if not match:
        logger.error(f"Invalid pipeline version: {version}")
        raise ValueError(f"Invalid pipeline version: {version}")
    return int(match.group(1))

# ... other tasks ...

@flow(name="genome-catalogue-import")
def import_genomes_flow(
    results_directory: str,
    catalogue_directory: str,
    catalogue_name: str,
    catalogue_version: str,
    gold_biome: str,
    pipeline_version: str,
    catalogue_type: str,
    update_metadata_only: bool = False,
    database: str = 'default',
    catalogue_biome_label: str = '',
):
    logger = get_run_logger()
    # ... flow implementation ...
```

## Troubleshooting

### Common Issues

1. **Flow not being picked up by agent**:
   - Ensure the agent is running and connected to the correct workspace
   - Check that the flow is registered in the correct project

2. **Import errors**:
   - Ensure the PYTHONPATH includes the root of your project
   - Check that all dependencies are installed

3. **Database connection issues**:
   - Verify database credentials and connection settings
   - Ensure the database is accessible from the environment where the flow is running

### Logs

Prefect logs can be viewed in several ways:

- In the Prefect UI under the "Flow Runs" section
- Via the Prefect CLI: `prefect logs get -f <flow-run-id>`
- In the agent logs if running locally

## Conclusion

Deploying the `import_genomes_flow.py` Prefect flow can be done in various ways depending on your infrastructure and requirements. The options above provide a starting point for deploying the flow in different environments.

For more detailed information on Prefect 1.0 deployments, refer to the [Prefect 1.0 documentation](https://docs.prefect.io/1.0/core/concepts/flows.html).