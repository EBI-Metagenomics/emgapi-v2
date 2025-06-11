# EMG API v2 - Claude Development Guide

## Project Overview

This is a Django-based bioinformatics application for MGnify (formerly EBI Metagenomics) that orchestrates large-scale genomic analysis pipelines. The system manages studies, samples, assemblies and analyses with sophisticated workflow orchestration.

## Architecture

### Core Apps
- **`analyses/`** - Core MGnify domain models (Study, Sample, Run, Assembly, Analysis)
- **`ena/`** - European Nucleotide Archive integration (Study, Sample models)
- **`workflows/`** - Bioinformatics pipeline orchestration using Prefect

### Key Domain Models
```
Study (MGYS*) ← Analysis (MGYA*) ← Sample ← Run/Assembly
     ↑                                ↑
 ena.Study                      ena.Sample
```

### Base Model Architecture
The codebase uses sophisticated Django abstract model inheritance:

- **`TimeStampedModel`** - Standard created_at/updated_at fields
- **`ENADerivedModel`** - Models that inherit metadata from ENA objects
- **`VisibilityControlledModel`** - Privacy/suppression state management
- **`WithDownloadsModel`** - Standardized file downloads with Pydantic schema validation
- **`WithWatchersModel`** - User notification system
- **`WithExperimentTypeModel`** - Experiment type classification
- **`WithStatusModel`** - JSON-based state machines

## Development Commands

### Testing
```bash
# Run tests
python manage.py test

# Run specific test
python manage.py test workflows.tests.test_simple_example_flow

# Run with coverage
coverage run --source='.' manage.py test
coverage report
```

### Database
```bash
# Make migrations
python manage.py makemigrations

# Apply migrations
python manage.py migrate

# Create superuser
python manage.py createsuperuser
```

### Workflow Management
```bash
# Prefect CLI access
python manage.py prefectcli

# Clear work directories
python manage.py clear_workdirs

# Force flow to crash (dev)
python manage.py force_flow_to_be_crashed <flow_run_id>

# Reconnect zombie jobs
python manage.py reconnect_zombie_jobs
```

### Data Import
```bash
# Import studies from legacy DB
python manage.py import_studies_from_legacy_db

# Import biomes from API v1
python manage.py import_biomes_from_api_v1

# Import v5 analysis
python manage.py import_v5_analysis

# Import assembler memory compute heuristics
python manage.py import_assembler_memory_compute_heuristics
```

## Known Refactoring Opportunities

### 1. Status Management Duplication (HIGH PRIORITY)
The status management pattern is duplicated across `Assembly`, `Analysis`, and `AssemblyAnalysisRequest` models. Consider creating a unified `WithStatusModel` base class.

### 2. Manager Complexity (MEDIUM PRIORITY)
Complex manager inheritance chains like `PublicAnalysisManagerIncludingAnnotations` could be simplified using composition patterns.

### 3. ENA Integration (MEDIUM PRIORITY)
Complex relationship management between MGnify and ENA models with signal-based state propagation could benefit from centralized synchronization logic.

## File Structure Notes

- **`analyses/base_models/`** - Abstract base models for common patterns
- **`workflows/flows/`** - Prefect flow definitions for pipeline orchestration
- **`workflows/flows/analyse_study_tasks/`** - Modular tasks for study analysis flows
- **`workflows/prefect_utils/`** - Utilities for Prefect integration
- **`emgapiv2/api/`** - Django Ninja API endpoints with Pydantic schemas
- **`static/`** - Static assets including MGnify branding

## Testing Strategy

- Unit tests in `tests/` directories within each app
- Integration tests for workflow orchestration
- Fixtures for complex domain objects in `fixtures/` directories
- Coverage testing recommended for new features

## Deployment

- Uses Docker containers with `Dockerfile` and `docker-compose.yaml`
- Kubernetes deployment configs in `deployment/ebi-wp-k8s-hl/`
- SLURM integration for HPC job submission
- Task runner with `Taskfile.yaml`

## Development Environment

- SLURM development environment available in `slurm-dev-environment/`
- Uses Prefect for workflow orchestration
- Django admin interface available for data management
- API documentation via Django Ninja's auto-generated docs
