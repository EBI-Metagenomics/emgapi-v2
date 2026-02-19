from pathlib import Path

import pytest
from pydantic import BaseModel

from workflows.models import AssemblyAnalysisBatch


@pytest.fixture
def batch(raw_reads_mgnify_study):
    return AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        asa_flow_run_id="1",
        virify_flow_run_id="2",
        map_flow_run_id="3",
    )


class AssemblyTestScenario(BaseModel):
    """Assembly test scenario configuration."""

    study_accession: str
    study_secondary: str
    assembly_accession_success: str
    assembly_accession_failed: str
    sample_accession: str
    run_accession: str
    fixture_source_dir: Path  # Where the permanent fixture files are stored
    workspace_dir: Path  # Temporary workspace for the test
    biome_path: str
    biome_name: str

    class Config:
        frozen = True


@pytest.fixture
def assembly_test_scenario(test_workspace):
    """Default assembly test scenario."""
    return AssemblyTestScenario(
        study_accession="PRJEB24849",
        study_secondary="ERP106708",
        assembly_accession_success="ERZ857107",
        assembly_accession_failed="ERZ857108",
        sample_accession="SAMN08514017",
        run_accession="SRR123456",
        fixture_source_dir=Path("/app/data/tests/assembly_v6_output/ERP106708"),
        workspace_dir=test_workspace,
        biome_path="root.engineered",
        biome_name="Engineered",
    )
