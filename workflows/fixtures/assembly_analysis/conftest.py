import pytest
from workflows.models import AssemblyAnalysisBatch


@pytest.fixture
def batch(raw_reads_mgnify_study):
    return AssemblyAnalysisBatch.objects.create(
        study=raw_reads_mgnify_study,
        asa_flow_run_id="1",
        virify_flow_run_id="2",
        map_flow_run_id="3",
    )
