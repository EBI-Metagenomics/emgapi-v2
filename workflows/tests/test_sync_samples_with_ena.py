import json
from unittest.mock import patch

import pytest
from prefect.artifacts import Artifact

from ena.models import Sample, Study
from workflows.flows.housekeeping.sync_samples_with_ena import (
    sync_samples_with_ena,
)


@patch(
    "workflows.flows.housekeeping.sync_samples_with_ena.sync_sample_metadata_from_ena"
)
@pytest.mark.django_db
def test_sync_samples_with_ena_by_accessions(mock_sync, prefect_harness):
    """Test that the flow syncs specific samples by accession, handling failures."""
    study = Study.objects.create(accession="PRJNA000001", title="Study OK")
    ers_01 = Sample.objects.create(accession="ERS000001", study=study)
    Sample.objects.create(accession="ERS000002", study=study)
    Sample.objects.create(accession="ERS000003", study=study)

    def mock_sync_sample(sample: Sample):
        if sample.accession in {"ERS000002", "ERS000003"}:
            raise RuntimeError("ENA error")
        sample.metadata["sample_title"] = "Updated sample"
        sample.metadata["lat"] = "69.6"
        sample.save()
        return

    mock_sync.side_effect = mock_sync_sample

    failed = sync_samples_with_ena(
        accessions=["ERS000001", "ERS000002", "ERS000003"],
        batch_size=1,
    )

    ers_01.refresh_from_db()
    assert ers_01.metadata["sample_title"] == "Updated sample"
    assert ers_01.metadata["lat"] == "69.6"

    assert failed == ["ERS000002", "ERS000003"]
    assert mock_sync.call_count == 3

    failed_syncs_table = Artifact.get("failed-ena-sample-syncs")
    assert failed_syncs_table.type == "table"
    assert json.loads(failed_syncs_table.data) == [
        {"accession": "ERS000002"},
        {"accession": "ERS000003"},
    ]


@patch(
    "workflows.flows.housekeeping.sync_samples_with_ena.sync_sample_metadata_from_ena"
)
@pytest.mark.django_db
def test_sync_samples_with_ena_resolves_secondary_accessions(
    mock_sync, prefect_harness
):
    """Test that the flow syncs samples looked up by secondary accession."""
    study = Study.objects.create(accession="PRJNA000001", title="Study OK")
    sample = Sample.objects.create(
        accession="ERS000001",
        additional_accessions=["SRS000001"],
        study=study,
    )

    failed = sync_samples_with_ena(accessions=["SRS000001"], batch_size=1)

    assert failed == []
    mock_sync.assert_called_once_with(sample)
