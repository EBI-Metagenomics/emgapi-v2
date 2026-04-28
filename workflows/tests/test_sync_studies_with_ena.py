import json
from unittest.mock import patch

import pytest
from prefect.artifacts import Artifact

from ena.models import Study
from workflows.flows.housekeeping.sync_studies_with_ena import (
    sync_studies_with_ena,
)


@patch(
    "workflows.flows.housekeeping.sync_studies_with_ena.sync_study_metadata_from_ena"
)
@pytest.mark.django_db
def test_sync_studies_with_ena_by_accessions(mock_sync, prefect_harness):
    """Test that the flow syncs specific studies by accession, handling failures."""
    prj_01, _ = Study.objects.get_or_create(accession="PRJNA000001", title="Study OK")
    prj_02, _ = Study.objects.get_or_create(accession="PRJNA000002", title="Study Fail")

    def mock_sync_study(study: Study):
        if study.accession == "PRJNA000002":
            raise RuntimeError("ENA error")
        study.title = "Study OK - and updated!"
        study.metadata["description"] = "Study OK - description updated!"
        study.save()
        return

    mock_sync.side_effect = mock_sync_study

    failed = sync_studies_with_ena(
        accessions=["PRJNA000001", "PRJNA000002"],
        batch_size=10,
    )

    prj_01.refresh_from_db()
    assert prj_01.title == "Study OK - and updated!"
    assert prj_01.metadata["description"] == "Study OK - description updated!"

    assert failed == ["PRJNA000002"]
    assert mock_sync.call_count == 2

    failed_syncs_table = Artifact.get("failed-ena-study-syncs")
    assert failed_syncs_table.type == "table"
    assert json.loads(failed_syncs_table.data) == [{"accession": "PRJNA000002"}]
