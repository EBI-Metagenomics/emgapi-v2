import json
from unittest.mock import patch

import pytest
from prefect.artifacts import Artifact

from ena.models import Study
from workflows.ena_utils.requestors import ENAAvailabilityException
from workflows.flows.housekeeping.sync_studies_with_ena import (
    sync_studies_with_ena,
)


@patch(
    "workflows.flows.housekeeping.sync_studies_with_ena.sync_study_metadata_from_ena"
)
@patch(
    "workflows.flows.housekeeping.sync_studies_with_ena.sync_privacy_state_of_ena_study_and_derived_objects"
)
@pytest.mark.django_db
def test_sync_studies_with_ena_by_accessions(
    mock_privacy_sync, mock_metadata_sync, prefect_harness
):
    """Test that the flow syncs specific studies by accession, handling failures."""
    prj_01, _ = Study.objects.get_or_create(accession="PRJNA000001", title="Study OK")
    prj_02, _ = Study.objects.get_or_create(accession="PRJNA000002", title="Study Fail")
    prj_03, _ = Study.objects.get_or_create(
        accession="PRJNA000003", title="Study Suppressed"
    )

    def mock_sync_study(study: Study):
        if study.accession == "PRJNA000002":
            raise RuntimeError("ENA error")
        if study.accession == "PRJNA000003":
            raise ENAAvailabilityException("Empty response")
        study.title = "Study OK - and updated!"
        study.metadata["description"] = "Study OK - description updated!"
        study.save()

    mock_metadata_sync.side_effect = mock_sync_study

    def mock_sync_privacy(study: Study, **kwargs):
        if study.accession in {"PRJNA000002", "PRJNA000003"}:
            study.is_suppressed = True
            study.save(update_fields=["is_suppressed"])

    mock_privacy_sync.side_effect = mock_sync_privacy

    failed = sync_studies_with_ena(
        accessions=["PRJNA000001", "PRJNA000002", "PRJNA000003"],
        batch_size=10,
    )

    prj_01.refresh_from_db()
    assert prj_01.title == "Study OK - and updated!"
    assert prj_01.metadata["description"] == "Study OK - description updated!"
    prj_03.refresh_from_db()
    assert prj_03.is_suppressed

    assert failed == ["PRJNA000002"]
    assert mock_metadata_sync.call_count == 3
    assert mock_privacy_sync.call_count == 3
    assert all(
        call.kwargs == {"also_check_suppressed_children": True}
        for call in mock_privacy_sync.call_args_list
    )

    failed_syncs_table = Artifact.get("failed-ena-study-syncs")
    assert failed_syncs_table.type == "table"
    assert json.loads(failed_syncs_table.data) == [{"accession": "PRJNA000002"}]
