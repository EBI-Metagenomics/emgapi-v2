from pathlib import Path
from unittest.mock import patch

import pytest
from django.http import Http404
from django.urls import reverse_lazy

from workflows.nextflow_utils.samplesheets import editable_location_for_samplesheet
from workflows.views import encode_samplesheet_path, validate_samplesheet_path


def test_samplesheet_editor_paths_validation(settings):
    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_slurm = (
        "/nfs/production/edit/here"
    )
    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_server = "/nfs/public/edit/here"

    settings.EMG_CONFIG.slurm.samplesheet_editing_allowed_inside = (
        "/nfs/production/edit/here"
    )

    samplesheet = "/nfs/production/edit/here/yes.csv"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    assert validate_samplesheet_path(samplesheet_encoded) == Path(samplesheet)

    samplesheet = "/nfs/production/cannot/edit/here/no.csv"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    with pytest.raises(Http404):
        validate_samplesheet_path(samplesheet_encoded)

    samplesheet = "/nfs/production/edit/here/no.jpeg"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    with pytest.raises(Http404):
        validate_samplesheet_path(samplesheet_encoded)

    samplesheet = "some/relative/dir/no.csv"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    with pytest.raises(Http404):
        validate_samplesheet_path(samplesheet_encoded)

    settings.EMG_CONFIG.slurm.samplesheet_editing_path_from_shared_filesystem = (
        "samplesheet_edits_here"
    )
    samplesheet = "/nfs/production/edit/here/yes.csv"
    assert editable_location_for_samplesheet(
        Path(samplesheet), settings.EMG_CONFIG.slurm.shared_filesystem_root_on_server
    ) == Path(
        "/nfs/public/edit/here/samplesheet_edits_here/nfs/production/edit/here/yes.csv"
    )


@pytest.mark.django_db
@patch("workflows.views.move_samplesheet_to_editable_location")
def test_samplesheet_fetch(mock_move_samplesheet, client, admin_client, settings):
    mock_move_samplesheet.return_value = None

    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_slurm = (
        "/nfs/production/edit/here"
    )
    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_server = "/app/data/edit/here"
    settings.EMG_CONFIG.slurm.samplesheet_editing_allowed_inside = (
        "/nfs/production/edit/here"
    )
    settings.EMG_CONFIG.slurm.samplesheet_editing_path_from_shared_filesystem = (
        "samplesheet_edits_here"
    )

    samplesheet_encoded = encode_samplesheet_path("/nfs/production/edit/here/ss.csv")

    fetch_view_url = reverse_lazy(
        "workflows:edit_samplesheet_fetch",
        kwargs={"filepath_encoded": samplesheet_encoded},
    )
    response = client.get(fetch_view_url)

    # No auth - get redirect to admin login
    assert mock_move_samplesheet.call_count == 0
    assert response.status_code == 302
    assert "admin/login" in response.url

    # Staff can edit samplesheets
    response = admin_client.get(fetch_view_url)
    assert mock_move_samplesheet.call_count == 1
    assert response.status_code == 302
    assert response.url == reverse_lazy(
        "workflows:edit_samplesheet_edit",
        kwargs={"filepath_encoded": samplesheet_encoded},
    )


# TODO: test of edit view/post