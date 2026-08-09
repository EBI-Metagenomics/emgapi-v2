import uuid
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import pytest
from django.conf import settings
from django.http import Http404
from django.urls import reverse

from workflows.nextflow_utils.samplesheets import (
    location_for_samplesheet_to_be_edited,
    location_where_samplesheet_was_edited,
)
from workflows.views import encode_samplesheet_path, validate_samplesheet_path

EMG_CONFIG = settings.EMG_CONFIG


def test_samplesheet_editor_paths_validation(settings):
    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_slurm = (
        "/nfs/production/edit/here"
    )
    settings.EMG_CONFIG.slurm.shared_filesystem_root_on_server = "/app/data/edit/here"

    settings.EMG_CONFIG.slurm.samplesheet_editing_allowed_inside = (
        "/nfs/production/edit/here"
    )

    samplesheet = "/nfs/production/edit/here/yes.csv"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    assert validate_samplesheet_path(samplesheet_encoded) == Path(samplesheet)

    samplesheet = "/nfs/production/edit/here/../outside/secret.csv"
    samplesheet_encoded = encode_samplesheet_path(samplesheet)
    with pytest.raises(Http404):
        validate_samplesheet_path(samplesheet_encoded)

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
    assert location_for_samplesheet_to_be_edited(
        Path(samplesheet), EMG_CONFIG.slurm.shared_filesystem_root_on_server
    ) == Path("/app/data/edit/here/samplesheet_edits_here/for_editing/yes.csv")

    # and if it was written, it'd be to from_editing subdir
    assert location_where_samplesheet_was_edited(
        Path(samplesheet), EMG_CONFIG.slurm.shared_filesystem_root_on_server
    ) == Path("/app/data/edit/here/samplesheet_edits_here/from_editing/yes.csv")

    # but the cluster would see it on the datamover fs
    assert location_where_samplesheet_was_edited(
        Path(samplesheet), EMG_CONFIG.slurm.shared_filesystem_root_on_slurm
    ) == Path("/nfs/production/edit/here/samplesheet_edits_here/from_editing/yes.csv")


@pytest.mark.django_db
@patch("workflows.views.move_samplesheet_to_editable_location")
def test_samplesheet_fetch(mock_move_samplesheet, client, admin_client, settings):

    class FakeFlowrun:
        id: uuid.UUID = uuid.UUID("eec1f603-7d7f-40a3-9c11-1bc24dc0ff54")

    mock_move_samplesheet.return_value = FakeFlowrun, Path("/")

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

    fetch_view_url = reverse(
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
    # should be redirected to the page awaiting the datamover, and then to the edit view

    edit_url = reverse(
        "workflows:edit_samplesheet_edit",
        kwargs={"filepath_encoded": samplesheet_encoded},
    )

    assert response.url == reverse(
        "workflows:wait_for_flowrun",
        kwargs={
            "flowrun_id": str(FakeFlowrun.id),
            "next_url": quote(edit_url, safe=""),
        },
    )


@pytest.mark.django_db
@patch("workflows.views.move_samplesheet_to_editable_location")
def test_samplesheet_fetch_rejects_parent_dir_escape(
    mock_move_samplesheet, admin_client, settings
):
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

    # This is the explicit path-traversal PoC shape: it starts inside the allowed
    # directory but resolves outside it once ".." is applied.
    traversal_samplesheet = "/nfs/production/edit/here/../outside/secret.csv"
    traversal_encoded = encode_samplesheet_path(traversal_samplesheet)

    fetch_view_url = reverse(
        "workflows:edit_samplesheet_fetch",
        kwargs={"filepath_encoded": traversal_encoded},
    )
    response = admin_client.get(fetch_view_url)

    assert response.status_code == 404
    assert mock_move_samplesheet.call_count == 0


# TODO: test of edit view/post
