from __future__ import annotations

import gzip
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from workflows.flows.housekeeping.fix_eggnog_tsv_headers import (
    repair_eggnog_tsv_headers,
    resync_eggnog_results_to_ftp,
)


def _write_gzipped_tsv(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as handle:
        handle.writelines(lines)


@pytest.mark.django_db
@patch(
    "workflows.flows.housekeeping.fix_eggnog_tsv_headers.collect_eggnog_header_fix_candidates"
)
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers.repair_eggnog_header_files")
@patch(
    "workflows.flows.housekeeping.fix_eggnog_tsv_headers.resync_eggnog_results_to_ftp"
)
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers.get_run_logger")
def test_repair_eggnog_tsv_headers_dry_run_lists_only(
    mock_logger,
    mock_resync,
    mock_repair,
    mock_collect,
    prefect_harness,
):
    mock_logger.return_value = Mock()
    candidates = [
        {
            "analysis_accession": "MGYA00000001",
            "download_alias": "sample_emapper_annotations.tsv.gz",
            "nfs_path": "/tmp/results/eggnog/sample_emapper_annotations.tsv.gz",
            "external_path": "/tmp/ftp/analyses/MGYA00000001/eggnog/sample_emapper_annotations.tsv.gz",
            "has_duplicate_header": True,
        }
    ]
    mock_collect.return_value = candidates

    result = repair_eggnog_tsv_headers(dry_run=True)

    assert result == candidates
    mock_collect.assert_called_once_with(None)
    mock_repair.assert_not_called()
    mock_resync.assert_not_called()


@pytest.mark.django_db
@patch(
    "workflows.flows.housekeeping.fix_eggnog_tsv_headers.collect_eggnog_header_fix_candidates"
)
@patch(
    "workflows.flows.housekeeping.fix_eggnog_tsv_headers.resync_eggnog_results_to_ftp"
)
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers.get_run_logger")
def test_repair_eggnog_tsv_headers_repairs_creates_backup_and_syncs(
    mock_logger,
    mock_resync,
    mock_collect,
    tmp_path,
    prefect_harness,
):
    mock_logger.return_value = Mock()
    target = tmp_path / "results" / "eggnog" / "sample_emapper_annotations.tsv.gz"
    _write_gzipped_tsv(
        target,
        [
            "#query\tseed_ortholog\n",
            "#query\tseed_ortholog\n",
            "gene1\tortholog1\n",
        ],
    )
    candidates = [
        {
            "analysis_accession": "MGYA00000001",
            "download_alias": "sample_emapper_annotations.tsv.gz",
            "nfs_path": str(target),
            "external_path": "/tmp/ftp/analyses/MGYA00000001/eggnog/sample_emapper_annotations.tsv.gz",
            "has_duplicate_header": True,
        }
    ]
    mock_collect.return_value = candidates

    result = repair_eggnog_tsv_headers(sync_to_ftp=True)

    assert result == [
        {
            "analysis_accession": "MGYA00000001",
            "download_alias": "sample_emapper_annotations.tsv.gz",
            "nfs_path": str(target),
            "external_path": "/tmp/ftp/analyses/MGYA00000001/eggnog/sample_emapper_annotations.tsv.gz",
            "has_duplicate_header": True,
            "backup_path": str(
                target.with_name("sample_emapper_annotations.tsv.gz.bak")
            ),
        }
    ]
    backup_path = target.with_name("sample_emapper_annotations.tsv.gz.bak")
    assert backup_path.exists()
    with gzip.open(target, "rt") as handle:
        assert handle.read().splitlines() == [
            "#query\tseed_ortholog",
            "gene1\tortholog1",
        ]
    with gzip.open(backup_path, "rt") as handle:
        assert handle.read().splitlines() == [
            "#query\tseed_ortholog",
            "#query\tseed_ortholog",
            "gene1\tortholog1",
        ]
    mock_collect.assert_called_once_with(None)
    mock_resync.assert_called_once_with(result)


@pytest.mark.django_db
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers._eggnog_analyses")
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers.run_deployment")
@patch("workflows.flows.housekeeping.fix_eggnog_tsv_headers.get_run_logger")
def test_resync_eggnog_results_to_ftp_uses_rsync(
    mock_logger,
    mock_run_deployment,
    mock_analyses,
    tmp_path,
    prefect_harness,
):
    mock_logger.return_value = Mock()
    analysis_root = tmp_path / "results"
    analysis = Mock(
        accession="MGYA00000001",
        results_dir=str(analysis_root),
        external_results_dir="analyses/MGYA00000001",
        is_private=False,
    )
    mock_analyses.return_value = [analysis]

    candidate = {
        "analysis_accession": "MGYA00000001",
        "download_alias": "sample_emapper_annotations.tsv.gz",
        "nfs_path": str(analysis_root / "eggnog" / "sample_emapper_annotations.tsv.gz"),
        "external_path": "/tmp/ftp/analyses/MGYA00000001/eggnog/sample_emapper_annotations.tsv.gz",
        "has_duplicate_header": True,
    }

    resync_eggnog_results_to_ftp([candidate])

    mock_run_deployment.assert_called_once()
    call_kwargs = mock_run_deployment.call_args.kwargs
    assert call_kwargs["parameters"]["move_command"] == "rsync -av"
    assert call_kwargs["parameters"]["source"] == [
        str(analysis_root / "eggnog" / "sample_emapper_annotations.tsv.gz")
    ]
