import io
import json
from pathlib import Path

import pytest
from django.core.management import call_command

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis, Assembly


def _create_analysis(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    accession: str,
    pipeline_version: str,
    external_results_dir: str,
    experiment_type: str = Analysis.ExperimentTypes.AMPLICON,
    assembly=None,
):
    return Analysis.objects.create(
        accession=accession,
        study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_study=raw_reads_mgnify_study.ena_study,
        pipeline_version=pipeline_version,
        experiment_type=experiment_type,
        external_results_dir=external_results_dir,
        assembly=assembly,
    )


def _json_output_lines(output: str):
    return [json.loads(line) for line in output.splitlines() if line.startswith("{")]


def _touch_files(root: Path, external_results_dir: str, paths: list[str]):
    analysis_root = root / external_results_dir
    for path in paths:
        file_path = analysis_root / path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text("legacy data")


@pytest.mark.django_db
def test_add_legacy_known_files_to_analysis_downloads_dry_run_does_not_save(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    external_results_dir = "ERP123/ERP123456/version_5.0/ERZ1/001/ERZ1_FASTA"
    public_results_root = tmp_path / "public"
    private_results_root = tmp_path / "private"
    analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003001",
        pipeline_version=Analysis.PipelineVersions.v5,
        external_results_dir=external_results_dir,
    )
    _touch_files(
        public_results_root,
        external_results_dir,
        [
            "qc_summary",
            "qc-statistics/seq-length.out.full",
            f"taxonomy-summary/SSU/{analysis.accession}_SSU.fasta.mseq.tsv",
            f"taxonomy-summary/LSU/{analysis.accession}_LSU.fasta.mseq.txt",
        ],
    )

    stdout = io.StringIO()
    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--accession",
        analysis.accession,
        "--dry-run",
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
        stdout=stdout,
    )

    analysis.refresh_from_db()
    assert analysis.downloads == []

    output_downloads = _json_output_lines(stdout.getvalue())
    assert len(output_downloads) == 4
    assert {row["download"]["path"] for row in output_downloads} >= {
        "qc_summary",
        f"taxonomy-summary/SSU/{analysis.accession}_SSU.fasta.mseq.tsv",
        f"taxonomy-summary/LSU/{analysis.accession}_LSU.fasta.mseq.txt",
    }
    assert (
        output_downloads[0]["external_path"]
        == "ERP123/ERP123456/version_5.0/ERZ1/001/ERZ1_FASTA/qc_summary"
    )


@pytest.mark.django_db
def test_add_legacy_known_files_to_analysis_downloads_adds_v4_1_assembly_files_idempotently(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    external_results_dir = "ERP123/ERP123456/version_4.1/ERZ000/001/ERZ000001_FASTA"
    public_results_root = tmp_path / "public"
    private_results_root = tmp_path / "private"
    assembly = Assembly.objects.create(
        ena_study=raw_reads_mgnify_study.ena_study,
        reads_study=raw_reads_mgnify_study,
        sample=raw_reads_mgnify_sample[0],
        ena_accessions=["ERZ000001"],
    )
    analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003002",
        pipeline_version=Analysis.PipelineVersions.v4_1,
        external_results_dir=external_results_dir,
        experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
        assembly=assembly,
    )
    _touch_files(
        public_results_root,
        external_results_dir,
        [
            "qc-statistics/summary.out",
            "ERZ000001_FASTA_summary.ipr",
            "ERZ000001_FASTA_summary.go",
        ],
    )
    analysis.add_download(
        DownloadFile(
            path="qc-statistics/summary.out",
            alias="qc-statistics/summary.out",
            file_type=DownloadFileType.OTHER,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group=Analysis.QC,
            short_description="Legacy quality control file",
            long_description="Quality control file",
        )
    )

    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--accession",
        analysis.accession,
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
    )
    analysis.refresh_from_db()
    first_run_downloads = list(analysis.downloads)

    paths = {download["path"] for download in first_run_downloads}
    groups_by_path = {
        download["path"]: download["download_group"] for download in first_run_downloads
    }
    assert "ERZ000001_FASTA_summary.ipr" in paths
    assert "ERZ000001_FASTA_summary.go" in paths
    assert (
        groups_by_path["ERZ000001_FASTA_summary.ipr"]
        == f"{Analysis.FUNCTIONAL_ANNOTATION}.interpro"
    )
    assert (
        groups_by_path["ERZ000001_FASTA_summary.go"]
        == f"{Analysis.FUNCTIONAL_ANNOTATION}.go_slims"
    )
    assert [download["path"] for download in first_run_downloads].count(
        "qc-statistics/summary.out"
    ) == 1

    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--accession",
        analysis.accession,
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
    )
    analysis.refresh_from_db()
    assert analysis.downloads == first_run_downloads


@pytest.mark.django_db
def test_add_legacy_known_files_to_analysis_downloads_v5_scope(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    v5_external_results_dir = "ERP123/ERP123456/version_5.0/ERZ1/001/ERZ1_FASTA"
    public_results_root = tmp_path / "public"
    private_results_root = tmp_path / "private"
    v5_analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003003",
        pipeline_version=Analysis.PipelineVersions.v5,
        external_results_dir=v5_external_results_dir,
    )
    v4_1_analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003004",
        pipeline_version=Analysis.PipelineVersions.v4_1,
        external_results_dir="ERP123/ERP123456/version_4.1/ERZ1/001/ERZ1_FASTA",
    )
    _touch_files(public_results_root, v5_external_results_dir, ["qc_summary"])

    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--v5",
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
    )

    v5_analysis.refresh_from_db()
    v4_1_analysis.refresh_from_db()
    assert v5_analysis.downloads
    assert v4_1_analysis.downloads == []


@pytest.mark.django_db
def test_add_legacy_known_files_to_analysis_downloads_uses_detected_taxonomy_marker(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    external_results_dir = "ERP123/ERP123456/version_5.0/ERZ1/001/ERZ1_FASTA"
    public_results_root = tmp_path / "public"
    private_results_root = tmp_path / "private"
    analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003005",
        pipeline_version=Analysis.PipelineVersions.v5,
        external_results_dir=external_results_dir,
    )
    _touch_files(
        public_results_root,
        external_results_dir,
        [
            "taxonomy-summary/SSU/krona.html",
            "tax/ssu_tax/BUGS_SSU.fasta.mseq_hdf5.tsv",
        ],
    )
    analysis.add_download(
        DownloadFile(
            path="tax/ssu_tax/BUGS_SSU.fasta.mseq_hdf5.biom",
            alias="BUGS_SSU_OTU_TABLE_HDF5.biom",
            file_type=DownloadFileType.BIOM,
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            download_group="all",
            short_description="SSU BIOM",
            long_description="SSU BIOM",
        )
    )

    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--accession",
        analysis.accession,
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
    )

    analysis.refresh_from_db()
    paths = {download["path"] for download in analysis.downloads}
    groups_by_path = {
        download["path"]: download["download_group"] for download in analysis.downloads
    }
    assert "taxonomy-summary/SSU/krona.html" in paths
    assert "tax/ssu_tax/BUGS_SSU.fasta.mseq_hdf5.tsv" in paths
    assert (
        groups_by_path["taxonomy-summary/SSU/krona.html"]
        == f"{Analysis.TAXONOMIES}.{Analysis.CLOSED_REFERENCE}.ssu"
    )
    assert "taxonomy-summary/LSU/krona.html" not in paths


@pytest.mark.django_db
def test_add_legacy_known_files_to_analysis_downloads_uses_private_local_root(
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
    tmp_path,
):
    external_results_dir = "private/ERP123/version_5.0/ERZ1/001/ERZ1_FASTA"
    public_results_root = tmp_path / "public"
    private_results_root = tmp_path / "private"
    analysis = _create_analysis(
        raw_reads_mgnify_study,
        raw_reads_mgnify_sample,
        accession="MGYA00003006",
        pipeline_version=Analysis.PipelineVersions.v5,
        external_results_dir=external_results_dir,
    )
    analysis.is_private = True
    analysis.save(update_fields=["is_private"])
    _touch_files(private_results_root, external_results_dir, ["qc_summary"])

    call_command(
        "add_legacy_known_files_to_analysis_downloads",
        "--accession",
        analysis.accession,
        "--public-results-root",
        str(public_results_root),
        "--private-results-root",
        str(private_results_root),
    )

    analysis.refresh_from_db()
    assert {download["path"] for download in analysis.downloads} == {"qc_summary"}
    assert analysis.downloads[0]["download_group"] == Analysis.QC
