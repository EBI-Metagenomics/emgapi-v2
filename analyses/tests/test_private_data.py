from urllib.parse import urlparse, parse_qs

import pytest
from django.conf import settings

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileIndexFile,
    DownloadType,
    DownloadFileType,
)
from analyses.schemas import (
    MGnifyStudyDownloadFile,
    MGnifyAnalysisDownloadFile,
    MGnifyAnalysisDetail,
    MGnifyStudyDetail,
)


def _download_with_parent_context(parent_obj, download_file, schema_class):
    """
    Validate a DownloadFile into a schema instance with parent context injected.

    :param parent_obj: The parent Django model (Analysis or Study).
    :param download_file: The DownloadFile pydantic model.
    :param schema_class: The schema class to validate into.
    :return: Schema instance with parent context.
    """
    return schema_class.model_validate(
        {
            **download_file.model_dump(),
            "parent_is_private": parent_obj.is_private,
            "parent_results_dir": parent_obj.external_results_dir,
        }
    )


def _assert_is_presigned_url(url):
    """Assert that a URL has pre-signed query parameters."""
    assert url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in url
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    assert "token" in query_params
    assert "expires" in query_params


@pytest.mark.django_db
def test_private_study_download_url(private_study_with_download):
    """Test that a private study download file has a secure URL."""
    download_file = private_study_with_download.downloads_as_objects[0]
    study_download = _download_with_parent_context(
        private_study_with_download, download_file, MGnifyStudyDownloadFile
    )
    url = MGnifyStudyDownloadFile.resolve_url(study_download)
    _assert_is_presigned_url(url)


@pytest.mark.django_db
def test_private_analysis_download_url(private_analysis_with_download):
    """Test that a private analysis download file has a secure URL."""
    download_file = private_analysis_with_download.downloads_as_objects[0]
    analysis_download = _download_with_parent_context(
        private_analysis_with_download, download_file, MGnifyAnalysisDownloadFile
    )
    url = MGnifyAnalysisDownloadFile.resolve_url(analysis_download)
    _assert_is_presigned_url(url)


@pytest.mark.django_db
def test_public_vs_private_study_urls(
    raw_reads_mgnify_study, private_study_with_download
):
    """Test that public and private study download URLs are different."""
    if not raw_reads_mgnify_study.external_results_dir:
        raw_reads_mgnify_study.external_results_dir = "MGYS/00/000/001"
        raw_reads_mgnify_study.save()

    if not raw_reads_mgnify_study.downloads:
        raw_reads_mgnify_study.add_download(
            DownloadFile(
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                file_type=DownloadFileType.TSV,
                alias=f"{raw_reads_mgnify_study.accession}_public_study_summary.tsv",
                short_description="Public study summary",
                long_description="Summary of taxonomic assignments for public study",
                path="study-summaries/public_study_summary.tsv",
                download_group="study_summary.public",
            )
        )

    public_download = _download_with_parent_context(
        raw_reads_mgnify_study,
        raw_reads_mgnify_study.downloads_as_objects[0],
        MGnifyStudyDownloadFile,
    )
    private_download = _download_with_parent_context(
        private_study_with_download,
        private_study_with_download.downloads_as_objects[0],
        MGnifyStudyDownloadFile,
    )

    public_url = MGnifyStudyDownloadFile.resolve_url(public_download)
    private_url = MGnifyStudyDownloadFile.resolve_url(private_download)

    assert public_url != private_url
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root in public_url
    _assert_is_presigned_url(private_url)


@pytest.mark.django_db
def test_private_analysis_results_dir(private_analysis_with_download):
    """Test that a private analysis results_dir uses a secure private URL."""
    url = MGnifyAnalysisDetail.resolve_results_dir(private_analysis_with_download)
    _assert_is_presigned_url(url)
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root not in url


@pytest.mark.django_db
def test_public_analysis_results_dir(amplicon_analysis_with_downloads, prefect_harness):
    """Test that a public analysis results_dir uses the public transfer services URL."""
    url = MGnifyAnalysisDetail.resolve_results_dir(amplicon_analysis_with_downloads)

    assert url is not None
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root in url
    assert settings.EMG_CONFIG.service_urls.private_data_url_root not in url


@pytest.mark.django_db
def test_private_analysis_index_file_url(private_analysis_with_indexed_download):
    """Test that index files for a private analysis get pre-signed URLs."""
    download_file = _download_with_parent_context(
        private_analysis_with_indexed_download,
        private_analysis_with_indexed_download.downloads_as_objects[0],
        MGnifyAnalysisDownloadFile,
    )
    index_files = MGnifyAnalysisDownloadFile.resolve_index_files(download_file)

    assert index_files is not None
    assert len(index_files) == 1
    _assert_is_presigned_url(index_files[0].url)


@pytest.mark.django_db
def test_private_study_index_file_url(private_study_with_indexed_download):
    """Test that index files for a private study get pre-signed URLs."""
    download_file = _download_with_parent_context(
        private_study_with_indexed_download,
        private_study_with_indexed_download.downloads_as_objects[0],
        MGnifyStudyDownloadFile,
    )
    index_files = MGnifyStudyDownloadFile.resolve_index_files(download_file)

    assert index_files is not None
    assert len(index_files) == 1
    _assert_is_presigned_url(index_files[0].url)


@pytest.mark.django_db
def test_public_analysis_index_file_url(raw_read_analyses):
    """Test that index files for a public analysis get regular (non-signed) URLs."""
    analysis = raw_read_analyses[0]
    dl = DownloadFile(
        alias="sequences.fasta.gz",
        short_description="Sequences",
        file_type=DownloadFileType.FASTA,
        download_type=DownloadType.SEQUENCE_DATA,
        long_description="Sequences with index",
        path="sequences.fasta.gz",
        download_group="all.sequences",
        index_file=DownloadFileIndexFile(
            path="sequences.fasta.gz.gzi",
            index_type="gzi",
        ),
        parent_identifier=analysis.accession,
    )
    download_file = _download_with_parent_context(
        analysis, dl, MGnifyAnalysisDownloadFile
    )
    index_files = MGnifyAnalysisDownloadFile.resolve_index_files(download_file)

    assert index_files is not None
    assert len(index_files) == 1
    index_url = index_files[0].url

    assert index_url is not None
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root in index_url
    assert "token" not in index_url


@pytest.mark.django_db
def test_private_analysis_downloads_via_detail_resolver(
    private_analysis_with_indexed_download,
):
    """Test that MGnifyAnalysisDetail.resolve_downloads injects parent context correctly."""
    downloads = MGnifyAnalysisDetail.resolve_downloads(
        private_analysis_with_indexed_download
    )

    assert len(downloads) == 1
    dl = downloads[0]

    _assert_is_presigned_url(dl.url)

    assert dl.index_files is not None
    assert len(dl.index_files) == 1
    _assert_is_presigned_url(dl.index_files[0].url)


@pytest.mark.django_db
def test_private_study_downloads_via_detail_resolver(
    private_study_with_indexed_download,
):
    """Test that MGnifyStudyDetail.resolve_downloads injects parent context correctly."""
    downloads = MGnifyStudyDetail.resolve_downloads(private_study_with_indexed_download)

    assert len(downloads) == 1
    dl = downloads[0]

    _assert_is_presigned_url(dl.url)

    assert dl.index_files is not None
    assert len(dl.index_files) == 1
    _assert_is_presigned_url(dl.index_files[0].url)
