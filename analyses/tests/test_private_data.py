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
)


@pytest.mark.django_db
def test_private_study_download_url(private_study_with_download):
    """Test that a private study download file has a secure URL."""
    # Get the download file
    download_file = private_study_with_download.downloads_as_objects[0]

    # Convert to MGnifyStudyDownloadFile to access the url property
    study_download_file = MGnifyStudyDownloadFile.model_validate(
        download_file.model_dump()
    )

    # Get the URL
    url = MGnifyStudyDownloadFile.resolve_url(study_download_file)

    # Check that the URL is a secure URL
    assert url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in url

    # Parse the URL to check for secure link parameters
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    # Check that the URL has the required secure link parameters
    assert "token" in query_params
    assert "expires" in query_params


@pytest.mark.django_db
def test_private_analysis_download_url(private_analysis_with_download):
    """Test that a private analysis download file has a secure URL."""
    # Get the download file
    download_file = private_analysis_with_download.downloads_as_objects[0]

    # Convert to MGnifyAnalysisDownloadFile to access the url property
    analysis_download_file = MGnifyAnalysisDownloadFile.model_validate(
        download_file.model_dump()
    )

    # Get the URL
    url = MGnifyAnalysisDownloadFile.resolve_url(analysis_download_file)

    # Check that the URL is a secure URL
    assert url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in url

    # Parse the URL to check for secure link parameters
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    # Check that the URL has the required secure link parameters
    assert "token" in query_params
    assert "expires" in query_params


@pytest.mark.django_db
def test_public_vs_private_study_urls(
    raw_reads_mgnify_study, private_study_with_download
):
    """Test that public and private study download URLs are different."""
    if not raw_reads_mgnify_study.external_results_dir:
        raw_reads_mgnify_study.external_results_dir = "MGYS/00/000/001"
        raw_reads_mgnify_study.save()

    # Add a download to the public study if it doesn't have one
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

    # Get the download files
    public_download = raw_reads_mgnify_study.downloads_as_objects[0]
    private_download = private_study_with_download.downloads_as_objects[0]

    # Convert to MGnifyStudyDownloadFile to access the url property
    public_study_download = MGnifyStudyDownloadFile.model_validate(
        public_download.model_dump()
    )
    private_study_download = MGnifyStudyDownloadFile.model_validate(
        private_download.model_dump()
    )

    # Get the URLs
    public_url = MGnifyStudyDownloadFile.resolve_url(public_study_download)
    private_url = MGnifyStudyDownloadFile.resolve_url(private_study_download)

    # Check that the URLs are different
    assert public_url != private_url

    # Check that the public URL uses the transfer_services_url_root
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root in public_url

    # Check that the private URL uses the private_data_url_root
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in private_url

    # Check that the private URL has secure link parameters
    parsed_url = urlparse(private_url)
    query_params = parse_qs(parsed_url.query)
    assert "token" in query_params
    assert "expires" in query_params


@pytest.mark.django_db
def test_private_analysis_results_dir(private_analysis_with_download):
    """Test that a private analysis results_dir uses a secure private URL."""
    url = MGnifyAnalysisDetail.resolve_results_dir(private_analysis_with_download)

    assert url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in url
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root not in url

    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    assert "token" in query_params
    assert "expires" in query_params


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
    download_file = private_analysis_with_indexed_download.downloads_as_objects[0]

    index_files = MGnifyAnalysisDownloadFile.resolve_index_files(download_file)

    assert index_files is not None
    assert len(index_files) == 1
    index_url = index_files[0].url

    assert index_url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in index_url

    parsed = urlparse(index_url)
    query_params = parse_qs(parsed.query)
    assert "token" in query_params
    assert "expires" in query_params


@pytest.mark.django_db
def test_private_study_index_file_url(private_study_with_indexed_download):
    """Test that index files for a private study get pre-signed URLs."""
    download_file = private_study_with_indexed_download.downloads_as_objects[0]

    index_files = MGnifyStudyDownloadFile.resolve_index_files(download_file)

    assert index_files is not None
    assert len(index_files) == 1
    index_url = index_files[0].url

    assert index_url is not None
    assert settings.EMG_CONFIG.service_urls.private_data_url_root in index_url

    parsed = urlparse(index_url)
    query_params = parse_qs(parsed.query)
    assert "token" in query_params
    assert "expires" in query_params


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
        parent_is_private=analysis.is_private,
        parent_results_dir=analysis.external_results_dir,
    )

    index_files = MGnifyAnalysisDownloadFile.resolve_index_files(dl)

    assert index_files is not None
    assert len(index_files) == 1
    index_url = index_files[0].url

    assert index_url is not None
    assert settings.EMG_CONFIG.service_urls.transfer_services_url_root in index_url
    assert "token" not in index_url
