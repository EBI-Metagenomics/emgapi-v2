from types import SimpleNamespace
from unittest.mock import patch

from analyses.base_models.with_downloads_models import DownloadType
from analyses.schemas import MGnifyAnalysisDownloadFile
from workflows.curations.predict_biomes import convert_taxonomy_download


def taxonomy_download(path, group):
    return SimpleNamespace(
        path=path,
        download_type=DownloadType.TAXONOMIC_ANALYSIS,
        download_group=group,
        parent_results_dir="/results",
        parent_is_private=False,
    )


def test_converter_accepts_mapseq_downloads_only():
    download = taxonomy_download(
        "SSU.fasta.mseq.tsv", "taxonomies.closed_reference.ssu"
    )
    with patch.object(
        MGnifyAnalysisDownloadFile,
        "_build_file_url",
        return_value="https://example.test/results/SSU.fasta.mseq.tsv",
    ):
        assert (
            convert_taxonomy_download(download)
            == "https://example.test/results/SSU.fasta.mseq.tsv"
        )

    download = taxonomy_download("SILVA-SSU.mseq", "taxonomy")
    with patch.object(
        MGnifyAnalysisDownloadFile,
        "_build_file_url",
        return_value="https://example.test/results/SILVA-SSU.mseq",
    ):
        assert (
            convert_taxonomy_download(download)
            == "https://example.test/results/SILVA-SSU.mseq"
        )


def test_converter_rejects_non_mapseq_downloads():
    assert convert_taxonomy_download(taxonomy_download("ssu.tsv", "taxonomy")) is None
    assert (
        convert_taxonomy_download(taxonomy_download("contigs.tsv.gz", "taxonomy"))
        is None
    )
    assert (
        convert_taxonomy_download(taxonomy_download("krona.txt.gz", "taxonomy")) is None
    )
    assert (
        convert_taxonomy_download(taxonomy_download("krona.html", "taxonomy")) is None
    )
    download = taxonomy_download("SSU.mseq", "quality_control")
    download.download_type = DownloadType.QUALITY_CONTROL
    assert convert_taxonomy_download(download) is None
