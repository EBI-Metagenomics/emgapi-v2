import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from analyses.base_models.with_downloads_models import DownloadType
from analyses.schemas import MGnifyAnalysisDownloadFile
from workflows.curations.predict_biomes import (
    classify_study,
    convert_taxonomy_download,
    get_study_sample_description,
)


def taxonomy_download(path, group):
    return SimpleNamespace(
        path=path,
        download_type=DownloadType.TAXONOMIC_ANALYSIS,
        download_group=group,
        parent_results_dir="/results",
        parent_is_private=False,
    )


@patch.object(
    MGnifyAnalysisDownloadFile,
    "_build_file_url",
    return_value="https://example.test/results/SSU.fasta.mseq.tsv",
)
def test_converter_accepts_mapseq_downloads_only(build_file_url):
    download = taxonomy_download(
        "SSU.fasta.mseq.tsv", "taxonomies.closed_reference.ssu"
    )
    assert (
        convert_taxonomy_download(download)
        == "https://example.test/results/SSU.fasta.mseq.tsv"
    )

    download = taxonomy_download("SILVA-SSU.mseq", "taxonomy")
    build_file_url.return_value = "https://example.test/results/SILVA-SSU.mseq"
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


def test_study_description_uses_existing_description_metadata():
    study = SimpleNamespace(
        title="Study title", metadata={"description": "Study details"}
    )
    sample = SimpleNamespace(sample_title="Sample title", metadata={})

    assert get_study_sample_description(study, sample) == (
        "Study title Study details",
        "Sample title",
    )


@patch.dict("os.environ", {"TRAPICHE_SINGULARITY_IMAGE": "trapiche.sif"})
@patch("workflows.curations.predict_biomes.latest_taxonomy_analysis")
@patch("workflows.curations.predict_biomes.trapiche_input")
@patch("workflows.curations.predict_biomes.Client.execute")
def test_classify_study_runs_trapiche_cli_in_singularity(
    run, build_input, latest_analysis
):
    sample = SimpleNamespace()
    analysis = Mock()
    study = SimpleNamespace(samples=Mock())
    study.samples.all.return_value = [sample]
    latest_analysis.return_value = analysis
    build_input.return_value = {"sample_id": "S1"}

    def execute(image, command, **kwargs):
        output_path = Path(command[1]).with_name("input_trapiche_results.ndjson")
        output_path.write_text(
            json.dumps({"final_selected_prediction": {"root:Environmental:Soil": 0.9}})
            + "\n",
            encoding="utf-8",
        )
        return {"return_code": 0, "message": []}

    run.side_effect = execute
    predictions = classify_study.fn(study)

    assert predictions[analysis].lineage == "root:Environmental:Soil"
    assert predictions[analysis].confidence == 0.9
    run.assert_called_once()
    assert run.call_args.args[0] == "trapiche.sif"
    assert run.call_args.args[1][0] == "trapiche"
    assert run.call_args.kwargs["return_result"] is True
