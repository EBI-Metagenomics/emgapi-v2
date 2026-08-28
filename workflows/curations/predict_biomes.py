import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from prefect.logging import get_logger
from pydantic import BaseModel
from spython.main import Client

from analyses.base_models.with_downloads_models import DownloadType
from analyses.models import Analysis, Sample, Study
from analyses.schemas import MGnifyAnalysisDownloadFile
from curations.models import TrapicheBiomeCuration
from emgapiv2.biome_lineage_utils import normalize_lineage
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


class PredictionResult(BaseModel):
    """Normalized prediction returned by a biome classifier."""

    lineage: str = ""
    confidence: float | None = None
    method: str = "trapiche"
    source: str = "trapiche"
    version: str = ""
    configuration: dict[str, Any] | None = (
        None  # TODO: to capture how was ran, text-only, tax....
    )
    raw_result: dict[str, Any] | None = None


def latest_taxonomy_analysis(sample: Sample) -> Analysis | None:
    """Return the newest ready analysis containing taxonomy annotations."""
    analyses = sample.analyses.filter(is_ready=True).order_by("-updated_at", "-id")
    for analysis in analyses:
        if (analysis.annotations or {}).get(Analysis.TAXONOMIES):
            return analysis
    return None


def get_study_sample_description(study: Study, sample: Sample) -> tuple[str, str]:
    """Get the study and sample title and description (if available) for Trapiche."""
    study_text = " ".join(
        str(part)
        for part in (
            study.title,
            study.metadata.get("description")
            or study.metadata.get("study_description", ""),
        )
        if part
    )
    sample_text = " ".join(
        str(part)
        for part in (sample.sample_title, sample.metadata.get("description", ""))
        if part
    )
    return study_text, sample_text


def trapiche_input(
    study: Study, sample: Sample, analysis: Analysis | None
) -> dict[str, Any]:
    """Build one Trapiche input row from a study, sample, and optional analysis."""
    study_text, sample_text = get_study_sample_description(study, sample)
    row = {
        "project_id": study.first_accession,
        "sample_id": sample.first_accession,
    }

    # Trapiche's preferred text input is an externally supplied biome label.
    # Existing curated biomes are valid external labels; descriptions remain a
    # fallback for records without one.
    if study.biome_id:
        row["ext_text_pred_project"] = [study.biome.pretty_lineage]
    else:
        row["project_description_text"] = study_text

    if sample.biome_id:
        row["ext_text_pred_sample"] = [sample.biome.pretty_lineage]
    elif sample_text:
        row["sample_description_text"] = sample_text

    if analysis:
        row["sample_taxonomy_paths"] = [
            path
            for download in MGnifyAnalysisDownloadFile.from_parent(
                analysis, analysis.downloads_as_objects
            )
            if (path := convert_taxonomy_download(download))
        ]
        row["analysis_id"] = analysis.accession
    return row


def convert_taxonomy_download(download: MGnifyAnalysisDownloadFile) -> str | None:
    """Convert a MAPseq download into a Trapiche input URL."""

    # TODO: this is generating an URL (for the FTP path of the file).
    #       we should probably be generating the _path_ as this is meant to run internally.

    if (
        download.parent_results_dir is None
        or download.download_type != DownloadType.TAXONOMIC_ANALYSIS
    ):
        return None

    file_url = MGnifyAnalysisDownloadFile._build_file_url(
        download.path,
        download.parent_results_dir,
        download.parent_is_private,
    )

    if file_url and ".mseq" in str(download.path).lower():
        return file_url

    return None


def convert_trapiche_response(raw: Any) -> PredictionResult:
    """Convert a Trapiche response item into a normalized prediction result."""
    if isinstance(raw, PredictionResult):
        return raw

    if not isinstance(raw, dict):
        return PredictionResult()

    selected = (
        raw.get("final_selected_prediction")
        or raw.get("constrained_unambiguous_prediction")
        or raw.get("raw_unambiguous_prediction")
        or raw.get("raw_refined_prediction")
    )

    if isinstance(selected, dict):
        lineage, confidence = next(iter(selected.items()), ("", None))
    elif isinstance(selected, (list, tuple)) and selected:
        lineage, confidence = selected[0], selected[1] if len(selected) > 1 else None
    else:
        lineage, confidence = (
            raw.get("predicted_lineage") or raw.get("lineage") or "",
            raw.get("confidence"),
        )

    return PredictionResult(
        lineage=normalize_lineage(lineage),
        confidence=confidence,
        method=raw.get("method", "trapiche"),
        source=raw.get("source", "trapiche"),
        version="v1",  # TODO: fill this with the trapiche version
        configuration={},  # TODO: capture how the workflow was run, text-only, tax....
        raw_result=raw,
    )


@task(name="Classify a study using Trapiche")
def classify_study(
    study: Study,
    samples=None,
) -> dict[Analysis, PredictionResult]:
    """Classify selected samples and return their results."""
    logger = get_logger()

    samples = list(study.samples.all() if samples is None else samples)
    selected = [(sample, latest_taxonomy_analysis(sample)) for sample in samples]
    rows = [
        trapiche_input(study, sample, analysis)
        for sample, analysis in selected
        if analysis is not None
    ]

    if not rows:
        logger.info("No samples selected for Trapiche to process.")
        return {}

    image = os.getenv("TRAPICHE_SINGULARITY_IMAGE")
    if not image:
        raise RuntimeError("TRAPICHE_SINGULARITY_IMAGE must point to a Trapiche image")

    # TODO: should this be an artifact instead? to allow for better debugability
    with TemporaryDirectory(prefix="trapiche-") as temp_dir:

        logger.info(f"Running Trapiche on selected samples, total {len(rows)}.")

        temp_path = Path(temp_dir)
        input_path = temp_path / "input.ndjson"
        output_path = temp_path / "input_trapiche_results.ndjson"
        input_path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )

        result = Client.execute(
            image,
            ["trapiche", str(input_path)],
            bind=temp_dir,
            return_result=True,
        )

        if result.get("return_code"):
            raise RuntimeError(
                f"Trapiche Singularity command failed: {result.get('message', '')}"
            )

        output = [
            json.loads(line)
            for line in output_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        analyses = [analysis for _, analysis in selected if analysis is not None]

        return {
            analysis: convert_trapiche_response(result)
            for analysis, result in zip(analyses, output)
        }


@flow(flow_run_name="Predict biomes for {study_accession}")
def predict_biomes(
    study_accession: str, sample_accessions: list[str] | None = None
) -> str:
    """Predict biomes for every sample in a study or selected sample accessions.

    Curations are stored as ``suggested`` records for curator review. Each run
    updates the current curation for each analysis with the latest result.
    """
    study = Study.objects_not_suppressed.get_by_accession(study_accession)
    samples = None
    if sample_accessions is not None:
        samples = list(study.samples.filter(ena_accessions__overlap=sample_accessions))

    predictions = classify_study(study, samples=samples)

    prediction = None
    for analysis, result in predictions.items():
        prediction = TrapicheBiomeCuration.objects.record(analysis, result)

    return str(prediction.pk) if prediction else ""
