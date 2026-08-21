from typing import Any

from django.db.models import QuerySet
from pydantic import BaseModel
from trapiche.api import TrapicheWorkflowFromSequence
from trapiche.config import TrapicheWorkflowParams

from activate_django_first import EMG_CONFIG  # noqa: F401

from analyses.base_models.with_downloads_models import DownloadType
from analyses.models import Analysis, Biome, Sample, Study
from analyses.schemas import MGnifyAnalysisDownloadFile
from data_stewardship.models import (
    BiomeSampleBiomePrediction,
    BiomeStudyBiomePrediction,
)
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


def normalize_lineage(lineage: str | None) -> str:
    """Remove empty lineage components and surrounding whitespace."""
    return ":".join(part.strip() for part in (lineage or "").split(":") if part.strip())


def map_lineage(lineage: str | None, biomes: QuerySet | None = None) -> Biome | None:
    """Map a classifier lineage to the corresponding stored biome."""
    normalized = normalize_lineage(lineage)
    if not normalized:
        return None
    queryset = biomes if biomes is not None else Biome.objects.all()
    return queryset.filter(path=Biome.lineage_to_path(normalized)).first()


def latest_taxonomy_analysis(sample: Sample) -> Analysis | None:
    """Return the newest ready analysis containing taxonomy annotations."""
    analyses = sample.analyses.filter(is_ready=True).order_by("-updated_at", "-id")
    for analysis in analyses:
        if (analysis.annotations or {}).get(Analysis.TAXONOMIES):
            return analysis
    return None


def _description(study: Study, sample: Sample) -> tuple[str, str]:
    """Build the study and sample text fields sent to Trapiche."""
    study_text = " ".join(
        str(value)
        for value in (study.title, (study.metadata or {}).get("description", ""))
        if value
    )
    sample_text = " ".join(
        str(value)
        for value in (
            sample.sample_title,
            (sample.metadata or {}).get("description", ""),
        )
        if value
    )
    return study_text, sample_text


def trapiche_input(
    study: Study, sample: Sample, analysis: Analysis | None
) -> dict[str, Any]:
    """Build one Trapiche input row from a study, sample, and optional analysis."""
    study_text, sample_text = _description(study, sample)
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


def _taxonomy_path(download: MGnifyAnalysisDownloadFile) -> str | None:
    """Resolve a download using MGnify's canonical file URL builder."""
    if download.parent_results_dir is None:
        return None
    return MGnifyAnalysisDownloadFile._build_file_url(
        download.path,
        download.parent_results_dir,
        download.parent_is_private,
    )


def convert_taxonomy_download(download: MGnifyAnalysisDownloadFile) -> str | None:
    """Convert a MAPseq download into a Trapiche input URL."""
    if download.download_type != DownloadType.TAXONOMIC_ANALYSIS:
        return None
    path = _taxonomy_path(download)
    if path and ".mseq" in str(download.path).lower():
        return path
    return None


def _result(raw: Any) -> PredictionResult:
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
        normalize_lineage(lineage),
        confidence,
        raw.get("method", "trapiche"),
        raw.get("source", "trapiche"),
        "v1",  # TODO: fill this with the trapiche version
        "TODO",  # TODO: to capture how was ran, text-only, tax....
    )


@task(name="Classify a study using Trapiche")
def classify_study(
    study: Study,
    samples=None,
) -> tuple[dict[Sample, tuple[PredictionResult, Analysis | None]], dict[str, Any]]:
    """Classify selected samples and return their results with the study summary."""
    samples = list(study.samples.all() if samples is None else samples)
    selected = {sample: latest_taxonomy_analysis(sample) for sample in samples}
    rows = [trapiche_input(study, sample, selected[sample]) for sample in samples]

    # Defaults
    # TrapicheWorkflowParams(
    #     run_text=True, run_vectorise=True, run_taxonomy=True,
    #     keep_text_results=True, keep_vectorise_results=False, keep_taxonomy_results=True, output_keys=None
    #     When output_k is None, the keep_* flags decide what to include.
    # )

    runner = TrapicheWorkflowFromSequence(workflow_params=TrapicheWorkflowParams())
    output = list(runner.run(rows) or [])
    return (
        {
            sample: (
                _result(output[index]) if index < len(output) else PredictionResult(),
                selected[sample],
            )
            for index, sample in enumerate(samples)
        },
        runner.study_summary or {},
    )


@flow(flow_run_name="Predict biomes for {study_accession}")
def predict_biomes(
    study_accession: str, sample_accessions: list[str] | None = None
) -> str:
    """Predict biomes for every sample in a study or selected sample accessions.

    Predictions are stored as ``suggested`` records for curator review. Existing
    predictions for the selected targets are replaced with the latest classifier
    result, including their evidence and provenance.
    """
    study = Study.objects_not_suppressed.get_by_accession(study_accession)
    samples = None
    if sample_accessions is not None:
        samples = list(study.samples.filter(ena_accessions__overlap=sample_accessions))

    predictions, study_summary = classify_study(study, samples=samples)

    for sample, (result, analysis) in predictions.items():
        BiomeSampleBiomePrediction.objects.replace(
            sample, result, [analysis] if analysis else []
        )

    if sample_accessions is None:
        lineages = [result for result, _ in predictions.values() if result.lineage]
        study_result = max(
            lineages,
            key=lambda result: result.confidence or 0,
            default=PredictionResult(),
        )
        study_result.configuration = {
            **(study_result.configuration or {}),
            "study_summary": study_summary,
        }
        evidence = [analysis for _, analysis in predictions.values() if analysis]
        prediction = BiomeStudyBiomePrediction.objects.replace(
            study, study_result, evidence
        )
    else:
        prediction = BiomeStudyBiomePrediction.objects.filter(study=study).first()

    return str(prediction.pk) if prediction else ""
