import pytest

from analyses.models import Analysis
from curations.models import TrapicheBiomeCuration
from workflows.curations.predict_biomes import PredictionResult, predict_biomes


@pytest.mark.django_db
def test_record_creates_new_suggested_curation(amplicon_analysis_with_downloads):
    analysis = amplicon_analysis_with_downloads
    recorded = TrapicheBiomeCuration.objects.record(
        analysis, PredictionResult(lineage="root:new", confidence=0.8)
    )
    assert recorded.raw_lineage == "root:new"
    assert recorded.status == "suggested"
    assert recorded.provider == "trapiche"


@pytest.mark.django_db
def test_record_replaces_the_existing_analysis_curation(
    amplicon_analysis_with_downloads,
):
    analysis = amplicon_analysis_with_downloads
    existing = TrapicheBiomeCuration.objects.create(
        analysis=analysis, raw_lineage="root:old", status="approved"
    )
    recorded = TrapicheBiomeCuration.objects.record(
        analysis, PredictionResult(lineage="root:new", confidence=0.8)
    )
    assert recorded.pk == existing.pk
    assert recorded.raw_lineage == "root:new"
    assert recorded.status == "suggested"


@pytest.mark.django_db
def test_workflow_persists_analysis_predictions(
    raw_reads_mgnify_study, raw_reads_mgnify_sample, monkeypatch
):
    analyses = [
        Analysis.objects.create(
            study=raw_reads_mgnify_study,
            sample=sample,
            annotations={Analysis.TAXONOMIES: [{"taxonomy": "Bacteria"}]},
        )
        for sample in raw_reads_mgnify_sample
    ]

    def classify_study(study, samples=None):
        return {
            analysis: PredictionResult(
                lineage=f"root:prediction:{index}", confidence=index
            )
            for index, analysis in enumerate(analyses)
        }

    monkeypatch.setattr(
        "workflows.curations.predict_biomes.classify_study", classify_study
    )
    prediction_id = predict_biomes.fn(raw_reads_mgnify_study.first_accession)
    prediction = TrapicheBiomeCuration.objects.get(pk=prediction_id)
    assert prediction.raw_lineage == "root:prediction:2"
    assert TrapicheBiomeCuration.objects.count() == len(analyses)
    assert set(TrapicheBiomeCuration.objects.values_list("analysis_id", flat=True)) == {
        analysis.id for analysis in analyses
    }
