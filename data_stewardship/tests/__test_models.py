import pytest

from data_stewardship.models import (
    BiomeSampleBiomePrediction,
    BiomeStudyBiomePrediction,
)
from data_stewardship.workflows.predict_biomes import PredictionResult, predict_biomes


@pytest.mark.django_db
def test_replacing_prediction_resets_review_state(raw_reads_mgnify_study, user):
    prediction = BiomeStudyBiomePrediction.objects.create(
        study=raw_reads_mgnify_study,
        raw_predicted_lineage="root:old",
        status=BiomeStudyBiomePrediction.Status.APPROVED,
        curator=user,
        note="Reviewed result",
    )

    replaced = BiomeStudyBiomePrediction.objects.replace(
        raw_reads_mgnify_study,
        PredictionResult(lineage="root:new", confidence=0.8),
    )

    assert replaced == prediction
    assert replaced.raw_predicted_lineage == "root:new"
    assert replaced.status == BiomeStudyBiomePrediction.Status.SUGGESTED
    assert replaced.curator is None
    assert replaced.note == ""


@pytest.mark.django_db
def test_study_workflow_persists_sample_and_study_predictions(
    raw_reads_mgnify_study, raw_reads_mgnify_sample, monkeypatch
):
    def classify_study(study, samples=None):
        samples = list(samples or study.samples.all())
        return (
            {
                sample: (
                    PredictionResult(
                        lineage=f"root:prediction:{index}", confidence=index
                    ),
                    None,
                )
                for index, sample in enumerate(samples)
            },
            {
                study.first_accession: {
                    "confident": {"root:prediction:2": [samples[2].first_accession]},
                    "low_confidence": {
                        "root:prediction:0": [samples[0].first_accession]
                    },
                }
            },
        )

    monkeypatch.setattr(
        "data_stewardship.workflows.predict_biomes.classify_study", classify_study
    )

    prediction_id = predict_biomes.fn(raw_reads_mgnify_study.first_accession)

    prediction = BiomeStudyBiomePrediction.objects.get(pk=prediction_id)
    assert prediction.raw_predicted_lineage == "root:prediction:2"
    assert prediction.configuration == {
        "study_summary": {
            raw_reads_mgnify_study.first_accession: {
                "confident": {
                    "root:prediction:2": [raw_reads_mgnify_sample[2].first_accession]
                },
                "low_confidence": {
                    "root:prediction:0": [raw_reads_mgnify_sample[0].first_accession]
                },
            }
        }
    }
    assert BiomeSampleBiomePrediction.objects.count() == len(raw_reads_mgnify_sample)
