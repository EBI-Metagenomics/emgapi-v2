import pytest

from curations.models import (
    TrapicheBiomeCuration,
)
from workflows.curations.predict_biomes import PredictionResult, predict_biomes


@pytest.mark.django_db
def test_record_creates_new_suggested_curation(raw_reads_mgnify_study, user):
    prediction = TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:old",
        status=TrapicheBiomeCuration.Status.APPROVED,
        curator=user,
        note="Reviewed result",
    )

    recorded = TrapicheBiomeCuration.objects.record(
        raw_reads_mgnify_study,
        PredictionResult(lineage="root:new", confidence=0.8),
    )

    assert recorded != prediction
    assert recorded.raw_lineage == "root:new"
    assert recorded.status == TrapicheBiomeCuration.Status.SUGGESTED
    assert recorded.provider == "trapiche"
    assert prediction.status == TrapicheBiomeCuration.Status.APPROVED
    assert prediction.curator == user


@pytest.mark.django_db
def test_effective_curation_prefers_approved_and_excludes_rejected(
    raw_reads_mgnify_study,
):
    rejected = TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:rejected",
        status=TrapicheBiomeCuration.Status.REJECTED,
    )
    suggested = TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:suggested",
    )
    approved = TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:approved",
        status=TrapicheBiomeCuration.Status.APPROVED,
    )

    assert (
        TrapicheBiomeCuration.objects.effective_for_study(raw_reads_mgnify_study)
        == approved
    )
    assert rejected != approved
    assert suggested != approved


@pytest.mark.django_db
def test_effective_sample_curation_falls_back_to_study(
    raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    curation = TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:study",
    )

    assert (
        TrapicheBiomeCuration.objects.effective_for_sample(raw_reads_mgnify_sample[0])
        == curation
    )


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
        "workflows.curations.predict_biomes.classify_study", classify_study
    )

    prediction_id = predict_biomes.fn(raw_reads_mgnify_study.first_accession)

    prediction = TrapicheBiomeCuration.objects.get(pk=prediction_id)
    assert prediction.raw_lineage == "root:prediction:2"
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
    assert TrapicheBiomeCuration.objects.filter(sample__isnull=False).count() == len(
        raw_reads_mgnify_sample
    )
