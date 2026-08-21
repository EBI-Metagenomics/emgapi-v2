import pytest

from data_stewardship.models import (
    BiomeSampleBiomePrediction,
    BiomeStudyBiomePrediction,
)


@pytest.mark.django_db
def test_suppressed_study_prediction_is_not_exposed(
    ninja_api_client, raw_reads_mgnify_study
):
    BiomeStudyBiomePrediction.objects.create(
        study=raw_reads_mgnify_study, raw_predicted_lineage="root:Environmental"
    )
    raw_reads_mgnify_study.is_suppressed = True
    raw_reads_mgnify_study.save()

    response = ninja_api_client.get(
        f"/biome-predictions/studies/{raw_reads_mgnify_study.accession}"
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_suppressed_sample_prediction_is_not_exposed(
    ninja_api_client, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    BiomeSampleBiomePrediction.objects.create(
        sample=sample, raw_predicted_lineage="root:Environmental"
    )
    sample.is_suppressed = True
    sample.save()

    response = ninja_api_client.get(
        f"/biome-predictions/samples/{sample.first_accession}"
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_prediction_list_matches_public_visibility(
    ninja_api_client, raw_reads_mgnify_study, webin_private_study
):
    BiomeStudyBiomePrediction.objects.create(
        study=raw_reads_mgnify_study, raw_predicted_lineage="root:public"
    )
    BiomeStudyBiomePrediction.objects.create(
        study=webin_private_study, raw_predicted_lineage="root:private"
    )

    response = ninja_api_client.get("/biome-predictions/")

    assert response.status_code == 200
    assert [item["study_accession"] for item in response.json()["items"]] == [
        raw_reads_mgnify_study.accession
    ]
