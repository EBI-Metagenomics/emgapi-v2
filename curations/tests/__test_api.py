import pytest

from curations.models import (
    TrapicheBiomeCuration,
)


@pytest.mark.django_db
def test_suppressed_study_prediction_is_not_exposed(
    ninja_api_client, raw_reads_mgnify_study
):
    TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study, raw_lineage="root:Environmental"
    )
    raw_reads_mgnify_study.is_suppressed = True
    raw_reads_mgnify_study.save()

    response = ninja_api_client.get(
        f"/curations/studies/{raw_reads_mgnify_study.accession}/biome"
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_suppressed_sample_prediction_is_not_exposed(
    ninja_api_client, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    TrapicheBiomeCuration.objects.create(
        study=sample.studies.first(), sample=sample, raw_lineage="root:Environmental"
    )
    sample.is_suppressed = True
    sample.save()

    response = ninja_api_client.get(
        f"/curations/samples/{sample.first_accession}/biome"
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_prediction_list_matches_public_visibility(
    ninja_api_client, raw_reads_mgnify_study, webin_private_study
):
    TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study, raw_lineage="root:public"
    )
    TrapicheBiomeCuration.objects.create(
        study=webin_private_study, raw_lineage="root:private"
    )

    response = ninja_api_client.get("/curations/biomes")

    assert response.status_code == 200
    assert [item["study_accession"] for item in response.json()["items"]] == [
        raw_reads_mgnify_study.accession
    ]


@pytest.mark.django_db
def test_sample_biome_returns_study_fallback(
    ninja_api_client, raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    TrapicheBiomeCuration.objects.create(
        study=raw_reads_mgnify_study,
        raw_lineage="root:Environmental",
    )

    response = ninja_api_client.get(
        f"/curations/samples/{sample.first_accession}/biome"
    )

    assert response.status_code == 200
    assert response.json()["sample_accession"] == sample.first_accession
    assert response.json()["study_accession"] == raw_reads_mgnify_study.accession
