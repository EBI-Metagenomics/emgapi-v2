import pytest

from curations.models import TrapicheBiomeCuration


@pytest.mark.django_db
def test_suppressed_analysis_prediction_is_not_exposed(
    ninja_api_client, amplicon_analysis_with_downloads
):
    analysis = amplicon_analysis_with_downloads
    TrapicheBiomeCuration.objects.create(analysis=analysis, raw_lineage="root:test")
    analysis.is_suppressed = True
    analysis.save()

    response = ninja_api_client.get(f"/curations/analyses/{analysis.accession}/biome")

    assert response.status_code == 404


@pytest.mark.django_db
def test_prediction_list_matches_public_visibility(
    ninja_api_client, amplicon_analysis_with_downloads, private_analysis_with_download
):
    TrapicheBiomeCuration.objects.create(
        analysis=amplicon_analysis_with_downloads, raw_lineage="root:public"
    )
    TrapicheBiomeCuration.objects.create(
        analysis=private_analysis_with_download, raw_lineage="root:private"
    )

    response = ninja_api_client.get("/curations/biomes")

    assert response.status_code == 200
    assert [item["analysis_accession"] for item in response.json()["items"]] == [
        amplicon_analysis_with_downloads.accession
    ]


@pytest.mark.django_db
def test_analysis_biome_returns_effective_curation(
    ninja_api_client, amplicon_analysis_with_downloads
):
    analysis = amplicon_analysis_with_downloads
    TrapicheBiomeCuration.objects.create(
        analysis=analysis, raw_lineage="root:old", status="rejected"
    )
    TrapicheBiomeCuration.objects.create(
        analysis=analysis, raw_lineage="root:Environmental", status="approved"
    )

    response = ninja_api_client.get(f"/curations/analyses/{analysis.accession}/biome")

    assert response.status_code == 200
    assert response.json()["analysis_accession"] == analysis.accession
    assert response.json()["study_accession"] == analysis.study.accession
    assert response.json()["sample_accession"] == analysis.sample.first_accession
