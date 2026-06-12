import pytest


@pytest.mark.django_db
def test_public_studies_feed(webin_private_study, raw_reads_mgnify_study, client):
    response = client.get("/rss/studies/")
    assert response.status_code == 200
    content = response.content.decode("utf-8")
    assert content.startswith("<?xml")
    assert raw_reads_mgnify_study.accession in content
    assert webin_private_study.accession not in content
