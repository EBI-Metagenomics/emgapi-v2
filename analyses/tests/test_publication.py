import pytest

from analyses.models import Publication, StudyPublication


@pytest.mark.django_db(transaction=True, reset_sequences=True)
def test_publication():
    # Test creating a publication
    publication = Publication.objects.create(
        pubmed_id=12345678,
        title="Test Publication",
        published_year=2023,
        metadata=Publication.PublicationMetadata(
            pub_type="journal article",
            iso_journal="Test Journal",
            authors="Test Author",
            doi="10.1234/test",
        ),
    )
    publication.refresh_from_db()
    m: Publication.PublicationMetadata = publication.metadata
    assert publication.pubmed_id == 12345678
    assert publication.title == "Test Publication"
    assert publication.published_year == 2023
    assert m.authors == "Test Author"
    assert m.doi == "10.1234/test"
    assert m.iso_journal == "Test Journal"


@pytest.mark.django_db(transaction=True)
def test_publication_study_relationship(raw_reads_mgnify_study):
    # Test creating a publication and associating it with a study
    publication = Publication.objects.create(
        pubmed_id=12345678,
        title="Test Publication",
        published_year=2023,
        metadata=Publication.PublicationMetadata(
            doi="10.1234/test",
        ),
    )

    # Associate the publication with the study
    StudyPublication.objects.create(
        study=raw_reads_mgnify_study,
        publication=publication,
    )

    # Check that the relationship exists
    assert publication.studies.count() == 1
    assert publication.studies.first() == raw_reads_mgnify_study
    assert raw_reads_mgnify_study.publications.count() == 1
    assert raw_reads_mgnify_study.publications.first() == publication


@pytest.mark.django_db(transaction=True)
def test_publication_api(ninja_api_client, raw_reads_mgnify_study):
    # Create a publication and associate it with a study
    publication = Publication.objects.create(
        pubmed_id=12345678,
        title="Test Publication",
        published_year=2023,
        metadata=Publication.PublicationMetadata(
            doi="10.1234/test",
        ),
    )

    StudyPublication.objects.create(
        study=raw_reads_mgnify_study,
        publication=publication,
    )

    # Test the publication detail endpoint
    response = ninja_api_client.get(f"/publications/{publication.pk}")
    assert response.status_code == 200
    assert response.json()["pubmed_id"] == publication.pubmed_id
    assert response.json()["title"] == publication.title
    assert response.json()["published_year"] == publication.published_year
    assert len(response.json()["studies"]) == 1
    assert (
        response.json()["studies"][0]["accession"] == raw_reads_mgnify_study.accession
    )

    # Test the publications list endpoint
    response = ninja_api_client.get("/publications/")
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["items"][0]["pubmed_id"] == publication.pk

    # Test the study publications endpoint
    response = ninja_api_client.get(
        f"/studies/{raw_reads_mgnify_study.accession}/publications/"
    )
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["items"][0]["pubmed_id"] == publication.pk
