import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def publication(raw_reads_mgnify_study):
    publication = mg_models.Publication.objects.create(
        pubmed_id=30975860,
        title="The Origin of Species",
        published_year=1859,
        metadata={
            "authors": "Charles Darwin",
            "doi": "10.1017/CBO9780511694295",
            "isbn": "9780511694295",
            "iso_journal": "Cambridge University Press",
            "pubmed_central_id": 1,
            "volume": "1",
            "pub_type": "Book",
        },
    )

    # Associate the publication with the study
    mg_models.StudyPublication.objects.create(
        study=raw_reads_mgnify_study,
        publication=publication,
    )

    return publication
