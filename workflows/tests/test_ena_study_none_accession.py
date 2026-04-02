import pytest
import logging
import analyses.models
import ena.models


@pytest.mark.django_db(transaction=True)
def test_ena_study_none_accession(prefect_harness, httpx_mock):
    logger = logging.getLogger("test_ena_study_none_accession")

    study_accession = "SRP000183"

    ena_study, created = ena.models.Study.objects.get_or_create(
        accession=study_accession,
        defaults={
            "title": "None accession study",
            "additional_accessions": [None],
        },
    )
    ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")
    logger.info(mgnify_study.ena_accessions)
    assert None not in mgnify_study.ena_accessions
