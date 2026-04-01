import pytest
import logging
import analyses.models
import ena.models

from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)
from workflows.ena_utils.ena_api_requests import get_study_from_ena


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_normal_ena_study(prefect_harness, httpx_mock):
    logger = logging.getLogger("test_normal_ena_study")
    httpx_mock.add_response(
        url="https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=%22%28study_accession%3DSRP386966+OR+secondary_study_accession%3DSRP386966%29%22&fields=study_accession&limit=&format=json&dataPortal=metagenome",
        json=[{"study_accession": "PRJNA855972"}],
    )
    httpx_mock.add_response(
        url="https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=%22%28study_accession%3DSRP386966+OR+secondary_study_accession%3DSRP386966%29%22&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name&limit=10&format=json&dataPortal=metagenome",
        json=[
            {
                "study_accession": "PRJNA855972",
                "study_title": "HOT cruise 319 enriched seawater sequencing from Station ALOHA",
                "study_description": "Seawater from 25m at Station ALOHA in the North Pacific Subtropical Gyre was partitioned into tailed phage enriched and membrane vesicle enriched fractions by filtration and centrifuge.",
                "center_name": "SCOPE",
                "secondary_study_accession": "SRP386966",
                "study_name": "marine plankton metagenome",
            }
        ],
    )

    study_accession = "SRP386966"

    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession, limit=10)
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")
    logger.info(mgnify_study.ena_accessions)
    assert None not in mgnify_study.ena_accessions


@pytest.mark.httpx_mock(
    should_mock=should_not_mock_httpx_requests_to_prefect_server,
    can_send_already_matched_responses=True,
)
@pytest.mark.django_db(transaction=True)
def test_strange_ena_study(prefect_harness, httpx_mock):
    logger = logging.getLogger("test_strange_ena_study")
    httpx_mock.add_response(
        url="https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=%22%28study_accession%3DSRP000183+OR+secondary_study_accession%3DSRP000183%29%22&fields=study_accession&limit=&format=json&dataPortal=metagenome",
        json=[],
    )
    # httpx_mock.add_response(
    #     url="https://www.ebi.ac.uk/ena/portal/api/search?result=study&query=%22%28study_accession%3DSRP000183+OR+secondary_study_accession%3DSRP000183%29%22&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name&limit=10&format=json&dataPortal=metagenome",
    #     json = [],
    # )

    study_accession = "SRP000183"

    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession, limit=10)
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")
    logger.info(mgnify_study.ena_accessions)
    assert None not in mgnify_study.ena_accessions
