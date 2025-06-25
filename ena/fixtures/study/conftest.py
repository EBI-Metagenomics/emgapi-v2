import django
import pytest

django.setup()

import ena.models as ena_models


@pytest.fixture
def raw_read_ena_study():
    raw_reads_study = {"primary_accession": "PRJNA398089", "title": "Project 1"}
    return ena_models.Study.objects.get_or_create(
        accession=raw_reads_study["primary_accession"], title=raw_reads_study["title"]
    )[0]


@pytest.fixture
def assembly_ena_study():
    raw_reads_study = {"primary_accession": "PRJEB25958", "title": "Project 2"}
    return ena_models.Study.objects.get_or_create(
        accession=raw_reads_study["primary_accession"], title=raw_reads_study["title"]
    )[0]


@pytest.fixture
def private_webin():
    return "Webin-12345"


@pytest.fixture
def webin_private_ena_study(private_webin):
    private_study = {"primary_accession": "PRJNA999999", "title": "Project 9"}
    return ena_models.Study.objects.get_or_create(
        accession=private_study["primary_accession"],
        title=private_study["title"],
        is_private=True,
        webin_submitter=private_webin,
    )[0]
