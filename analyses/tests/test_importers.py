import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import connection

import ena.models
from analyses.models import Biome, Study
from workflows.data_io_utils.legacy_emg_dbs import LegacyStudy
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_biome_importer(httpx_mock):
    httpx_mock.add_response(
        url="http://old.api/v1/biomes?page=1",
        json={
            "links": {
                "next": "http://old.api/v1/biomes?page=2",
            },
            "data": [{"id": "root", "attributes": {"biome-name": "Root"}}],
        },
    )
    httpx_mock.add_response(
        url="http://old.api/v1/biomes?page=2",
        json={
            "links": {
                "next": None,
            },
            "data": [{"id": "root:Deep", "attributes": {"biome-name": "Deep"}}],
        },
    )
    call_command("import_biomes_from_api_v1", "-u", "http://old.api/v1/biomes")
    assert Biome.objects.count() == 2
    assert Biome.objects.filter(path="root.deep").exists()
    assert Biome.objects.get(path="root.deep").biome_name == "Deep"
    assert Biome.objects.get(path="root.deep").pretty_lineage == "root:Deep"


@pytest.fixture
def biome_for_legacy():
    Biome.objects.create(
        path=Biome.lineage_to_path("root:Environmental:Planetary:Martian soil"),
        biome_name="Martian soil",
    )


@pytest.mark.django_db(transaction=True)
def test_import_legacy_studies(
    mock_legacy_emg_db_session, ninja_api_client, caplog, biome_for_legacy
):
    # assume DB has been set up so that NEW studies are being created with accessions higher than some start point:
    with connection.cursor() as cursor:
        cursor.execute("SELECT setval('analyses_study_id_seq', 7000, false);")
        next_id = cursor.execute("SELECT nextval('analyses_study_id_seq');")
        assert next_id.fetchone() == (7000,)
    # this is so that we are properly testing insertions at accessions below the current nextval

    # fixture has one public study MGYS 5000:
    # with no args, should do a dry run from MGYS 0
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db", "--dry_run")

    assert "Would have Imported 1 studies" in caplog.text
    assert "5000" in caplog.text
    assert Study.objects.count() == 0

    caplog.clear()

    # after 5001 should be no studies
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db", "-a", "5001", "--dry_run")

    assert "Would have Imported 0 studies" in caplog.text

    caplog.clear()

    # dry run off should import a study
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db")
        assert "Imported 1 studies" in caplog.text

    assert Study.objects.count() == 1
    assert Study.objects.first().accession == "MGYS00005000"

    caplog.clear()

    # should raise exception if try to reimport same one
    with pytest.raises(ValidationError) as e:
        call_command("import_studies_from_legacy_db")

    assert "Study 5000 already exists" in e.value.message

    # new study should be importable
    study = LegacyStudy(
        id=5002,
        centre_name="VENUS",
        study_name="Bugs on venus",
        ext_study_id="ERP3",
        is_private=False,
        project_id="PRJ3",
        is_suppressed=False,
        biome_id=1,
    )
    with mock_legacy_emg_db_session() as session:
        session.add(study)

    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db", "-a", 5000)
        assert "Imported 1 studies" in caplog.text

    assert Study.objects.count() == 2
    assert Study.objects.order_by("-created_at").first().accession == "MGYS00005002"

    # should both be flagged as legacy
    assert Study.objects.filter(features__has_prev6_analyses=True).count() == 2

    # get_or_create on a new MGnify study, for the ENA study imported as legacy, should return legacy MGYS not new one
    ena_study = ena.models.Study.objects.get_ena_study("ERP3")  # should be MGYS00005002

    mg_study: Study = Study.objects.get_or_create_for_ena_study(ena_study)
    assert mg_study.accession == "MGYS00005002"
