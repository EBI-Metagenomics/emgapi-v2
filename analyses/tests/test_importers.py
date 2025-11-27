import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import connection

import ena.models
from analyses.models import Biome, Study, SuperStudy, SuperStudyStudy, Publication
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyStudy,
    LegacySuperStudy,
    LegacySuperStudyStudy,
    LegacyPublication,
    LegacyStudyPublication,
)
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
    combine_caplog_records,
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
        caplog_text = combine_caplog_records(caplog.records)

    assert "Would have Imported 1 studies" in caplog_text
    assert "5000" in caplog_text
    assert Study.objects.count() == 0

    caplog.clear()

    # after 5001 should be no studies
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db", "-a", "5001", "--dry_run")
        caplog_text = combine_caplog_records(caplog.records)

    assert "Would have Imported 0 studies" in caplog_text

    caplog.clear()

    # dry run off should import a study
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported 1 studies" in caplog_text

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
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported 1 studies" in caplog_text

    assert Study.objects.count() == 2
    assert Study.objects.order_by("-created_at").first().accession == "MGYS00005002"

    # should both be flagged as legacy
    assert Study.objects.filter(features__has_prev6_analyses=True).count() == 2

    # get_or_create on a new MGnify study, for the ENA study imported as legacy, should return legacy MGYS not new one
    ena_study = ena.models.Study.objects.get_ena_study("ERP3")  # should be MGYS00005002

    mg_study: Study = Study.objects.get_or_create_for_ena_study(ena_study)
    assert mg_study.accession == "MGYS00005002"


@pytest.mark.django_db(transaction=True)
def test_import_legacy_super_studies(
    mock_legacy_emg_db_session, ninja_api_client, caplog, biome_for_legacy
):
    # First, we need to import a study to associate with the super study
    study = LegacyStudy(
        id=5003,
        centre_name="MARS",
        study_name="Bugs on mars",
        ext_study_id="ERP4",
        is_private=False,
        project_id="PRJ4",
        is_suppressed=False,
        biome_id=1,
    )
    with mock_legacy_emg_db_session() as session:
        session.add(study)

    # Import the study
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported" in caplog_text
        assert "studies" in caplog_text

    assert Study.objects.filter(id=5003).exists()
    caplog.clear()

    # Create a super study in the legacy DB
    super_study = LegacySuperStudy(
        study_id=1,
        title="Test Super Study",
        description="This is a test super study",
        url_slug="test-super-study",
        logo=None,
    )

    # Create a super study study association
    super_study_study = LegacySuperStudyStudy(
        id=1,
        study_id=5003,
        super_study_id=1,
        is_flagship=True,
    )

    with mock_legacy_emg_db_session() as session:
        session.add(super_study)
        session.add(super_study_study)

    # Test dry run
    with caplog.at_level("INFO"):
        call_command("import_super_studies_from_legacy_db", "--dry_run")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Would have Imported 1 super studies" in caplog_text
        assert "Would make super study for ID 1 / Test Super Study" in caplog_text
        assert "Would associate study" in caplog_text

    # Verify nothing was imported
    assert SuperStudy.objects.count() == 0
    caplog.clear()

    # Test actual import
    with caplog.at_level("INFO"):
        call_command("import_super_studies_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported 1 super studies" in caplog_text
        assert "Created new super study object" in caplog_text
        assert "Associated study" in caplog_text

    # Verify super study was imported
    assert SuperStudy.objects.count() == 1
    super_study = SuperStudy.objects.first()
    assert super_study.slug == "test-super-study"
    assert super_study.title == "Test Super Study"
    assert super_study.description == "This is a test super study"

    # Verify study association
    assert SuperStudyStudy.objects.count() == 1
    super_study_study = SuperStudyStudy.objects.first()
    assert super_study_study.super_study == super_study
    assert super_study_study.study.id == 5003
    assert super_study_study.is_flagship is True

    caplog.clear()

    # Test that trying to import the same super study again raises an error
    with pytest.raises(ValidationError) as e:
        call_command("import_super_studies_from_legacy_db")

    assert "Super Study with slug test-super-study already exists" in e.value.message


@pytest.mark.django_db(transaction=True)
def test_import_legacy_publications(
    mock_legacy_emg_db_session, ninja_api_client, caplog, biome_for_legacy
):
    # First, we need to import a study to associate with the publications
    study = LegacyStudy(
        id=5004,
        centre_name="JUPITER",
        study_name="Bugs on Jupiter",
        ext_study_id="ERP5",
        is_private=False,
        project_id="PRJ5",
        is_suppressed=False,
        biome_id=1,
    )
    with mock_legacy_emg_db_session() as session:
        session.add(study)

    # Import the study
    with caplog.at_level("INFO"):
        call_command("import_studies_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported" in caplog_text
        assert "studies" in caplog_text

    assert Study.objects.filter(id=5004).exists()
    caplog.clear()

    # Create a publication in the legacy DB
    publication = LegacyPublication(
        pub_id=1,
        authors="Test Author, Another Author",
        doi="10.1234/test.12345",
        isbn="978-1-234-56789-0",
        iso_journal="Test J. Metagenomics",
        medline_journal="Test Journal of Metagenomics",
        pubmed_id=12345678,
        pub_title="Test Publication for MGnify",
        raw_pages="123-456",
        volume="10",
        published_year=2023,
        pub_type="Journal Article",
    )

    # Create a publication-study association
    pub_study = LegacyStudyPublication(
        id=1,
        study_id=5004,
        pub_id=1,
    )

    with mock_legacy_emg_db_session() as session:
        session.add(publication)
        session.add(pub_study)

    # Test dry run
    with caplog.at_level("INFO"):
        call_command("import_publications_from_legacy_db", "--dry_run")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Would have Imported 1 publications" in caplog_text
        assert (
            "Would make publication for ID 1 / Test Publication for MGnify"
            in caplog_text
        )
        assert "Would associate study" in caplog_text

    # Verify nothing was imported
    assert Publication.objects.count() == 0
    caplog.clear()

    # Test actual import
    with caplog.at_level("INFO"):
        call_command("import_publications_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert "Imported 1 publications" in caplog_text
        assert "Created new publication object" in caplog_text
        assert "Associated study" in caplog_text

    # Verify publication was imported
    assert Publication.objects.count() == 1
    pub = Publication.objects.first()
    assert pub.pubmed_id == 12345678
    assert pub.title == "Test Publication for MGnify"
    assert pub.published_year == 2023
    assert pub.metadata.authors == "Test Author, Another Author"
    assert pub.metadata.doi == "10.1234/test.12345"
    assert pub.metadata.isbn == "978-1-234-56789-0"

    # Verify study association
    assert pub.studies.count() == 1
    study = pub.studies.first()
    assert study.id == 5004

    caplog.clear()

    # Test that trying to import the same publication again skips it
    with caplog.at_level("INFO"):
        call_command("import_publications_from_legacy_db")
        caplog_text = combine_caplog_records(caplog.records)
        assert (
            "Publication with PubMed ID 12345678 already exists, skipping"
            in caplog_text
        )
