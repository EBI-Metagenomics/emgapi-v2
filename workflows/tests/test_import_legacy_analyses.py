import re

import pymongo
import pytest
from sqlalchemy import select

from analyses.models import Analysis, Study, Sample
import ena.models
from workflows.flows.legacy.tasks.make_sample_from_legacy_emg_db import (
    make_sample_from_legacy_emg_db,
)
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyStudy,
    LegacySample,
    legacy_emg_db_session,
)
from workflows.flows.legacy.flows.import_legacy_analyses import (
    import_legacy_analyses,
    import_all_legacy_analyses,
)
from workflows.prefect_utils.testing_utils import (
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)


def test_legacy_db(in_memory_legacy_emg_db):
    with in_memory_legacy_emg_db as session:
        # check fixture is working since it is a bit complicated in itself, as well as the legacy db tables
        study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == 5000)
        legacy_study: LegacyStudy = session.scalar(study_select_stmt)
        assert legacy_study is not None
        assert legacy_study.centre_name == "MARS"


def test_legacy_db_mocker(mock_legacy_emg_db_session):
    print("the session is ", legacy_emg_db_session)  # Should show the mock

    with legacy_emg_db_session() as session:
        assert str(session.bind.url) == "sqlite:///:memory:"
        # check fixture is working since it is a bit complicated in itself, as well as the legacy db tables
        study_select_stmt = select(LegacyStudy).where(LegacyStudy.id == 5000)
        legacy_study: LegacyStudy = session.scalar(study_select_stmt)
        assert legacy_study is not None
        assert legacy_study.centre_name == "MARS"


def test_mongo_taxonomy_mock(mock_mongo_client_for_taxonomy):
    # check the mongo mock fixture since it is a bit complicated in itself
    mongo_client = pymongo.MongoClient("mongodb://anything")
    db = mongo_client["any_db"]
    taxonomies_collection: pymongo.collection.Collection = db.analysis_job_taxonomy
    mgya_taxonomies = taxonomies_collection.find_one({"accession": "anything"})
    assert mgya_taxonomies is not None
    print(mgya_taxonomies["accession"])
    assert mgya_taxonomies["accession"] == "MGYA00012345"


def test_mongo_functional_mock(mock_mongo_client_for_taxonomy_and_protein_functions):
    # check the mongo mock fixture since it is a bit complicated in itself
    mongo_client = pymongo.MongoClient("mongodb://anything")
    db = mongo_client["any_db"]
    pfams_collection: pymongo.collection.Collection = db.analysis_job_pfam
    mgya_pfams = pfams_collection.find_one({"accession": "anything"})
    assert mgya_pfams is not None
    assert mgya_pfams["accession"] == "MGYA00012345"
    assert mgya_pfams["pfam_entries"][0]["count"] == 50

    interpros_collection: pymongo.collection.Collection = (
        db.analysis_job_interpro_identifiers
    )
    mgya_interpros = interpros_collection.find_one({"accession": "anything"})
    assert mgya_interpros is not None
    assert mgya_interpros["accession"] == "MGYA00012345"
    assert mgya_interpros["interpro_identifiers"][0]["count"] == 70


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_prefect_import_v5_analyses_flow(
    prefect_harness,
    mock_legacy_emg_db_session,
    mock_mongo_client_for_taxonomy_and_protein_functions,
    ninja_api_client,
    ena_any_sample_metadata,
    httpx_mock,
):
    httpx_mock.add_response(
        url=re.compile(
            r".*result=study.*ERP1.*"  # any query for ERP1, like checking public availability
        ),
        json=[{"study_accession": "ERP1"}],
        is_reusable=True,
        is_optional=False,
    )
    importer_flow_run = run_flow_and_capture_logs(
        import_legacy_analyses,
        mgys="MGYS00005000",
    )

    assert "Created analysis MGYA00012345 (V5 AMPLI)" in importer_flow_run.logs

    imported_analysis: Analysis = Analysis.objects.filter(
        accession="MGYA00012345"
    ).first()
    assert imported_analysis is not None
    assert imported_analysis.study.ena_study.accession == "ERP1"
    assert imported_analysis.study.first_accession == "ERP1"
    assert imported_analysis.study.accession == "MGYS00005000"
    assert imported_analysis.sample.first_accession == "SAMEA1"
    assert "SAMEA1" in imported_analysis.sample.ena_accessions
    assert "ERS1" in imported_analysis.sample.ena_accessions
    assert imported_analysis.study.biome.biome_name == "Martian soil"
    assert ["ERR1000"] == imported_analysis.run.ena_accessions

    imported_analysis_with_annos: Analysis = Analysis.objects_and_annotations.get(
        accession="MGYA00012345"
    )
    assert imported_analysis_with_annos is not None
    tax_annos = imported_analysis_with_annos.annotations.get(Analysis.TAXONOMIES)
    assert tax_annos == {
        "ssu": [
            {"count": 30, "organism": "Archaea:Euks::Something|5.0"},
            {"count": 40, "organism": "Bacteria|5.0"},
        ],
        "lsu": [
            {"count": 10, "organism": "Archaea:Euks::Something|5.0"},
            {"count": 20, "organism": "Bacteria|5.0"},
        ],
        "its_one_db": None,
        "unite": None,
        "pr2": None,
    }

    # check download files imported okay
    assert len(imported_analysis.downloads) == 1
    dl = imported_analysis.downloads[0]
    assert dl.get("path") == "tax/ssu_tax/BUGS_SSU.fasta.mseq_hdf5.biom"
    assert dl.get("alias") == "BUGS_SSU_OTU_TABLE_HDF5.biom"
    assert (
        dl.get("short_description")
        == "OTUs, counts and taxonomic assignments for SSU rRNA"
    )
    assert dl.get("download_type") == "Taxonomic analysis"

    # parsed schema object should work too
    assert (
        imported_analysis.downloads_as_objects[0].path
        == "tax/ssu_tax/BUGS_SSU.fasta.mseq_hdf5.biom"
    )

    response = ninja_api_client.get("/analyses/MGYA00012345")
    assert response.status_code == 200

    json_response = response.json()
    assert json_response["study_accession"] == "MGYS00005000"
    # assert (
    #     json_response["downloads"][0]["url"]
    #     == "https://www.ebi.ac.uk/metagenomics/api/v1/analyses/MGYA00012345/file/BUGS_SSU_OTU_TABLE_HDF5.biom"
    # )
    # TODO: the URLs now point to transfer services area only. Need to consider what we do for legacy data, later.

    response = ninja_api_client.get(
        "/analyses/MGYA00012345/annotations/taxonomies__ssu"
    )
    assert response.status_code == 200

    json_response = response.json()
    assert json_response["items"] == [
        {"count": 30, "organism": "Archaea:Euks::Something|5.0", "description": None},
        {"count": 40, "organism": "Bacteria|5.0", "description": None},
    ]

    response = ninja_api_client.get(
        "/analyses/MGYA00012345/annotations/taxonomies__lsu"
    )
    assert response.status_code == 200

    json_response = response.json()
    assert json_response["items"] == [
        {"count": 10, "organism": "Archaea:Euks::Something|5.0", "description": None},
        {"count": 20, "organism": "Bacteria|5.0", "description": None},
    ]


@pytest.mark.django_db
def test_make_sample_from_legacy_emg_db_multiple_studies(prefect_harness):
    # Setup: Existing Study and Sample in Django DB
    ena_study_1 = ena.models.Study.objects.create(accession="ERP1", title="Study 1")
    mg_study_1 = Study.objects.create(ena_study=ena_study_1)

    ena_sample = ena.models.Sample.objects.create(
        accession="SAMEA1",
        additional_accessions=["ERS1"],
        study=ena_study_1,
    )
    mg_sample = Sample.objects.create(
        ena_sample=ena_sample,
        ena_study=ena_study_1,
        ena_accessions=["SAMEA1", "ERS1"],
    )
    mg_sample.studies.add(mg_study_1)

    # Setup: A NEW Study in Django DB
    ena_study_2 = ena.models.Study.objects.create(
        accession="ERP2", title="Study 2, e.g. a TPA of Study 1"
    )
    mg_study_2 = Study.objects.create(ena_study=ena_study_2)

    # Setup: A LegacySample that matches the existing Sample
    legacy_sample = LegacySample(
        sample_id=1000,
        ext_sample_id="ERS1",
        primary_accession="SAMEA1",
    )

    # Pretend we were importing Study 2, and it included (e.g. via an Assembly) ERS1
    returned_sample = make_sample_from_legacy_emg_db(legacy_sample, mg_study_2)

    # ERS1 should now ALSO be linked to Study 2, since it was in the legacy db we imported
    assert returned_sample.id == mg_sample.id
    assert returned_sample.studies.count() == 2
    assert mg_study_1 in returned_sample.studies.all()
    assert mg_study_2 in returned_sample.studies.all()

    # Also check ena_sample has NOT changed its study (ENA samples are 1:1 with ENA studies in our simplified data model)
    assert returned_sample.ena_sample.study == ena_study_1


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_prefect_import_all_legacy_analyses_flow(
    prefect_harness,
    mock_legacy_emg_db_session,
    mock_mongo_client_for_taxonomy,
    ena_any_sample_metadata,
    httpx_mock,
):
    httpx_mock.add_response(
        url=re.compile(r".*result=study.*ERP1.*"),
        json=[{"study_accession": "ERP1"}],
        is_reusable=True,
    )

    importer_flow_run = run_flow_and_capture_logs(
        import_all_legacy_analyses,
    )

    assert "Found 1 legacy studies to import" in importer_flow_run.logs
    assert "Importing study MGYS00005000" in importer_flow_run.logs

    assert Study.objects.filter(accession="MGYS00005000").exists()
    assert Analysis.objects.filter(id=12345).exists()
