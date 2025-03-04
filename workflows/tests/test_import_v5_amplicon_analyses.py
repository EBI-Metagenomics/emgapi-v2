import pymongo
import pytest
from sqlalchemy import select

from analyses.models import Analysis
from workflows.data_io_utils.legacy_emg_dbs import LegacyStudy, legacy_emg_db_session
from workflows.flows.import_v5_amplicon_analyses import import_v5_amplicon_analyses
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


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


@pytest.mark.django_db(transaction=True)
def test_prefect_import_v5_amplicon_analyses_flow(
    prefect_harness,
    mock_legacy_emg_db_session,
    mock_mongo_client_for_taxonomy,
    ninja_api_client,
):
    importer_flow_run = run_flow_and_capture_logs(
        import_v5_amplicon_analyses,
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
