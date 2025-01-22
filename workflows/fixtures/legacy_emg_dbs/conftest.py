import sys
from contextlib import contextmanager
from unittest import mock

import pymongo
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyAnalysisJob,
    LegacyAnalysisJobDownload,
    LegacyBiome,
    LegacyDownloadDescription,
    LegacyDownloadSubdir,
    LegacyEMGBase,
    LegacyRun,
    LegacySample,
    LegacyStudy,
)


@pytest.fixture(scope="function")
def in_memory_legacy_emg_db():
    engine = create_engine("sqlite:///:memory:", echo=False)

    LegacyEMGBase.metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    biome = LegacyBiome(
        id=1,
        biome_name="Martian soil",
        lineage="root:Environmental:Planetary:Martian soil",
    )
    session.add(biome)

    study = LegacyStudy(
        id=5000,
        centre_name="MARS",
        study_name="Bugs on mars",
        ext_study_id="ERP1",
        is_private=False,
        project_id="PRJ1",
        is_suppressed=False,
        biome_id=1,
    )
    session.add(study)

    sample = LegacySample(
        sample_id=1000,
        ext_sample_id="ERS1",
        primary_accession="SAMEA1",
    )
    session.add(sample)

    run = LegacyRun(
        run_id=1001,
        sample_id=1000,
        accession="ERR1000",
        experiment_type_id=3,
        secondary_accession="ERR1000",  # yes, they are usually duplicated in legacy db
        instrument_platform="Sequencer",
        instrument_model="Box",
        study_id=5000,
    )
    session.add(run)

    analysis = LegacyAnalysisJob(
        job_id=12345,
        sample_id=1000,
        run_id=1001,
        study_id=5000,
        pipeline_id=6,  # 6 is v6.0 in legacy EMG DB
        result_directory="some/dir/in/results",
        external_run_ids="ERR1000",
        secondary_accession="ERR1000",
        experiment_type_id=2,  # amplicon
        analysis_status_id=3,  # completed
    )
    session.add(analysis)

    download_description = LegacyDownloadDescription(
        id=17,
        description="OTUs and taxonomic assignments for SSU rRNA",
        description_label="OTUs, counts and taxonomic assignments for SSU rRNA",  # usually shorter, just not in this case
    )
    session.add(download_description)

    download_subdir = LegacyDownloadSubdir(id=1, subdir="tax/ssu_tax")
    session.add(download_subdir)

    download = LegacyAnalysisJobDownload(
        job_id=12345,
        id=1,
        real_name="BUGS_SSU.fasta.mseq_hdf5.biom",
        alias="BUGS_SSU_OTU_TABLE_HDF5.biom",
        description_id=17,
        subdir_id=1,
        format_id=6,  # biom
        group_id=3,  # taxonomic analysis of some sort
    )
    session.add(download)

    session.commit()

    yield session

    session.close()
    engine.dispose()


@pytest.fixture(scope="function")
def mock_legacy_emg_db_session(in_memory_legacy_emg_db, monkeypatch):
    @contextmanager
    def mock_legacy_session():
        yield in_memory_legacy_emg_db

    monkeypatch.setattr(
        "workflows.data_io_utils.legacy_emg_dbs.legacy_emg_db_session",
        mock_legacy_session,
    )

    # forceful mocking of the session manager everywhere because monkeypatch doesn't catch already cached imports
    for module_name, module in sys.modules.items():
        if hasattr(module, "legacy_emg_db_session"):
            setattr(module, "legacy_emg_db_session", mock_legacy_session)

    return mock_legacy_session


@pytest.fixture
def mock_mongo_client_for_taxonomy(monkeypatch):
    mock_mgya_data = {
        "accession": "MGYA00012345",
        "job_id": "12345",
        "pipeline_version": "5.0",
        "taxonomy_lsu": [
            {"count": 10, "organism": "Archaea:Euks::Something|5.0"},
            {"count": 20, "organism": "Bacteria|5.0"},
        ],
        "taxonomy_ssu": [
            {"count": 30, "organism": "Archaea:Euks::Something|5.0"},
            {"count": 40, "organism": "Bacteria|5.0"},
        ],
    }

    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()
    mock_collection = mock.MagicMock()

    mock_collection.find_one.return_value = mock_mgya_data
    mock_db.analysis_job_taxonomy = mock_collection
    mock_client.__getitem__.return_value = mock_db

    monkeypatch.setattr(pymongo, "MongoClient", lambda *args, **kwargs: mock_client)

    return mock_client
