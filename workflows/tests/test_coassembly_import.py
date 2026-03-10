import re

import pytest

from analyses.models import Analysis, Run
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyStudy,
    LegacySample,
    LegacyRun,
    LegacyAnalysisJob,
    LegacyAssembly,
    LegacyAssemblyRun,
    LegacyAssemblySample,
)
from workflows.flows.legacy.flows.import_v5_analyses import import_v5_analyses
from workflows.prefect_utils.testing_utils import (
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)


@pytest.fixture
def coassembly_legacy_db(in_memory_legacy_emg_db):
    with in_memory_legacy_emg_db as session:
        # Add a co-assembly study (ID 6000)
        study = LegacyStudy(
            id=6000,
            centre_name="COASSEMBLY",
            study_name="Co-assembly study",
            ext_study_id="ERP6000",
            is_private=False,
            submission_account_id="Webin-6000",
            project_id="PRJ6000",
            is_suppressed=False,
            biome_id=1,
        )
        session.add(study)

        # Two samples
        sample1 = LegacySample(
            sample_id=6001, ext_sample_id="ERS6001", primary_accession="SAMEA6001"
        )
        sample2 = LegacySample(
            sample_id=6002, ext_sample_id="ERS6002", primary_accession="SAMEA6002"
        )
        session.add_all([sample1, sample2])

        # Two runs
        run1 = LegacyRun(
            run_id=6001,
            sample_id=6001,
            accession="ERR6001",
            experiment_type_id=4,
            secondary_accession="ERR6001",
            study_id=6000,
            instrument_platform="Illumina",
            instrument_model="HiSeq",
        )
        run2 = LegacyRun(
            run_id=6002,
            sample_id=6002,
            accession="ERR6002",
            experiment_type_id=4,
            secondary_accession="ERR6002",
            study_id=6000,
            instrument_platform="Illumina",
            instrument_model="HiSeq",
        )
        session.add_all([run1, run2])

        # One assembly
        assembly = LegacyAssembly(
            assembly_id=6001, accession="ERZ6001", study_id=6000, experiment_type_id=4
        )
        session.add(assembly)

        # Links
        session.add(
            LegacyAssemblyRun(assembly_run_id=6001, assembly_id=6001, run_id=6001)
        )
        session.add(
            LegacyAssemblyRun(assembly_run_id=6002, assembly_id=6001, run_id=6002)
        )
        session.add(
            LegacyAssemblySample(
                assembly_sample_id=6001, assembly_id=6001, sample_id=6001
            )
        )
        session.add(
            LegacyAssemblySample(
                assembly_sample_id=6002, assembly_id=6001, sample_id=6002
            )
        )

        # Analysis job for the co-assembly
        analysis = LegacyAnalysisJob(
            job_id=66666,
            sample_id=6001,
            assembly_id=6001,
            study_id=6000,
            pipeline_id=6,
            result_directory="coassembly/results",
            external_run_ids="ERR6001,ERR6002",
            secondary_accession="ERZ6001",
            experiment_type_id=4,
            analysis_status_id=3,
        )
        session.add(analysis)
        session.commit()
    return in_memory_legacy_emg_db


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_import_coassembly(
    prefect_harness,
    coassembly_legacy_db,
    mock_legacy_emg_db_session,
    mock_mongo_client_for_taxonomy_and_protein_functions,
    httpx_mock,
):
    httpx_mock.add_response(
        url=re.compile(r".*result=study.*ERP6000.*"),
        json=[{"study_accession": "ERP6000"}],
        is_reusable=True,
    )
    # Mock ENA sample metadata
    httpx_mock.add_response(
        url=re.compile(r".*result=sample.*SAMEA6001.*"),
        json=[{"sample_accession": "SAMEA6001"}],
        is_reusable=True,
    )
    httpx_mock.add_response(
        url=re.compile(r".*result=sample.*SAMEA6002.*"),
        json=[{"sample_accession": "SAMEA6002"}],
        is_reusable=True,
    )

    run_flow_and_capture_logs(
        import_v5_analyses,
        mgys="MGYS00006000",
    )

    analysis = Analysis.objects.get(id=66666)
    assert analysis.assembly is not None
    # ERZ is usually in ena_accessions
    assert "ERZ6001" in analysis.assembly.ena_accessions

    # Check that the assembly is linked to both runs
    runs = analysis.assembly.runs.all()
    assert runs.count() == 2
    run_accessions = {r.first_accession for r in runs}
    assert run_accessions == {"ERR6001", "ERR6002"}

    # Check that the assembly is linked to correct samples...
    # At the moment we expect only just first sample, as assemblies are transitioning to single virtual samples
    assert analysis.assembly.sample.first_accession == "SAMEA6001"
    assert not analysis.assembly.sample.related_samples.exists()
    # The other sample should still be linked via its run though
    assert (
        Run.objects.get(ena_accessions__contains=["ERR6002"]).sample.first_accession
        == "SAMEA6002"
    )
