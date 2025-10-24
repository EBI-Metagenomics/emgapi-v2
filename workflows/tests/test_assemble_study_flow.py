import json
import os
import shutil
from pathlib import Path
from typing import Optional, List
from unittest.mock import patch

import pandas as pd
import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy
from workflows.flows.assemble_study import AssemblerChoices, assemble_study
from workflows.flows.assemble_study_tasks.assemble_samplesheets import (
    get_reference_genome,
    update_assemblers_and_contaminant_ref_of_assemblies_from_samplesheet,
)
from workflows.flows.assemble_study_tasks.make_samplesheets import (
    make_samplesheets_for_runs_to_assemble,
)
from workflows.prefect_utils.analyses_models_helpers import mark_assembly_status

from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
    combine_caplog_records,
)

EMG_CONFIG = settings.EMG_CONFIG


@pytest.fixture
def assembly_study_input_mocker(biome_choices, user_choices):
    ## Pretend that a human resumed the flow with the biome picker, and then with the assembler selector.

    class MockAssembleStudyInput(BaseModel):
        biome: biome_choices
        assembler: AssemblerChoices
        webin_owner: Optional[str]
        watchers: List[user_choices]
        wait_for_samplesheet_editing: bool
        library_strategy_policy: ENALibraryStrategyPolicy

    return MockAssembleStudyInput


@pytest.mark.flaky(
    reruns=2
)  # sometimes fails due to missing report CSV. maybe xdist or shared tmp-dir problem?
@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.assemble_study"], indirect=True
)
@patch("workflows.flows.assemble_study_tasks.make_samplesheets.queryset_hash")
def test_prefect_assemble_study_flow(
    mock_queryset_hash_for_assemblies,
    prefect_harness,
    httpx_mock,
    mock_suspend_flow_run,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
    assemblers,
    admin_user,
    biome_choices,
    user_choices,
    assembly_study_input_mocker,
):
    ### ENA MOCKING ###
    accession = "SRP1"
    number_of_runs = 9
    number_not_assembled_runs = 1

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession%3D{accession}+OR+secondary_study_accession%3D{accession}%29%22&"
        f"fields=study_accession&"
        f"limit=&"
        f"format=json&"
        f"dataPortal=metagenome",
        json=[{"study_accession": accession}],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession={accession}%20OR%20secondary_study_accession={accession}%29%22&"
        f"limit=10&"
        f"format=json&"
        f"fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}&"
        f"dataPortal=metagenome",
        json=[
            {
                "study_title": "Metagenome of a wookie",
                "secondary_study_accession": "SRP1",
                "study_accession": "PRJNA1",
            }
        ],
    )

    """
    Runs description:
    SRR1: correct PE, WGS MetaG - metaspades
    SRR2: correct PE, WGS MetaG - metaspades (with failure)
    SRR3: correct SE, WGS MetaG - megahit
    SRR4: correct SE, WGS MetaG, LR - Flye
    SRR5: correct SE, WGS MetaG, ION_TORRENT - spades
    SRR6: correct PE, WGA MetaG - metaspades
    SRR7: correct PE, WGA MetaT - metaspades
    SRR8: incorrect SE, WGS MetaG - metaspades
    SRR9: incorrect PE, WGA MetaG - megahit
    """
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22%28study_accession=PRJNA1%20OR%20secondary_study_accession=PRJNA1%29%22"
        f"&limit=5000"
        f"&format=json"
        f"&fields=run_accession%2Csample_accession%2Csample_title%2Csecondary_sample_accession%2Cfastq_md5%2Cfastq_ftp%2Clibrary_layout%2Clibrary_strategy%2Clibrary_source%2Cscientific_name%2Chost_tax_id%2Chost_scientific_name%2Cinstrument_platform%2Cinstrument_model%2Clocation%2Clat%2Clon"
        f"&dataPortal=metagenome",
        json=[
            {
                "sample_accession": "SAMN01",
                "sample_title": "Wookie hair 1 (PE assembled with metaspades)",
                "secondary_sample_accession": "SRS1",
                "run_accession": "SRR1",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR1/SRR1_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR1/SRR1_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN02",
                "sample_title": "Wookie hair 2 (PE failed assembling with metaspades)",
                "secondary_sample_accession": "SRS2",
                "run_accession": "SRR2",
                "fastq_md5": "456;xyz",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR2/SRR2_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR2/SRR2_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN03",
                "sample_title": "Wookie hair 3 (SE should be assembled with megahit)",
                "secondary_sample_accession": "SRS3",
                "run_accession": "SRR3",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR3/SRR3.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN04",
                "sample_title": "Wookie hair 4 (LR should be assembled with flye)",
                "secondary_sample_accession": "SRS4",
                "run_accession": "SRR4",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR4/SRR4.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "OXFORD_NANOPORE",
                "instrument_model": "MinION",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN05",
                "sample_title": "Wookie hair 5 (ION_TORRENT should be assembled with spades)",
                "secondary_sample_accession": "SRS5",
                "run_accession": "SRR5",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR5/SRR5.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ION_TORRENT",
                "instrument_model": "Ion Torrent Proton",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN06",
                "sample_title": "Wookie hair 6 (PE labeled as WGA and METAGENOMIC)",
                "secondary_sample_accession": "SRS6",
                "run_accession": "SRR6",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR6/SRR6_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR6/SRR6_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGA",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN07",
                "sample_title": "Wookie hair 7 (PE labeled as WGA and METATRANSCRIPTOMIC)",
                "secondary_sample_accession": "SRS7",
                "run_accession": "SRR7",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR7/SRR7_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR7/SRR7_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGA",
                "library_source": "METATRANSCRIPTOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN08",
                "sample_title": "Wookie hair 8 (PE labeled as SE, should use changed LL)",
                "secondary_sample_accession": "SRS8",
                "run_accession": "SRR8",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR8/SRR8_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR8/SRR8_2.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN09",
                "sample_title": "Wookie hair 9 (SE labeled as PE, should use changed LL)",
                "secondary_sample_accession": "SRS9",
                "run_accession": "SRR9",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR9/SRR9.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGA",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
        ],
    )

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AssembleStudyInput":
            return assembly_study_input_mocker(
                biome=biome_choices["root.engineered"],
                assembler=AssemblerChoices.pipeline_default,
                webin_owner=None,
                watchers=[user_choices[admin_user.username]],
                wait_for_samplesheet_editing=False,
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    mock_queryset_hash_for_assemblies.return_value = "abc123"

    assembly_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/PRJNA1_miassembler/abc123"
    )
    assembly_folder.mkdir(exist_ok=True, parents=True)

    with open(f"{assembly_folder}/assembled_runs.csv", "w") as file:
        file.write("SRR1,metaspades,3.15.5\n")
        file.write("SRR3,megahit,1.2.9\n")
        file.write("SRR4,flye,2.9.5\n")
        file.write("SRR5,spades,3.15.5\n")
        file.write("SRR6,metaspades,3.15.5\n")
        file.write("SRR7,metaspades,3.15.5\n")
        file.write("SRR8,metaspades,3.15.5\n")
        file.write("SRR9,megahit,1.2.9")

    with open(f"{assembly_folder}/qc_failed_runs.csv", "w") as file:
        file.write("SRR2,filter_ratio_threshold_exceeded")

    # create fake results folders
    created_coverage_folders = [
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR3/SRR3/assembly/megahit/1.2.9/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR3/SRR3/assembly/megahit/1.2.9/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR4/SRR4/assembly/flye/2.9.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR5/SRR5/assembly/spades/3.15.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR6/SRR6/assembly/metaspades/3.15.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR7/SRR7/assembly/metaspades/3.15.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR8/SRR8/assembly/metaspades/3.15.5/coverage/",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR9/SRR9/assembly/megahit/1.2.9/coverage/",
    ]

    for folder in created_coverage_folders:
        os.makedirs(
            folder,
            exist_ok=True,
        )

    # create fake coverage files
    created_coverage_files = [
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/SRR1_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR3/SRR3/assembly/megahit/1.2.9/coverage/SRR3_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR4/SRR4/assembly/flye/2.9.5/coverage/SRR4_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR5/SRR5/assembly/spades/3.15.5/coverage/SRR5_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR6/SRR6/assembly/metaspades/3.15.5/coverage/SRR6_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR7/SRR7/assembly/metaspades/3.15.5/coverage/SRR7_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR8/SRR8/assembly/metaspades/3.15.5/coverage/SRR8_coverage.json",
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR9/SRR9/assembly/megahit/1.2.9/coverage/SRR9_coverage.json",
    ]

    for cov_file in created_coverage_files:
        with open(
            cov_file,
            "w",
        ) as file:
            json.dump(
                {"coverage": 0.04760503915318373, "coverage_depth": 273.694}, file
            )

    ### RUN WORKFLOW ###
    assemble_study(accession, upload=False)

    ### MOCKS WERE ALL CALLED ###
    mock_suspend_flow_run.assert_called()
    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    ### CLUSTER JOBS WERE SUBMITTED AND LOGGED AS EXPECTED ###
    assembly_samplesheet_table = Artifact.get("miassembler-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == number_of_runs
    # OXFORD_NANOPORE platform should be converted to 'ont' in samplesheet
    assert table_data[3]["platform"] == "ont"
    # ION_TORRENT should be iontorrent
    assert table_data[4]["platform"] == "iontorrent"
    # Check the genome used to decontamination
    assert table_data[4]["contaminant_reference"] == "honeybee.fna"

    ### DB OBJECTS WERE CREATED AS EXPECTED ###
    assert ena.models.Study.objects.count() == 1
    assert analyses.models.Study.objects.count() == 1
    mgys = analyses.models.Study.objects.select_related("ena_study").first()
    assert mgys.ena_study == ena.models.Study.objects.first()
    assert mgys.watchers.filter(id=admin_user.id).exists()

    assert ena.models.Sample.objects.count() == number_of_runs
    assert analyses.models.Sample.objects.count() == number_of_runs
    assert analyses.models.Run.objects.count() == number_of_runs
    assert analyses.models.Assembly.objects.count() == number_of_runs

    assembly = analyses.models.Assembly.objects.filter(
        run__ena_accessions__contains=["SRR1"]
    ).first()
    assert 0.0475 < assembly.metadata.get("coverage") < 0.0477

    assert assembly.dir == f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1"

    assert (
        analyses.models.Assembly.objects.filter(status__assembly_completed=True).count()
        == number_of_runs - number_not_assembled_runs
    )
    # for SE assembler should be swapped to megahit
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact="megahit"
        ).count()
        == 2
    )
    # but platform ION_TORRENT SE should be assembled with spades
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact="spades"
        ).count()
        == 1
    )
    # illumina PE reads assembled with metaspades
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact="metaspades"
        ).count()
        == 5
    )
    # platform OXFORD_NANOPORE should be assembled with Flye as LR
    assert (
        analyses.models.Assembly.objects.filter(assembler__name__iexact="flye").count()
        == 1
    )

    failed_assembly: analyses.models.Assembly = analyses.models.Assembly.objects.get(
        status__pre_assembly_qc_failed=True
    )

    assert (
        failed_assembly.status["pre_assembly_qc_failed_reason"]
        == "filter_ratio_threshold_exceeded"
    )

    shutil.rmtree(assembly_folder, ignore_errors=True)


@pytest.mark.flaky(
    reruns=2
)  # sometimes fails due to missing report CSV. maybe xdist or shared tmp-dir problem?
@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.assemble_study"], indirect=True
)
@patch("workflows.flows.assemble_study_tasks.make_samplesheets.queryset_hash")
def test_prefect_assemble_private_study_flow(
    mock_queryset_hash_for_assemblies,
    prefect_harness,
    httpx_mock,
    mock_suspend_flow_run,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
    assemblers,
    admin_user,
    user_choices,
    biome_choices,
    assembly_study_input_mocker,
):
    ### ENA MOCKING ###
    accession = "SRP1"

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession%3D{accession}+OR+secondary_study_accession%3D{accession}%29%22&"
        f"fields=study_accession&"
        f"limit=&"
        f"format=json&"
        f"dataPortal=metagenome",
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
        json=[{"study_accession": accession}],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession%3D{accession}+OR+secondary_study_accession%3D{accession}%29%22&"
        f"fields=study_accession&"
        f"limit=&"
        f"format=json&"
        f"dataPortal=metagenome",
        match_headers={},  # public call should not find private study
        json=[],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession={accession}%20OR%20secondary_study_accession={accession}%29%22&"
        f"limit=10&"
        f"format=json&"
        f"fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}&"
        f"dataPortal=metagenome",
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
        json=[
            {
                "study_title": "Metagenome of a wookie",
                "secondary_study_accession": "SRP1",
                "study_accession": "PRJNA1",
            }
        ],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22%28study_accession=PRJNA1%20OR%20secondary_study_accession=PRJNA1%29%22"
        f"&limit=5000"
        f"&format=json"
        f"&fields=run_accession%2Csample_accession%2Csample_title%2Csecondary_sample_accession%2Cfastq_md5%2Cfastq_ftp%2Clibrary_layout%2Clibrary_strategy%2Clibrary_source%2Cscientific_name%2Chost_tax_id%2Chost_scientific_name%2Cinstrument_platform%2Cinstrument_model%2Clocation%2Clat%2Clon"
        f"&dataPortal=metagenome",
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
        json=[
            {
                "sample_accession": "SAMN01",
                "sample_title": "Wookie hair 1",
                "secondary_sample_accession": "SRS1",
                "run_accession": "SRR1",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.dcc_private.example.org/vol/fastq/SRR1/SRR1_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR1/SRR1_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
            {
                "sample_accession": "SAMN02",
                "sample_title": "Wookie hair 2",
                "secondary_sample_accession": "SRS2",
                "run_accession": "SRR2",
                "fastq_md5": "456;xyz",
                "fastq_ftp": "ftp.dcc_private.example.org/vol/fastq/SRR2/SRR2_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR2/SRR2_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
                "lat": "52",
                "lon": "0",
                "location": "hinxton",
            },
        ],
    )

    ## Pretend that a human resumed the flow with the biome picker, and then with the assembler selector.
    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AssembleStudyInput":
            return assembly_study_input_mocker(
                biome=biome_choices["root.engineered"],
                assembler=AssemblerChoices.pipeline_default,
                webin_owner="Webin-1",
                watchers=[user_choices[admin_user.username]],
                wait_for_samplesheet_editing=False,
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    mock_queryset_hash_for_assemblies.return_value = "xyz789"

    assembly_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/PRJNA1_miassembler/xyz789"
    )
    assembly_folder.mkdir(exist_ok=True, parents=True)

    with (assembly_folder / "assembled_runs.csv").open("w") as file:
        file.write("SRR1,metaspades,3.15.5")

    with (assembly_folder / "qc_failed_runs.csv").open("w") as file:
        file.write("SRR2,filter_ratio_threshold_exceeded")

    (
        assembly_folder / "PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/"
    ).mkdir(parents=True, exist_ok=True)

    with (
        assembly_folder
        / "PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/SRR1_coverage.json"
    ).open("w") as file:
        json.dump({"coverage": 0.04760503915318373, "coverage_depth": 273.694}, file)

    ### RUN WORKFLOW ###
    assemble_study(accession, upload=False)

    ### DB OBJECTS WERE CREATED AS EXPECTED ###
    assert ena.models.Study.objects.count() == 1
    assert analyses.models.Study.public_objects.count() == 0  # no public study created
    assert analyses.models.Study.objects.count() == 1  # a private study created
    mgys: analyses.models.Study = analyses.models.Study.objects.select_related(
        "ena_study"
    ).first()
    assert mgys.is_private

    for assembly in mgys.assemblies_reads.filter(is_private=True):
        assert assembly.is_private

    shutil.rmtree(assembly_folder, ignore_errors=True)


@pytest.mark.django_db(transaction=True)
def test_assembly_statuses(prefect_harness, mgnify_assemblies):
    assembly = mgnify_assemblies[0]
    assert not any(assembly.status.values())

    # marking as failed should work
    mark_assembly_status(
        assembly, assembly.AssemblyStates.ASSEMBLY_FAILED, reason="It broke"
    )
    assembly.refresh_from_db()
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_FAILED]

    # making as complete later (perhaps a retry) should work, and can unset failed at same time
    mark_assembly_status(
        assembly,
        assembly.AssemblyStates.ASSEMBLY_COMPLETED,
        unset_statuses=[
            assembly.AssemblyStates.ASSEMBLY_FAILED,
            assembly.AssemblyStates.ASSEMBLY_BLOCKED,
        ],
    )
    assembly.refresh_from_db()

    assert not assembly.status[assembly.AssemblyStates.ASSEMBLY_FAILED]
    assert not assembly.status[assembly.AssemblyStates.ASSEMBLY_BLOCKED]
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_COMPLETED]

    # reason should have been updated too
    assert (
        assembly.status["assembly_failed_reason"]
        == "Explicitly unset when setting assembly_completed"
    )

    # reason should not have been updated for a status that was not previously set
    assert "assembly_blocked_reason" not in assembly.status


@pytest.mark.django_db(transaction=True)
def test_assembler_changed_in_samplesheet(
    prefect_harness,
    mgnify_assemblies,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
):
    # all assemblies should be initially metaspades, except one which was initially megahit
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact=analyses.models.Assembler.METASPADES
        ).count()
        > 1
    )
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact=analyses.models.Assembler.MEGAHIT
        ).count()
        == 1
    )

    study = mgnify_assemblies[0].reads_study
    study.biome = analyses.models.Biome.objects.first()
    study.save()

    samplesheets = make_samplesheets_for_runs_to_assemble(
        mgnify_study_accession=study.accession, assembler=mgnify_assemblies[0].assembler
    )
    samplesheet: Path = samplesheets[0][
        0
    ]  # first samplesheet, path component of path,hash tuple
    assert samplesheet.exists()

    # edit samplesheet, change assembler
    with samplesheet.open("r") as ss_handle:
        ss = ss_handle.read()
    ss = ss.replace("metaspades", "megahit")
    with samplesheet.open("w") as ss_handle:
        ss_handle.write(ss)

    # run assembly on samplesheet
    # note that assembler arg will be metaspades
    ss_df = pd.read_csv(samplesheet)
    update_assemblers_and_contaminant_ref_of_assemblies_from_samplesheet(ss_df)

    # flow should have updated assemblies to have megahit assembler, as per edited samplesheet
    assert (
        analyses.models.Assembly.objects.filter(
            assembler__name__iexact=analyses.models.Assembler.MEGAHIT
        ).count()
        > 1
    )


@pytest.mark.django_db(transaction=True)
def test_reference_genome_selection(prefect_harness, mgnify_assemblies, caplog):
    study = analyses.models.Study.objects.first()

    ref = get_reference_genome(study)
    assert ref is None
    assert f"Found no run in {study} with host taxon info" in combine_caplog_records(
        caplog.records
    )
    caplog.clear()

    run = study.runs.first()
    run.metadata[analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID] = (
        999  # not a support tax id
    )
    run.save()
    ref = get_reference_genome(study)
    assert ref is None
    assert f"Using run {run} for determining host" in combine_caplog_records(
        caplog.records
    )
    caplog.clear()

    run.metadata[analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID] = 7460  # honeybee
    run.save()
    ref = get_reference_genome(study)
    assert ref == "honeybee.fna"

    run.metadata[analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID] = None
    run.metadata[analyses.models.Run.CommonMetadataKeys.HOST_SCIENTIFIC_NAME] = (
        "Gallus gallus"  # chicken
    )
    run.save()
    ref = get_reference_genome(study)
    assert ref == "chicken.fna"
