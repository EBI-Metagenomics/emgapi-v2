import json
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Optional, List

import pandas as pd
import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
import ena.models
from workflows.flows.assemble_study import AssemblerChoices, assemble_study
from workflows.flows.assemble_study_tasks.assemble_samplesheets import (
    get_reference_genome,
    update_assemblies_assemblers_from_samplesheet,
)
from workflows.flows.assemble_study_tasks.make_samplesheets import (
    make_samplesheets_for_runs_to_assemble,
)
from workflows.prefect_utils.analyses_models_helpers import (
    task_mark_assembly_status,
    get_users_as_choices,
)
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

EMG_CONFIG = settings.EMG_CONFIG


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.assemble_study"], indirect=True
)
def test_prefect_assemble_study_flow(
    prefect_harness,
    httpx_mock,
    mock_suspend_flow_run,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
    assemblers,
    admin_user,
):
    ### ENA MOCKING ###
    accession = "SRP1"
    number_of_runs = 5
    number_not_assembled_runs = 1

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession%3D{accession}+OR+secondary_study_accession%3D{accession}%29%22&"
        f"fields=study_accession&"
        f"limit=&"
        f"format=json",
        json=[{"study_accession": accession}],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession={accession}%20OR%20secondary_study_accession={accession}%29%22&"
        f"limit=10&"
        f"format=json&"
        f"fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
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
        f"&fields={','.join(EMG_CONFIG.ena.readrun_metadata_fields)}"
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
            },
        ],
    )

    ## Pretend that a human resumed the flow with the biome picker, and then with the assembler selector.
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AssembleStudyInput(BaseModel):
        biome: BiomeChoices
        assembler: AssemblerChoices
        webin_owner: Optional[str]
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AssembleStudyInput":
            return AssembleStudyInput(
                biome=BiomeChoices["root.engineered"],
                assembler=AssemblerChoices.pipeline_default,
                webin_owner=None,
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    assembly_folder = f"{EMG_CONFIG.slurm.default_workdir}/PRJNA1_miassembler"
    os.mkdir(assembly_folder)

    with open(f"{assembly_folder}/assembled_runs.csv", "w") as file:
        file.write("SRR1,metaspades,3.15.5\n")
        file.write("SRR3,megahit,1.2.9\n")
        file.write("SRR4,flye,2.9.5\n")
        file.write("SRR5,spades,3.15.5")

    with open(f"{assembly_folder}/qc_failed_runs.csv", "w") as file:
        file.write("SRR2,filter_ratio_threshold_exceeded")

    # create fake results folders
    os.makedirs(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/",
        exist_ok=True,
    )
    os.makedirs(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR3/SRR3/assembly/megahit/1.2.9/coverage/",
        exist_ok=True,
    )
    os.makedirs(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR4/SRR4/assembly/flye/2.9.5/coverage/",
        exist_ok=True,
    )
    os.makedirs(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR5/SRR5/assembly/spades/3.15.5/coverage/",
        exist_ok=True,
    )
    # create fake coverage files
    with open(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR1/SRR1/assembly/metaspades/3.15.5/coverage/SRR1_coverage.json",
        "w",
    ) as file:
        json.dump({"coverage": 0.04760503915318373, "coverage_depth": 273.694}, file)
    with open(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR3/SRR3/assembly/megahit/1.2.9/coverage/SRR3_coverage.json",
        "w",
    ) as file:
        json.dump({"coverage": 0.04960503915318373, "coverage_depth": 273.694}, file)
    with open(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR4/SRR4/assembly/flye/2.9.5/coverage/SRR4_coverage.json",
        "w",
    ) as file:
        json.dump({"coverage": 0.049, "coverage_depth": 276.694}, file)
    with open(
        f"{assembly_folder}/PRJNA1/PRJNA1/SRR5/SRR5/assembly/spades/3.15.5/coverage/SRR5_coverage.json",
        "w",
    ) as file:
        json.dump({"coverage": 0.049, "coverage_depth": 276.694}, file)

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
        run__ena_accessions__contains="SRR1"
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
        == 1
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
        == 2
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


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.assemble_study"], indirect=True
)
def test_prefect_assemble_private_study_flow(
    prefect_harness,
    httpx_mock,
    mock_suspend_flow_run,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    top_level_biomes,
    assemblers,
    admin_user,
):
    ### ENA MOCKING ###
    accession = "SRP1"

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession%3D{accession}+OR+secondary_study_accession%3D{accession}%29%22&"
        f"fields=study_accession&"
        f"limit=&"
        f"format=json",
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
        f"format=json",
        match_headers={},  # public call should not find private study
        json=[],
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study&"
        f"query=%22%28study_accession={accession}%20OR%20secondary_study_accession={accession}%29%22&"
        f"limit=10&"
        f"format=json&"
        f"fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
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
        f"&fields={','.join(EMG_CONFIG.ena.readrun_metadata_fields)}"
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
            },
        ],
    )

    ## Pretend that a human resumed the flow with the biome picker, and then with the assembler selector.
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AssembleStudyInput(BaseModel):
        biome: BiomeChoices
        assembler: AssemblerChoices
        webin_owner: Optional[str]
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AssembleStudyInput":
            return AssembleStudyInput(
                biome=BiomeChoices["root.engineered"],
                assembler=AssemblerChoices.pipeline_default,
                webin_owner="Webin-1",
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    assembly_folder = Path(f"{EMG_CONFIG.slurm.default_workdir}/PRJNA1_miassembler")
    assembly_folder.mkdir(exist_ok=True)

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
    task_mark_assembly_status(
        assembly, assembly.AssemblyStates.ASSEMBLY_FAILED, reason="It broke"
    )
    assembly.refresh_from_db()
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_FAILED]

    # making as complete later (perhaps a retry) should work, and can unset failed at same time
    task_mark_assembly_status(
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
        mgnify_study=study, assembler=mgnify_assemblies[0].assembler
    )
    samplesheet: Path = samplesheets[0]
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
    update_assemblies_assemblers_from_samplesheet(ss_df)

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
    assert f"Found no run in {study} with host taxon info" in caplog.text
    caplog.clear()

    run = study.runs.first()
    run.metadata[analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID] = (
        999  # not a support tax id
    )
    run.save()
    ref = get_reference_genome(study)
    assert ref is None
    assert f"Using run {run} for determining host" in caplog.text
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
