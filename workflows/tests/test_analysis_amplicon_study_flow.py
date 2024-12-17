import json
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from django.conf import settings
from django.db.models import Q
from prefect.artifacts import Artifact

import analyses.models
from workflows.data_io_utils.file_rules.base_rules import FileRule
from workflows.flows.analysis_amplicon_study import analysis_amplicon_study

EMG_CONFIG = settings.EMG_CONFIG


def generate_fake_pipeline_all_results(amplicon_run_folder, run):
    # QC
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_seqfu.tsv",
        "w",
    ):
        pass
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}.fastp.json",
        "w",
    ) as fastp:
        json.dump({"summary": {"before_filtering": {"total_bases": 10}}}, fastp)

    # PRIMER IDENTIFICATION
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}/{run}.cutadapt.json",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}/{run}_primers.fasta",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}/{run}_primer_validation.tsv",
        "w",
    ):
        pass

    # AMPLIFIED REGION INFERENCE
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}/{run}.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}/{run}.16S.V3-V4.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}/{run}.18S.V9.txt",
        "w",
    ):
        pass

    # ASV
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/16S-V3-V4",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/18S-V9",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/concat",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/{run}_dada2_stats.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/{run}_DADA2-SILVA_asv_tax.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/{run}_DADA2-PR2_asv_tax.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/{run}_asv_seqs.fasta",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/18S-V9/{run}_18S-V9_asv_read_counts.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/16S-V3-V4/{run}_16S-V3-V4_asv_read_counts.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.asv_folder}/concat/{run}_concat_asv_read_counts.tsv",
        "w",
    ):
        pass

    # SEQUENCE CATEGORISATION
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU.fasta",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}.tblout.deoverlapped",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU_rRNA_bacteria.RF00177.fa",
        "w",
    ):
        pass

    # TAXONOMY SUMMARY
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}.html",
        "w",
    ) as ssu_krona, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.tsv",
        "w",
    ) as ssu_tsv, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.txt",
        "w",
    ):
        ssu_tsv.writelines(
            [
                "# Constructed from biome file\n",
                "# OTU ID\tSSU\ttaxonomy\ttaxid\n",
                "36901\t1.0\tsk__Bacteria;k__;p__Bacillota;c__Bacilli\t91061\n",
                "60237\t2.0\tsk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Carnobacteriaceae;g__Trichococcus\t82802\n"
                "65035\t1.0\tsk__Bacteria;k__;p__Bacillota;c__Clostridia;o__Eubacteriales;f__Eubacteriaceae;g__Eubacterium;s__Eubacterium_coprostanoligenes\t290054\n",
            ]
        )
        ssu_krona.write("<html></html>")
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.txt",
        "w",
    ):
        pass
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_16S-V3-V4_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_16S-V3-V4.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_DADA2-SILVA.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_18S-V9_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_18S-V9.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_concat_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_concat.html",
        "w",
    ):
        pass
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_16S-V3-V4_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_16S-V3-V4.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_DADA2-PR2.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_18S-V9_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_18S-V9.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_concat_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_concat.html",
        "w",
    ):
        pass


def generate_fake_pipeline_no_asvs(amplicon_run_folder, run):
    # QC
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_seqfu.tsv",
        "w",
    ):
        pass

    # AMPLIFIED REGION INFERENCE
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}/{run}.tsv",
        "w",
    ):
        pass

    # SEQUENCE CATEGORISATION
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU.fasta",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}.tblout.deoverlapped",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU_rRNA_bacteria.RF00177.fa",
        "w",
    ):
        pass

    # TAXONOMY SUMMARY
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU",
        exist_ok=True,
    )
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.tsv",
        "w",
    ) as ssu_tsv, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.txt",
        "w",
    ):
        ssu_tsv.writelines(
            [
                "# OTU ID\tSSU\ttaxonomy\ttaxid\n",
                "36901\t1.0\tsk__Bacteria;k__;p__Bacillota;c__Bacilli\t91061\n",
            ]
        )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.txt",
        "w",
    ):
        pass


MockFileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty (unit test mock)",
    test=lambda f: True,  # allows empty files created by mocks
)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@patch("workflows.flows.analysis_amplicon_study.queryset_hash")
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.amplicon.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
async def test_prefect_analyse_amplicon_flow(
    mock_queryset_hash_for_amplicon,
    prefect_harness,
    httpx_mock,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_ena_study,
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for amplicon runs and launch it with samplesheet.
    One run has all results, one run failed
    """
    mock_queryset_hash_for_amplicon.return_value = "abc123"

    study_accession = "PRJNA398089"
    amplicon_run_all_results = "SRR_all_results"
    amplicon_run_failed = "SRR_failed"
    amplicon_run_no_asv = "SRR_no_asv"
    amplicon_run_no_qc = "SRR_no_qc"
    amplicon_run_extra_dada2 = "SRR_extra_dada2"
    runs = [
        amplicon_run_all_results,
        amplicon_run_failed,
        amplicon_run_no_asv,
        amplicon_run_no_qc,
        amplicon_run_extra_dada2,
    ]

    # mock ENA response
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22(study_accession={study_accession}%20OR%20secondary_study_accession={study_accession})%20AND%20library_strategy=AMPLICON%22"
        f"&limit=10000"
        f"&format=json"
        f"&fields={','.join(EMG_CONFIG.ena.readrun_metadata_fields)}"
        f"&dataPortal=metagenome",
        json=[
            {
                "sample_accession": "SAMN08514017",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514017",
                "run_accession": amplicon_run_all_results,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run_all_results}/{amplicon_run_all_results}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run_all_results}/{amplicon_run_all_results}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
            },
            {
                "sample_accession": "SAMN08514018",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514018",
                "run_accession": amplicon_run_failed,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run_failed}/{amplicon_run_failed}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run_failed}/{amplicon_run_failed}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
            },
            {
                "sample_accession": "SAMN08514019",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514019",
                "run_accession": amplicon_run_no_asv,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run_no_asv}/{amplicon_run_no_asv}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run_no_asv}/{amplicon_run_no_asv}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
            },
            {
                "sample_accession": "SAMN08514020",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514020",
                "run_accession": amplicon_run_no_qc,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run_no_qc}/{amplicon_run_no_qc}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run_no_qc}/{amplicon_run_no_qc}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
            },
            {
                "sample_accession": "SAMN08514021",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514021",
                "run_accession": amplicon_run_extra_dada2,
                "fastq_md5": "123;abc",
                "fastq_ftp": f"ftp.sra.example.org/vol/fastq/{amplicon_run_extra_dada2}/{amplicon_run_extra_dada2}_1.fastq.gz;ftp.sra.example.org/vol/fastq/{amplicon_run_extra_dada2}/{amplicon_run_extra_dada2}_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
            },
        ],
    )

    # create fake results
    amplicon_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_amplicon_v6/abc123"
    )
    amplicon_folder.mkdir(exist_ok=True, parents=True)

    with open(
        f"{amplicon_folder}/{EMG_CONFIG.amplicon_pipeline.completed_runs_csv}", "w"
    ) as file:
        file.write(f"{amplicon_run_all_results},all_results" + "\n")
        file.write(f"{amplicon_run_no_asv},no_asvs" + "\n")
        file.write(f"{amplicon_run_no_qc},no_asvs" + "\n")
        file.write(f"{amplicon_run_extra_dada2},no_asvs")
    with open(
        f"{amplicon_folder}/{EMG_CONFIG.amplicon_pipeline.failed_runs_csv}", "w"
    ) as file:
        file.write(f"{amplicon_run_failed},failed")

    # ------- results for completed runs with all results
    generate_fake_pipeline_all_results(
        f"{amplicon_folder}/{amplicon_run_all_results}", amplicon_run_all_results
    )
    # ------- results for completed runs with no asv results
    generate_fake_pipeline_no_asvs(
        f"{amplicon_folder}/{amplicon_run_no_asv}", amplicon_run_no_asv
    )
    generate_fake_pipeline_no_asvs(
        f"{amplicon_folder}/{amplicon_run_no_qc}", amplicon_run_no_qc
    )
    generate_fake_pipeline_no_asvs(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}", amplicon_run_extra_dada2
    )
    # rm qc to generate failed sanity check run
    shutil.rmtree(
        f"{amplicon_folder}/{amplicon_run_no_qc}/{EMG_CONFIG.amplicon_pipeline.qc_folder}"
    )
    # add extra dada2 folder
    os.makedirs(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA",
        exist_ok=True,
    )

    # RUN MAIN FLOW
    await analysis_amplicon_study(study_accession=study_accession)

    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    assembly_samplesheet_table = await Artifact.get("amplicon-v6-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == len(runs)

    assert (
        await analyses.models.Analysis.objects.filter(
            run__ena_accessions__contains=amplicon_run_all_results
        ).acount()
        == 1
    )

    # check completed runs (all runs in completed list - might contain sanity check not passed as well)
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_completed=True
        ).acount()
        == 4
    )
    # check failed runs
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_pre_qc_failed=True
        ).acount()
        == 1
    )
    # check sanity check runs
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_post_sanity_check_failed=True
        ).acount()
        == 2
    )
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_completed_reason="all_results"
        ).acount()
        == 1
    )
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_completed_reason="no_asvs"
        ).acount()
        == 3
    )
    assert (
        await analyses.models.Analysis.objects.filter(
            status__analysis_post_sanity_check_failed_reason="No qc folder"
        ).acount()
        == 1
    )
    assert (
        await analyses.models.Analysis.objects.filter(
            Q(
                status__analysis_post_sanity_check_failed_reason__icontains="DADA2-SILVA in taxonomy-summary"
            )
        ).acount()
        == 1
    )

    analysis_which_should_have_taxonomies_imported: analyses.models.Analysis = (
        await analyses.models.Analysis.objects_and_annotations.aget(
            run__ena_accessions__contains=amplicon_run_all_results
        )
    )
    assert (
        analyses.models.Analysis.TAXONOMIES
        in analysis_which_should_have_taxonomies_imported.annotations
    )
    assert (
        analyses.models.Analysis.TaxonomySources.SSU.value
        in analysis_which_should_have_taxonomies_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    ssu = analysis_which_should_have_taxonomies_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.SSU.value]
    assert len(ssu) == 3
    assert ssu[0]["organism"] == "sk__Bacteria;k__;p__Bacillota;c__Bacilli"
