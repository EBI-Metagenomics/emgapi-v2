import json
import os
import shutil
from pathlib import Path
from textwrap import dedent
from unittest.mock import patch

import pytest
from django.conf import settings
from django.db.models import Q
from prefect.artifacts import Artifact

import analyses.models
from workflows.data_io_utils.file_rules.base_rules import FileRule, GlobRule
from workflows.data_io_utils.file_rules.common_rules import GlobHasFilesCountRule
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_study_summaries,
)
from workflows.flows.analysis_amplicon_study import analysis_amplicon_study
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

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
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_multiqc_report.html",
        "w",
    ):
        pass

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
    ) as pr2_tax:
        pr2_tax.write(
            dedent(
                """\
        7	d__Bacteria
        2	d__Bacteria	sg__CCD
        1	d__Bacteria	sg__CCD	dv__Chloroflexi	sdv__Chloroflexi_X	c__Anaerolineae	o__Anaerolineales
        1	d__Bacteria	sg__CCD	dv__Chloroflexi	sdv__Chloroflexi_X	c__Chloroflexia	o__Chloroflexales	f__Roseiflexaceae
        1	d__Bacteria	sg__CCD	dv__Chloroflexi	sdv__Chloroflexi_X	c__Dehalococcoidia
        1	d__Bacteria	sg__CCD	dv__Chloroflexi	sdv__Chloroflexi_X	c__Dehalococcoidia	o__S085
        1	d__Bacteria	sg__CCD	dv__Patescibacteria	sdv__Patescibacteria_X	c__Parcubacteria	o__Candidatus_Adlerbacteria
        1	d__Bacteria	sg__CCD	dv__Patescibacteria	sdv__Patescibacteria_X	c__Parcubacteria	o__Candidatus_Kaiserbacteria
        8	d__Bacteria	sg__CCD	dv__Patescibacteria	sdv__Patescibacteria_X	c__Saccharimonadia	o__Saccharimonadales
        19	d__Bacteria	sg__CCD	dv__Patescibacteria	sdv__Patescibacteria_X	c__Saccharimonadia	o__Saccharimonadales	f__Saccharimonadales_X
        """
            )
        )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_16S-V3-V4_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ) as dada2_16s_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_16S-V3-V4.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_DADA2-SILVA.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_18S-V9_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ) as dada2_18s_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_18S-V9.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_concat_DADA2-SILVA_asv_krona_counts.txt",
        "w",
    ) as dada2_concat_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{run}_concat.html",
        "w",
    ):
        for file in [
            dada2_16s_krona_count,
            dada2_18s_krona_count,
            dada2_concat_krona_count,
        ]:
            file.write(
                dedent(
                    """\
            54	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Kitasatosporales	f__Streptomycetaceae	g__Streptomyces
            20	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Microbacteriaceae
            9	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Arthrobacter
            28	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Pseudarthrobacter
            100	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Propionibacteriales	f__Nocardioidaceae	g__Aeromicrobium
            """
                )
            )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_16S-V3-V4_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ) as dada2_16s_pr2_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_16S-V3-V4.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_DADA2-PR2.mseq",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_18S-V9_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ) as dada2_18s_pr2_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_18S-V9.html",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_concat_DADA2-PR2_asv_krona_counts.txt",
        "w",
    ) as dada2_concat_pr2_krona_count, open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-PR2/{run}_concat.html",
        "w",
    ):
        for file in [
            dada2_16s_pr2_krona_count,
            dada2_18s_pr2_krona_count,
            dada2_concat_pr2_krona_count,
        ]:
            file.write(
                dedent(
                    """\
            2776	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Chitinophagales	f__Chitinophagaceae	g__Chitinophaga	s__Chitinophaga_sp.
            1566	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Cytophagales	f__Spirosomaceae	g__Dyadobacter	s__Dyadobacter_sp.
            135696	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Flavobacteriales	f__Flavobacteriaceae	g__Flavobacterium	s__Flavobacterium_sp.
            10847	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Sphingobacteriales	f__Sphingobacteriaceae	g__Pedobacter	s__Pedobacter_sp.
            192	d__Bacteria	sg__PANNAM	dv__Proteobacteria	sdv__Proteobacteria_X	c__Alphaproteobacteria	o__Caulobacterales	f__Caulobacteraceae	g__Caulobacter	s__Caulobacter_sp.
            """
                )
            )


def generate_fake_pipeline_no_asvs(amplicon_run_folder, run):
    # QC
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}",
        exist_ok=True,
    )
    with open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_seqfu.tsv",
        "w",
    ), open(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_multiqc_report.html",
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


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analyse_study_tasks.make_samplesheet_amplicon.queryset_hash")
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.amplicon.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
def test_prefect_analyse_amplicon_flow(
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
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
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
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
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
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
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
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
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
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina MiSeq",
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
    with open(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{amplicon_run_extra_dada2}.html",
        "w",
    ), open(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{amplicon_run_extra_dada2}_DADA2-SILVA.mseq",
        "w",
    ), open(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{amplicon_run_extra_dada2}_DADA2-SILVA.tsv",
        "w",
    ), open(
        f"{amplicon_folder}/{amplicon_run_extra_dada2}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/DADA2-SILVA/{amplicon_run_extra_dada2}_DADA2-SILVA.txt",
        "w",
    ) as dada2_silva_txt:
        dada2_silva_txt.write(
            dedent(
                """\
        54	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Kitasatosporales	f__Streptomycetaceae	g__Streptomyces
        20	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Microbacteriaceae
        9	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Arthrobacter
        28	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Pseudarthrobacter
        100	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Propionibacteriales	f__Nocardioidaceae	g__Aeromicrobium
        137	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Propionibacteriales	f__Nocardioidaceae	g__Nocardioides
        433	sk__Bacteria	k__	p__Bacillota	c__Bacilli	o__Bacillales	f__Paenibacillaceae	g__Paenibacillus
        2776	sk__Bacteria	k__	p__Bacteroidota	c__Chitinophagia	o__Chitinophagales	f__Chitinophagaceae	g__Chitinophaga
        1566	sk__Bacteria	k__	p__Bacteroidota	c__Cytophagia	o__Cytophagales	f__Spirosomataceae	g__Dyadobacter
        135696	sk__Bacteria	k__	p__Bacteroidota	c__Flavobacteriia	o__Flavobacteriales	f__Flavobacteriaceae	g__Flavobacterium
        """
            )
        )

    # RUN MAIN FLOW
    analysis_amplicon_study(study_accession=study_accession)

    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    assembly_samplesheet_table = Artifact.get("amplicon-v6-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == len(runs)

    assert (
        analyses.models.Analysis.objects.filter(
            run__ena_accessions__contains=amplicon_run_all_results
        ).count()
        == 1
    )

    # check completed runs (all runs in completed list - might contain sanity check not passed as well)
    assert (
        analyses.models.Analysis.objects.filter(status__analysis_completed=True).count()
        == 4
    )
    # check failed runs
    assert (
        analyses.models.Analysis.objects.filter(status__analysis_qc_failed=True).count()
        == 1
    )
    # check sanity check runs
    assert (
        analyses.models.Analysis.objects.filter(
            status__analysis_post_sanity_check_failed=True
        ).count()
        == 2
    )
    assert (
        analyses.models.Analysis.objects.filter(
            status__analysis_completed_reason="all_results"
        ).count()
        == 1
    )
    assert (
        analyses.models.Analysis.objects.filter(
            status__analysis_completed_reason="no_asvs"
        ).count()
        == 3
    )
    assert (
        analyses.models.Analysis.objects.filter(
            status__analysis_post_sanity_check_failed_reason="No qc folder"
        ).count()
        == 1
    )
    assert (
        analyses.models.Analysis.objects.filter(
            Q(
                status__analysis_post_sanity_check_failed_reason__icontains="DADA2-SILVA in taxonomy-summary"
            )
        ).count()
        == 1
    )

    analysis_which_should_have_taxonomies_imported: analyses.models.Analysis = (
        analyses.models.Analysis.objects_and_annotations.get(
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

    study = analyses.models.Study.objects.get_or_create_for_ena_study(study_accession)
    assert (
        study.results_dir == f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6"
    )

    Directory(
        path=study.results_dir,
        glob_rules=[
            GlobHasFilesCountRule[12]
        ],  # 6 for the samplesheet, same 6 for the "merge"
    )

    with (
        Path(study.results_dir) / "abc123_DADA2-SILVA_18S-V9_asv_study_summary.tsv"
    ).open("r") as summary:
        lines = summary.readlines()
        assert lines[0] == "taxonomy\tSRR_all_results\n"  # one run (the one with ASVs)
        assert "100" in lines[-1]
        assert "g__Aeromicrobium" in lines[-1]

    # manually remove the merged study summaries
    for file in Path(study.results_dir).glob(f"{study.first_accession}*"):
        file.unlink()

    # test merging of study summaries again, with cleanup enabled
    merge_study_summaries(mgnify_study_accession=study.accession, cleanup_partials=True)
    Directory(
        path=study.results_dir,
        glob_rules=[
            GlobHasFilesCountRule[6],
            GlobRule(
                rule_name="All files are study level",
                glob_patten=f"{study.first_accession}*study_summary.tsv",
                test=lambda f: len(list(f)) == 6,
            ),
        ],
    )

    study.refresh_from_db()
    assert len(study.downloads_as_objects) == 6
