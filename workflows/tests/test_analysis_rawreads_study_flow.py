from textwrap import dedent
import json
import logging
import os
from enum import Enum
from pathlib import Path
from typing import List, Optional
from unittest.mock import patch

import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
from workflows.data_io_utils.file_rules.base_rules import FileRule
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy
from workflows.flows.analysis_rawreads_study import analysis_rawreads_study
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

EMG_CONFIG = settings.EMG_CONFIG


def generate_fake_rawreads_pipeline_results(results_dir, sample_accession):
    """
    Generate fake raw-reads pipeline results for testing.

    Based on the directory structure provided in the issue description.

    :param results_dir: Directory to create the fake results in
    :param sample_accession: Sample accession to use in file names
    """

    logger = logging.getLogger("generate_dummy_data_debug")
    # Create the main directory
    os.makedirs(results_dir, exist_ok=True)

    # Create function-summary directory and subdirectories
    func_dir = f"{results_dir}/{sample_accession}/function-summary"
    logger.info(f"Creating dummy functional results at {func_dir}")
    pfam_dir = f"{func_dir}/Pfam-A"
    os.makedirs(pfam_dir, exist_ok=True)
    with open(f"{pfam_dir}/{sample_accession}_Pfam-A.txt", "wt") as f:
        f.write(
            dedent(
                """\
                # function	read_count	coverage_depth	coverage_breadth
                PF02826.25	43	9.162921348314606	0.9606741573033708
                PF00389.36	14	3.8358208955223883	0.6567164179104478
                PF10417.14	5	3.292682926829268	0.7317073170731707
                PF13614.12	15	3.0338983050847457	0.807909604519774
                PF03061.28	7	3.0126582278481013	0.9620253164556962
                PF00004.35	12	3.0	0.7175572519083969
                PF13173.12	10	2.7829457364341086	0.8914728682170543
                """
            )
        )
    os.makedirs(f"{pfam_dir}/raw", exist_ok=True)
    with open(f"{pfam_dir}/raw/{sample_accession}_Pfam-A.domtbl", "w"):
        pass

    # Create taxonomy-summary directory and subdirectories
    tax_dir = f"{results_dir}/{sample_accession}/taxonomy-summary"
    logger.info(f"Creating dummy taxonomy results at {tax_dir}")

    # mOTUs
    motus_dir = f"{tax_dir}/mOTUs"
    os.makedirs(motus_dir, exist_ok=True)
    with open(f"{motus_dir}/{sample_accession}_mOTUs.txt", "wt") as f:
        f.write(
            dedent(
                """\
                # Count	Kingdom	Phylum	Class	Order	Family	Genus	Species
                1.0	k__Bacteria	p__Firmicutes	c__Bacilli	o__Lactobacillales	f__Lactobacillaceae	g__Lactobacillus	s__Lactobacillus gasseri
                2.0	k__Bacteria	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium longum [Bifidobacterium longum CAG:69/Bifidobacterium longum]
                1.0	k__Bacteria	p__Bacteroidetes	c__Bacteroidia	o__Bacteroidales	f__Bacteroidaceae	g__Bacteroides	s__Bacteroides thetaiotaomicron
                """
            )
        )
    os.makedirs(f"{motus_dir}/raw", exist_ok=True)
    with open(f"{motus_dir}/raw/{sample_accession}_mOTUs.out", "w"):
        pass
    os.makedirs(f"{motus_dir}/krona", exist_ok=True)
    with open(f"{motus_dir}/krona/{sample_accession}_mOTUs.html", "w"):
        pass

    # SILVA-SSU
    silvassu_dir = f"{tax_dir}/SILVA-SSU"
    os.makedirs(silvassu_dir, exist_ok=True)
    with open(f"{silvassu_dir}/{sample_accession}_SILVA-SSU.txt", "wt") as f:
        f.write(
            dedent(
                """\
                # Count	Superkingdom	Kingdom	Phylum	Class	Order	Family	Genus	Species
                1	sk__Bacteria	k__	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_breve
                3	sk__Bacteria	k__	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_longum
                """
            )
        )
    os.makedirs(f"{silvassu_dir}/mapseq", exist_ok=True)
    with open(f"{silvassu_dir}/mapseq/{sample_accession}_SILVA-SSU.mseq", "w"):
        pass
    os.makedirs(f"{silvassu_dir}/krona", exist_ok=True)
    with open(f"{silvassu_dir}/krona/{sample_accession}_SILVA-SSU.html", "w"):
        pass

    # SILVA-LSU
    silvalsu_dir = f"{tax_dir}/SILVA-LSU"
    os.makedirs(silvalsu_dir, exist_ok=True)
    with open(f"{silvalsu_dir}/{sample_accession}_SILVA-LSU.txt", "wt") as f:
        f.write(
            dedent(
                """\
                # Count	Superkingdom	Kingdom	Phylum	Class	Order	Family	Genus	Species
                4	sk__Bacteria	k__	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_breve
                18	sk__Bacteria	k__	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_longum
                """
            )
        )
    os.makedirs(f"{silvalsu_dir}/mapseq", exist_ok=True)
    with open(f"{silvalsu_dir}/mapseq/{sample_accession}_SILVA-LSU.mseq", "w"):
        pass
    os.makedirs(f"{silvalsu_dir}/krona", exist_ok=True)
    with open(f"{silvalsu_dir}/krona/{sample_accession}_SILVA-LSU.html", "w"):
        pass

    # Create qc directory and subdirectories
    qc_dir = f"{results_dir}/{sample_accession}/qc-stats"
    logger.info(f"Creating dummy QC results at {qc_dir}")
    fastp_dir = f"{qc_dir}/fastp"
    os.makedirs(fastp_dir, exist_ok=True)
    with open(f"{fastp_dir}/{sample_accession}_fastp.json", "w") as f:
        f.write(
            """\
            {
                    "summary": {
                            "fastp_version": "0.23.4",
                            "sequencing": "paired end (150 cycles + 150 cycles)",
                            "before_filtering": {
                                    "total_reads":2012,
                                    "total_bases":251320,
                                    "q20_bases":247619,
                                    "q30_bases":241027,
                                    "q20_rate":0.985274,
                                    "q30_rate":0.959044,
                                    "read1_mean_length":125,
                                    "read2_mean_length":124,
                                    "gc_content":0.46886
                            },
                            "after_filtering": {
                                    "total_reads":2012,
                                    "total_bases":251273,
                                    "q20_bases":247578,
                                    "q30_bases":240993,
                                    "q20_rate":0.985295,
                                    "q30_rate":0.959088,
                                    "read1_mean_length":125,
                                    "read2_mean_length":124,
                                    "gc_content":0.468884
                            }
                    },
                    "filtering_result": {
                            "passed_filter_reads": 2012,
                            "low_quality_reads": 0,
                            "too_many_N_reads": 0,
                            "too_short_reads": 0,
                            "too_long_reads": 0
                    }
            }
            """
        )

    # Create decontam directory and subdirectories
    decontam_dir = f"{results_dir}/{sample_accession}/decontam-stats"
    logger.info(f"Creating dummy Decontam results at {decontam_dir}")

    host_dir = f"{decontam_dir}/host"
    os.makedirs(host_dir, exist_ok=True)
    with open(
        f"{host_dir}/{sample_accession}_short_read_host_all_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )
    with open(
        f"{host_dir}/{sample_accession}_short_read_host_mapped_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )
    with open(
        f"{host_dir}/{sample_accession}_short_read_host_unmapped_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )

    phix_dir = f"{decontam_dir}/phix"
    os.makedirs(phix_dir, exist_ok=True)
    with open(
        f"{phix_dir}/{sample_accession}_short_read_phix_all_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )
    with open(
        f"{phix_dir}/{sample_accession}_short_read_phix_mapped_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )
    with open(
        f"{phix_dir}/{sample_accession}_short_read_phix_unmapped_summary_stats.txt", "w"
    ) as f:
        f.write(
            dedent(
                """\
                # This file was produced by samtools stats (1.21+htslib-1.21) and can be plotted using plot-bamstats
                # This file contains statistics for all reads.
                # The command line was:  stats test_sample.bam
                # CHK, Checksum [2]Read Names   [3]Sequences    [4]Qualities
                # CHK, CRC32 of reads which passed filtering followed by addition (32bit overflow)
                CHK	45671ef4	e06d1a38	571d0a18
                # Summary Numbers. Use `grep ^SN | cut -f 2-` to extract this part.
                SN	raw total sequences:	2012	# excluding supplementary and secondary reads
                SN	filtered sequences:	0
                SN	sequences:	2012
                SN	is sorted:	0
                SN	1st fragments:	1006
                SN	last fragments:	1006
                SN	reads mapped:	53
                SN	reads mapped and paired:	50	# paired-end technology bit set + both mates mapped
                SN	reads unmapped:	1959
                SN	reads properly paired:	50	# proper-pair bit set
                SN	reads paired:	2012	# paired-end technology bit set
                # First Fragment Qualities. Use `grep ^FFQ | cut -f 2-` to extract this part.
                # Columns correspond to qualities and rows to cycles. First column is the cycle number.
                FFQ	1	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	171	0	0	0	1	0	0	52	0	15	43	1	75	81	228	272	48	2	1	2	14	0
                """
            )
        )

    multiqc_dir = f"{results_dir}/{sample_accession}/multiqc"
    logger.info(f"Creating dummy sample multiqc results at {multiqc_dir}")
    os.makedirs(multiqc_dir, exist_ok=True)
    with open(f"{multiqc_dir}/{sample_accession}_multiqc_report.html", "wt") as f:
        f.write(
            dedent(
                """\
                <html>
                <body>
                <h1>MultiQC report</h1>
                Looks good to me!
                </body>
                </html>
                """
            )
        )


def generate_fake_rawreads_pipeline_summary_results(results_dir):
    """
    Generate fake raw-reads pipeline results for testing.

    Based on the directory structure provided in the issue description.

    :param results_dir: Directory to create the fake results in
    """

    logger = logging.getLogger("generate_dummy_summary_data_debug")

    os.makedirs(results_dir, exist_ok=True)

    # Create multiqc directory and subdirectories
    study_multiqc_dir = f"{results_dir}/multiqc"
    logger.info(f"Creating dummy study multiqc results at {study_multiqc_dir}")
    os.makedirs(study_multiqc_dir, exist_ok=True)
    with open(f"{study_multiqc_dir}/multiqc_report.html", "wt") as f:
        f.write(
            dedent(
                """\
                <html>
                <body>
                <h1>MultiQC report</h1>
                Looks good to me!
                </body>
                </html>
                """
            )
        )


MockFileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty (unit test mock)",
    test=lambda f: True,  # allows empty files created by mocks
)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analyse_study_tasks.make_samplesheet_rawreads.queryset_hash")
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.rawreads.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_rawreads_study"], indirect=True
)
def test_prefect_analyse_rawreads_flow(
    mock_queryset_hash_for_rawreads,
    prefect_harness,
    httpx_mock,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_ena_study,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for assembly runs and launch it with samplesheet.
    One assembly has all results, one assembly failed
    """
    mock_queryset_hash_for_rawreads.return_value = "abc123"

    study_accession = "ERP136383"
    all_results = ["ERR10889188", "ERR10889197", "ERR10889214", "ERR10889221"]

    # mock ENA response
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_title%2Csecondary_study_accession"
        f"&limit=10"
        f"&format=json"
        f"&dataPortal=metagenome",
        json=[
            {
                "study_accession": study_accession,
                "secondary_study_accession": study_accession,
                "study_title": "Infant stool metagenomic datasets",
            },
        ],
        is_reusable=True,
        is_optional=True,
    )
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_accession"
        f"&limit="
        f"&format=json"
        f"&dataPortal=metagenome",
        json=[{"study_accession": study_accession}],
        is_reusable=True,
        is_optional=True,
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22%28%28study_accession={study_accession}+OR+secondary_study_accession={study_accession}%29%20AND%20library_strategy=WGS%29%22"
        f"&limit=10000"
        f"&format=json"
        f"&fields=run_accession%2Csample_accession%2Csample_title%2Csecondary_sample_accession%2Cfastq_md5%2Cfastq_ftp%2Clibrary_layout%2Clibrary_strategy%2Clibrary_source%2Cscientific_name%2Chost_tax_id%2Chost_scientific_name%2Cinstrument_platform%2Cinstrument_model%2Clocation%2Clat%2Clon"
        f"&dataPortal=metagenome",
        json=[
            {
                "run_accession": "ERR10889188",
                "sample_accession": "SAMEA112437737",
                "sample_title": "stool",
                "secondary_sample_accession": "ERS14548047",
                "fastq_md5": "67613b1159c7d80eb3e3ca2479650ad5;6e6c5b0db3919b904a5e283827321fb9;41dc11f68009af8bc1e955a2c8d95d05",
                "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "human gut metagenome",
                "host_tax_id": "",
                "host_scientific_name": "",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina HiSeq 2500",
                "location": "19.754234 S 30.156915 E",
                "lat": "-19.754234",
                "lon": "30.156915",
            },
            {
                "run_accession": "ERR10889197",
                "sample_accession": "SAMEA112437712",
                "sample_title": "stool",
                "secondary_sample_accession": "ERS14548022",
                "fastq_md5": "323cd88dc2b3ccd17b47d8581a2be280;265ec70078cbdf9e92acd0ebf45aed8d;9402981ef0e93ea329bda73eb779d65c",
                "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "human gut metagenome",
                "host_tax_id": "",
                "host_scientific_name": "",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina HiSeq 2500",
                "location": "19.754234 S 30.156915 E",
                "lat": "-19.754234",
                "lon": "30.156915",
            },
            {
                "run_accession": "ERR10889214",
                "sample_accession": "SAMEA112437729",
                "sample_title": "stool",
                "secondary_sample_accession": "ERS14548039",
                "fastq_md5": "4dd2c869a5870984af3147cd8c34cca2;edc7298b1598848a71583718ff54ba8d;2aeeea4a32fae3318baff8bc7b3bc70c",
                "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "human gut metagenome",
                "host_tax_id": "",
                "host_scientific_name": "",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina HiSeq 2500",
                "location": "19.754234 S 30.156915 E",
                "lat": "-19.754234",
                "lon": "30.156915",
            },
            {
                "run_accession": "ERR10889221",
                "sample_accession": "SAMEA112437721",
                "sample_title": "stool",
                "secondary_sample_accession": "ERS14548031",
                "fastq_md5": "700d1ace4b6142dd32935039fb457618;3afa250c864c960324d424c523673f5f;a585727360a76bb22a948c30c5c41dd1",
                "fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "WGS",
                "library_source": "METAGENOMIC",
                "scientific_name": "human gut metagenome",
                "host_tax_id": "",
                "host_scientific_name": "",
                "instrument_platform": "ILLUMINA",
                "instrument_model": "Illumina HiSeq 2500",
                "location": "19.754234 S 30.156915 E",
                "lat": "-19.754234",
                "lon": "30.156915",
            },
        ],
        is_reusable=True,
        is_optional=True,
    )

    # create fake results
    summary_folder = Path(f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6")
    summary_folder.mkdir(exist_ok=True, parents=True)
    generate_fake_rawreads_pipeline_summary_results(summary_folder)

    rawreads_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_rawreads_v6/abc123"
    )
    rawreads_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{rawreads_folder}/{EMG_CONFIG.rawreads_pipeline.completed_runs_csv}",
        "w",
    ) as file:
        for r in all_results:
            file.write(f"{r},all_results" + "\n")

    # Generate fake pipeline results for the successful assembly
    for r in all_results:
        generate_fake_rawreads_pipeline_results(rawreads_folder, r)

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]
        library_strategy_policy: Optional[ENALibraryStrategyPolicy]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices["root.engineered"],
                watchers=[UserChoices[admin_user.username]],
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # RUN MAIN FLOW
    analysis_rawreads_study(study_accession=study_accession)

    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()
    mock_suspend_flow_run.assert_called()

    # Check samplesheet
    samplesheet_table = Artifact.get("rawreads-v6-initial-sample-sheet")
    assert samplesheet_table.type == "table"
    table_data = json.loads(samplesheet_table.data)
    assert len(table_data) == len(all_results)
    assert table_data[0]["sample"] in all_results

    # check biome and watchers were set correctly
    study = analyses.models.Study.objects.get_or_create_for_ena_study(study_accession)
    assert study.biome.biome_name == "Engineered"
    assert admin_user == study.watchers.first()

    # Check that the study has v6 analyses
    study.refresh_from_db()
    assert study.features.has_v6_analyses

    # Check all analyses were added to database
    assert (
        sum(
            [
                analyses.models.Analysis.objects.filter(
                    run__ena_accessions__contains=[r]
                ).count()
                for r in all_results
            ]
        )
        == 4
    )

    # Check taxonomic and functional annotations
    analysis_which_should_have_annotations_imported: analyses.models.Analysis = (
        analyses.models.Analysis.objects_and_annotations.get(
            run__ena_accessions__contains=[all_results[0]]
        )
    )

    assert (
        analyses.models.Analysis.TAXONOMIES
        in analysis_which_should_have_annotations_imported.annotations
    )
    assert (
        analyses.models.Analysis.TaxonomySources.SSU.value
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.LSU.value
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.MOTUS.value
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.SSU.value]
    assert len(test_annotation) == 2
    assert (
        test_annotation[0]["organism"]
        == "sk__Bacteria;k__;p__Actinobacteria;c__Actinobacteria;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium_breve"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.LSU.value]
    assert len(test_annotation) == 2
    assert (
        test_annotation[1]["organism"]
        == "sk__Bacteria;k__;p__Actinobacteria;c__Actinobacteria;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium_longum"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.MOTUS.value]
    assert len(test_annotation) == 3
    assert (
        test_annotation[0]["organism"]
        == "k__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales;f__Lactobacillaceae;g__Lactobacillus;s__Lactobacillus gasseri"
    )

    assert (
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        in analysis_which_should_have_annotations_imported.annotations
    )
    assert (
        analyses.models.Analysis.FunctionalSources.PFAM.value
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
    ][analyses.models.Analysis.FunctionalSources.PFAM.value]
    assert len(test_annotation) == 3
    logger = logging.getLogger("test_logger")
    logger.info(test_annotation)
    assert test_annotation["read_count"][2]["function"] == "PF10417.14"
    assert test_annotation["coverage_depth"][1]["coverage_depth"] == 3.835820895522388
    assert (
        test_annotation["coverage_breadth"][3]["coverage_breadth"] == 0.807909604519774
    )

    # Check files
    workdir = Path(f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6")
    assert workdir.is_dir()
    assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"
