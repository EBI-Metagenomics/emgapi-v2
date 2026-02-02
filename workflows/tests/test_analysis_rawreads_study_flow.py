from textwrap import dedent
import json
import gzip
import logging
import os
import re
import shutil
import glob
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union
from unittest.mock import Mock, patch

import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

from workflows.data_io_utils.file_rules.base_rules import GlobRule

import analyses.models
import analyses.base_models.with_experiment_type_models
from workflows.data_io_utils.file_rules.base_rules import FileRule
from workflows.data_io_utils.file_rules.common_rules import GlobHasFilesCountRule
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_study_summaries,
    STUDY_SUMMARY_TSV,
)
from workflows.flows.analysis_rawreads_study import analysis_rawreads_study
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
    run_flow_and_capture_logs,
)
from workflows.flows.analyse_study_tasks.cleanup_pipeline_directories import (
    # delete_study_results_dir,
    delete_study_nextflow_workdir,
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

    # Create multiqc directory and subdirectories
    logger.info(f"Creating dummy study multiqc results at {results_dir}")
    with open(f"{results_dir}/study_multiqc_report.html", "wt") as f:
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

    # Create function-summary directory and subdirectories
    func_dir = f"{results_dir}/{sample_accession}/function-summary"
    logger.info(f"Creating dummy functional results at {func_dir}")
    pfam_dir = f"{func_dir}/pfam"
    os.makedirs(pfam_dir, exist_ok=True)
    with gzip.open(f"{pfam_dir}/{sample_accession}_pfam.txt.gz", "wb") as f:
        f.write(
            dedent(
                """\
                # function	read_count	coverage_depth	coverage_breadth
                PF21175.2	1	0.9583333333333334	0.9583333333333334
                PF10418.14	1	0.926829268292683	0.926829268292683
                PF17802.7	1	0.7692307692307693	0.7692307692307693
                PF17769.7	1	0.7142857142857143	0.7142857142857143
                PF10531.15	1	0.6909090909090909	0.6909090909090909
                PF22269.2	1	0.6612903225806451	0.6612903225806451
                PF13411.12	1	0.6376811594202898	0.6376811594202898
                PF00515.34	1	0.5882352941176471	0.5882352941176471
                PF16320.10	1	0.5625	0.5625
                PF13186.11	1	0.5522388059701493	0.5522388059701493
                PF16124.10	1	0.5303030303030303	0.5303030303030303
                PF13807.11	1	0.47560975609756095	0.47560975609756095
                PF22811.2	1	0.4523809523809524	0.4523809523809524
                PF01782.24	1	0.4523809523809524	0.4523809523809524
                PF00009.33	2	0.4148936170212766	0.32978723404255317
                PF00005.33	2	0.40145985401459855	0.40145985401459855
                PF08428.16	1	0.39473684210526316	0.39473684210526316
                PF00679.30	1	0.3707865168539326	0.3707865168539326
                PF00448.28	2	0.3673469387755102	0.3673469387755102
                PF06755.17	1	0.35714285714285715	0.35714285714285715
                PF10800.13	1	0.34615384615384615	0.34615384615384615
                PF02922.24	1	0.3373493975903614	0.3373493975903614
                PF00472.26	1	0.33620689655172414	0.33620689655172414
                PF23139.1	1	0.32894736842105265	0.32894736842105265
                PF18818.7	1	0.30952380952380953	0.30952380952380953
                PF21018.2	1	0.2727272727272727	0.2727272727272727
                PF14284.11	1	0.272108843537415	0.272108843537415
                PF13288.12	1	0.2608695652173913	0.2608695652173913
                PF00308.24	1	0.24691358024691357	0.24691358024691357
                PF12978.13	1	0.24528301886792453	0.24528301886792453
                PF00724.26	2	0.2309941520467836	0.14912280701754385
                PF19306.5	1	0.2261904761904762	0.2261904761904762
                PF06924.17	1	0.22598870056497175	0.22598870056497175
                PF02397.22	1	0.22346368715083798	0.22346368715083798
                PF03816.19	1	0.20666666666666667	0.20666666666666667
                PF00849.27	1	0.19736842105263158	0.19736842105263158
                PF13614.12	1	0.1864406779661017	0.1864406779661017
                PF09985.14	1	0.17105263157894737	0.17105263157894737
                PF01435.24	1	0.16666666666666666	0.16666666666666666
                PF03796.21	1	0.1568627450980392	0.1568627450980392
                PF17657.6	1	0.1566265060240964	0.1566265060240964
                PF00814.31	1	0.15151515151515152	0.15151515151515152
                PF03613.19	1	0.1509433962264151	0.1509433962264151
                PF04898.20	1	0.1444043321299639	0.1444043321299639
                PF11997.14	1	0.1417910447761194	0.1417910447761194
                PF02601.20	1	0.12698412698412698	0.12698412698412698
                PF02896.24	1	0.12627986348122866	0.12627986348122866
                PF00393.24	1	0.11724137931034483	0.11724137931034483
                PF01702.25	1	0.10644257703081232	0.10644257703081232
                PF01041.23	1	0.10277777777777777	0.10277777777777777
                PF05649.18	1	0.10236220472440945	0.10236220472440945
                PF06965.17	1	0.10160427807486631	0.10160427807486631
                PF00478.31	1	0.10144927536231885	0.10144927536231885
                PF00860.26	1	0.10025706940874037	0.10025706940874037
                PF07971.18	1	0.09051724137931035	0.09051724137931035
                PF12979.12	1	0.08882521489971347	0.08882521489971347
                PF00330.25	1	0.08855291576673865	0.08855291576673865
                PF01425.27	1	0.08764044943820225	0.08764044943820225
                PF00171.27	1	0.08676789587852494	0.08676789587852494
                PF02652.20	1	0.07279693486590039	0.07279693486590039
                PF13597.11	1	0.060498220640569395	0.060498220640569395
                PF02901.20	1	0.06027820710973725	0.06027820710973725
                PF09586.16	1	0.045508982035928146	0.045508982035928146
                """
            ).encode()
        )
    os.makedirs(f"{pfam_dir}", exist_ok=True)
    with open(f"{pfam_dir}/{sample_accession}_pfam.stats.json", "wt") as f:
        f.write(r'{"reads_mapped": 67, "hmm_count": 63, "read_hit_count": 67}')

    # Create taxonomy-summary directory and subdirectories
    tax_dir = f"{results_dir}/{sample_accession}/taxonomy-summary"
    logger.info(f"Creating dummy taxonomy results at {tax_dir}")

    # mOTUs
    motus_dir = f"{tax_dir}/motus"
    os.makedirs(motus_dir, exist_ok=True)
    with gzip.open(f"{motus_dir}/{sample_accession}_motus.txt.gz", "wb") as f:
        f.write(
            dedent(
                """\
                # Count	Kingdom	Phylum	Class	Order	Family	Genus	Species
                1.0	k__Bacteria	p__Firmicutes	c__Bacilli	o__Lactobacillales	f__Lactobacillaceae	g__Lactobacillus	s__Lactobacillus gasseri
                2.0	k__Bacteria	p__Actinobacteria	c__Actinobacteria	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium longum [Bifidobacterium longum CAG:69/Bifidobacterium longum]
                1.0	k__Bacteria	p__Bacteroidetes	c__Bacteroidia	o__Bacteroidales	f__Bacteroidaceae	g__Bacteroides	s__Bacteroides thetaiotaomicron
                2.0	unassigned
                """
            ).encode()
        )
    with open(f"{motus_dir}/{sample_accession}_motus.html", "w"):
        pass

    # SILVA-SSU
    silvassu_dir = f"{tax_dir}/silva-ssu"
    os.makedirs(silvassu_dir, exist_ok=True)
    with gzip.open(f"{silvassu_dir}/{sample_accession}_silva-ssu.txt.gz", "wb") as f:
        f.write(
            dedent(
                """\
                # Count	Superkingdom	Kingdom	Phylum	Class	Order	Family	Genus	Species
                1	sk__Bacteria	k__	p__Actinomycetota
                1	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes
                1	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Bifidobacteriales
                17	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium
                1	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_breve
                3	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Bifidobacteriales	f__Bifidobacteriaceae	g__Bifidobacterium	s__Bifidobacterium_longum
                2	sk__Bacteria	k__	p__Bacillota
                1	sk__Bacteria	k__	p__Bacillota	c__Bacilli
                1	sk__Bacteria	k__	p__Bacillota	c__Bacilli	o__Lactobacillales	f__Lactobacillaceae
                2	sk__Bacteria	k__	p__Bacillota	c__Bacilli	o__Lactobacillales	f__Streptococcaceae	g__Streptococcus
                3	sk__Bacteria	k__	p__Bacillota	c__Clostridia	o__Eubacteriales	f__Lachnospiraceae
                1	sk__Bacteria	k__	p__Bacillota	c__Clostridia	o__Eubacteriales	f__Lachnospiraceae	g__Anaerostipes
                2	sk__Bacteria	k__	p__Bacillota	c__Erysipelotrichia
                2	sk__Bacteria	k__	p__Bacteroidota	c__Bacteroidia	o__Bacteroidales	f__Bacteroidaceae	g__Bacteroides
                1	sk__Bacteria	k__	p__Bacteroidota	c__Bacteroidia	o__Bacteroidales	f__Prevotellaceae
                2	sk__Bacteria	k__	p__Pseudomonadota	c__Gammaproteobacteria	o__Enterobacterales
                2	sk__Bacteria	k__	p__Pseudomonadota	c__Gammaproteobacteria	o__Enterobacterales	f__Enterobacteriaceae
                """
            ).encode()
        )
    with open(f"{silvassu_dir}/{sample_accession}_silva-ssu.html", "w"):
        pass

    # SILVA-LSU
    silvalsu_dir = f"{tax_dir}/silva-lsu"
    os.makedirs(silvalsu_dir, exist_ok=True)
    with gzip.open(f"{silvalsu_dir}/{sample_accession}_silva-lsu.txt.gz", "wb") as f:
        f.write(
            dedent(
                """\
                # Count	Superkingdom	Kingdom	Phylum	Class	Order	Family	Genus	Species
                12	sk__Bacteria
                1	sk__Bacteria	k__	p__Bacillota	c__Bacilli	o__Lactobacillales	f__Streptococcaceae	g__Streptococcus	s__Streptococcus_pneumoniae
                1	sk__Bacteria	k__	p__Bacillota	c__Clostridia	o__Eubacteriales
                1	sk__Bacteria	k__	p__Bacteroidota	c__Flavobacteriia	o__Flavobacteriales	f__Flavobacteriaceae
                1	sk__Bacteria	k__	p__Bacteroidota	c__Flavobacteriia	o__Flavobacteriales	f__Flavobacteriaceae	g__Myroides	s__Myroides_odoratimimus
                4	sk__Bacteria	k__	p__Pseudomonadota	c__Gammaproteobacteria	o__Pseudomonadales
                1	sk__Bacteria	k__	p__Pseudomonadota	c__Gammaproteobacteria	o__Pseudomonadales	f__Pseudomonadaceae
                5	sk__Eukaryota	k__Metazoa
                5	sk__Eukaryota	k__Viridiplantae
                1	sk__Eukaryota	k__Viridiplantae	p__Streptophyta	c__Magnoliopsida	o__Fabales
                """
            ).encode()
        )
    with open(f"{silvalsu_dir}/{sample_accession}_silva-lsu.html", "w"):
        pass

    # Create qc directory and subdirectories
    qc_dir = f"{results_dir}/{sample_accession}/qc"
    logger.info(f"Creating dummy QC results at {qc_dir}")
    os.makedirs(qc_dir, exist_ok=True)
    with open(f"{qc_dir}/{sample_accession}_decontamination.fastp.json", "wt") as f:
        f.write(
            dedent(
                """\
                {
                    "summary": {
                        "fastp_version": "0.24.0",
                        "sequencing": "paired end (125 cycles + 125 cycles)",
                        "before_filtering": {
                            "total_reads":19874,
                            "total_bases":2451997,
                            "q20_bases":2420132,
                            "q30_bases":2362697,
                            "q20_rate":0.987004,
                            "q30_rate":0.963581,
                            "read1_mean_length":122,
                            "read2_mean_length":123,
                            "gc_content":0.482007
                        },
                        "after_filtering": {
                            "total_reads":19618,
                            "total_bases":2435089,
                            "q20_bases":2403364,
                            "q30_bases":2346196,
                            "q20_rate":0.986972,
                            "q30_rate":0.963495,
                            "read1_mean_length":124,
                            "gc_content":0.482265
                        }
                    },
                    "filtering_result": {
                        "passed_filter_reads": 19874,
                        "corrected_reads": 3,
                        "corrected_bases": 4,
                        "low_quality_reads": 0,
                        "too_many_N_reads": 0,
                        "too_short_reads": 0,
                        "too_long_reads": 0
                    }
                }
                """
            )
        )

    with open(f"{qc_dir}/{sample_accession}_qc.fastp.json", "wt") as f:
        f.write(
            dedent(
                """\
                {
                    "summary": {
                        "fastp_version": "0.24.0",
                        "sequencing": "paired end (125 cycles + 125 cycles)",
                        "before_filtering": {
                            "total_reads":20000,
                            "total_bases":2482860,
                            "q20_bases":2450508,
                            "q30_bases":2392136,
                            "q20_rate":0.98697,
                            "q30_rate":0.96346,
                            "read1_mean_length":123,
                            "read2_mean_length":124,
                            "gc_content":0.48317
                        },
                        "after_filtering": {
                            "total_reads":19884,
                            "total_bases":2453236,
                            "q20_bases":2421359,
                            "q30_bases":2363901,
                            "q20_rate":0.987006,
                            "q30_rate":0.963585,
                            "read1_mean_length":122,
                            "read2_mean_length":123,
                            "gc_content":0.481981
                        }
                    },
                    "filtering_result": {
                        "passed_filter_reads": 19884,
                        "low_quality_reads": 0,
                        "too_many_N_reads": 0,
                        "too_short_reads": 116,
                        "too_long_reads": 0
                    }
                }
                """
            )
        )

    with open(f"{qc_dir}/{sample_accession}_multiqc_report.html", "wt") as f:
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


def simulate_copy_results(
    source: Path, target: Path, allowed_extensions: Union[set, list], logger=None
):
    for fp in source.rglob("**/*"):
        # study summaries
        if any(
            [
                re.match(f"PRJ.*{STUDY_SUMMARY_TSV}", fp.name),
                re.match(f"[DES]RP.*{STUDY_SUMMARY_TSV}", fp.name),
            ]
        ):
            target_ = (
                target
                / "study-summaries"
                / f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
                / fp.name
            )
            if logger:
                logger.info(f"Copying summary file {fp} to {target_}.")
            target_.parent.mkdir(exist_ok=True)
            shutil.copyfile(fp, target_)
            continue

        # analysis results
        if fp.name.split(".")[-1] in allowed_extensions:
            target_ = target / fp.relative_to(source)
            if logger:
                logger.info(f"Copying analysis results file {fp} to {target_}.")
            target_.parent.mkdir(exist_ok=True)
            shutil.copyfile(fp, target_)
            continue


MockFileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty (unit test mock)",
    test=lambda f: True,  # allows empty files created by mocks
)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analyse_study_tasks.raw_reads.run_rawreads_pipeline_via_samplesheet.queryset_hash"
)
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.rawreads.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_rawreads_study"], indirect=True
)
def test_prefect_analyse_rawreads_flow(
    mock_run_deployment,
    mock_queryset_hash_for_rawreads,
    prefect_harness,
    httpx_mock,
    ena_any_sample_metadata,
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
    # Mock run_deployment to prevent actual deployment execution
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    samplesheet_hash = "xyz789"
    mock_queryset_hash_for_rawreads.return_value = samplesheet_hash

    study_accession = "ERP136384"
    all_results = ["ERR10889189", "ERR10889198", "ERR10889215", "ERR10889222"]

    # mock ENA response
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name"
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
                "run_accession": "ERR10889189",
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
                "run_accession": "ERR10889198",
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
                "run_accession": "ERR10889215",
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
                "run_accession": "ERR10889222",
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
    rawreads_folder = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / Path(study_accession)
        / Path(
            f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
        )
        / Path(samplesheet_hash)
    )
    rawreads_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{rawreads_folder}/{EMG_CONFIG.rawreads_pipeline.completed_runs_csv}",
        "w",
    ) as file:
        for r in all_results:
            file.write(f"{r},all_results" + "\n")

    # Generate fake pipeline results for the successful raw-reads run
    for r in all_results:
        generate_fake_rawreads_pipeline_results(rawreads_folder, r)

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]
        library_strategy_policy: Optional[ENALibraryStrategyPolicy]
        webin_owner: Optional[str]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices["root.engineered"],
                watchers=[UserChoices[admin_user.username]],
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
                webin_owner=None,
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

    # check completed runs (all runs in completed list - might contain sanity check not passed as well)
    assert study.analyses.filter(status__analysis_completed=True).count() == 4

    assert (
        study.analyses.filter(status__analysis_completed_reason="all_results").count()
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
        analyses.models.Analysis.TaxonomySources.SSU
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.LSU
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.MOTUS
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.SSU]
    assert len(test_annotation) == 17
    assert (
        test_annotation[4]["organism"]
        == "sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium_breve"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.LSU]
    assert len(test_annotation) == 10
    assert (
        test_annotation[1]["organism"]
        == "sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Streptococcaceae;g__Streptococcus;s__Streptococcus_pneumoniae"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.MOTUS]
    assert len(test_annotation) == 4
    assert (
        test_annotation[0]["organism"]
        == "k__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales;f__Lactobacillaceae;g__Lactobacillus;s__Lactobacillus gasseri"
    )

    assert (
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        in analysis_which_should_have_annotations_imported.annotations
    )
    assert (
        analyses.models.Analysis.FunctionalSources.PFAM
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
    ][analyses.models.Analysis.FunctionalSources.PFAM]
    assert len(test_annotation["read_count"]) == 63
    assert test_annotation["read_count"][2]["function"] == "PF17802.7"
    assert test_annotation["coverage_depth"][1]["coverage_depth"] == 0.926829268292683
    assert (
        test_annotation["coverage_breadth"][3]["coverage_breadth"] == 0.7142857142857143
    )

    # Check files
    workdir = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / Path(study_accession)
        / Path(
            f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
        )
    )
    assert workdir.is_dir()
    assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"

    # Check summaries
    summary_dir = (
        study.results_dir_path
        / f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
    )
    Directory(
        path=summary_dir,
        glob_rules=[GlobHasFilesCountRule[8]],  # 6 study summaries, 2 directories
    )

    with (
        summary_dir / "summaries" / f"{samplesheet_hash}_motus_study_summary.tsv"
    ).open("r") as summary:
        lines = summary.readlines()
        assert (
            lines[0] == "taxonomy\tERR10889189\tERR10889198\tERR10889215\tERR10889222\n"
        )
        assert (
            lines[1]
            == "k__Bacteria;p__Actinobacteria;c__Actinobacteria;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium longum [Bifidobacterium longum CAG:69/Bifidobacterium longum]\t2\t2\t2\t2\n"
        )
        assert "g__Lactobacillus" in lines[-2]

    # manually remove the merged study summaries
    for file in Path(summary_dir).glob(f"{study.first_accession}*"):
        file.unlink()

    # test merging of study summaries again, with cleanup disabled
    merge_study_summaries(
        mgnify_study_accession=study.accession,
        cleanup_partials=False,
        analysis_type="rawreads",
    )
    Directory(
        path=summary_dir,
        glob_rules=[
            GlobHasFilesCountRule[
                8
            ],  # study ones generated, and partials left in place
            GlobRule(
                rule_name="All study level files are present",
                glob_pattern=f"{study.first_accession}*{STUDY_SUMMARY_TSV}",
                test=lambda f: len(list(f)) == 6,
            ),
        ],
    )

    study.refresh_from_db()
    assert len(study.downloads_as_objects) == 6

    # test merging of study summaries again – expect default bludgeon should overwrite the existing ones
    logged_run = run_flow_and_capture_logs(
        merge_study_summaries,
        mgnify_study_accession=study.accession,
        cleanup_partials=True,
        analysis_type="rawreads",
    )
    assert (
        logged_run.logs.count(
            f"Deleting {str(Path(summary_dir) / study.first_accession)}"
        )
        == 6
    )
    Directory(
        path=summary_dir,
        glob_rules=[
            GlobHasFilesCountRule[8],  # partials deleted, just merged ones
            GlobRule(
                rule_name="All files are study level",
                glob_pattern=f"{study.first_accession}*{STUDY_SUMMARY_TSV}",
                test=lambda f: len(list(f)) == 6,
            ),
        ],
    )

    study.refresh_from_db()
    assert len(study.downloads_as_objects) == 6
    assert study.features.has_v6_analyses

    # simulate copy_v6_pipeline_results and copy_v6_summaries
    study.external_results_dir = (
        f"/tmp/external/{study_accession[:-3]}/{study_accession}"
    )
    study.save()

    logger = logging.getLogger("simulate_copy_results")

    source = study.results_dir_path
    target = Path(study.external_results_dir)

    # test case where not everything is copied
    allowed_extensions = {
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "gz",
        "fasta",
        "csv",
        "deoverlapped",
    }
    simulate_copy_results(source, target, allowed_extensions, logger=logger)

    # run deleting
    # delete_study_nextflow_workdir(study_workdir, analyses_to_attempt)
    # delete_study_results_dir(study.results_dir_path, study)

    # check files
    assert study.results_dir_path.is_dir()

    # test case where everything is copied
    allowed_extensions = {
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "json",
        "gz",
        "fasta",
        "csv",
        "deoverlapped",
    }
    simulate_copy_results(source, target, allowed_extensions, logger=logger)

    # run deleting
    # delete_study_results_dir(study.results_dir_path, study)

    # check files
    # assert not study.results_dir_path.is_dir()

    n = len(list(glob.glob(f"{study.external_results_dir}/**/*", recursive=True)))
    logger.info(
        f"External results directory {study.external_results_dir} has {n} files."
    )
    Directory(
        path=Path(study.external_results_dir),
        glob_rules=[
            GlobRule(
                rule_name="Recursive number of files",
                glob_pattern="**/*",
                test=lambda x: len(list(x)) == 96,
            )
        ],
    )

    # test Nextflow workdir cleanup
    nextflow_workdir = (
        Path("/tmp/work")
        / Path(study_accession)
        / Path(
            f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
        )
        / samplesheet_hash
    )
    # generate plausible nextflow directories
    os.makedirs(nextflow_workdir / "h5" / "h5scdo9q3u4nefpsldkfvubqp349", exist_ok=True)
    assert nextflow_workdir.is_dir()

    analyses_to_attempt = study.analyses.order_by("id").values_list("id", flat=True)
    # set one of the analyses to incomplete
    analysis_obj = analyses.models.Analysis.objects.get(pk=analyses_to_attempt[0])
    analysis_obj.mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_FAILED, True
    )

    delete_study_nextflow_workdir(nextflow_workdir, analyses_to_attempt)
    assert nextflow_workdir.is_dir()

    # # set it to complete
    analysis_obj.mark_status(
        analyses.models.Analysis.AnalysisStates.ANALYSIS_FAILED, False
    )
    delete_study_nextflow_workdir(nextflow_workdir, analyses_to_attempt)
    assert not nextflow_workdir.is_dir()


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analyse_study_tasks.raw_reads.run_rawreads_pipeline_via_samplesheet.queryset_hash"
)
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.rawreads.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_rawreads_study"], indirect=True
)
def test_prefect_analyse_rawreads_flow_private_data(
    mock_run_deployment,
    mock_queryset_hash_for_rawreads,
    prefect_harness,
    httpx_mock,
    ena_any_sample_metadata,
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
    # Mock run_deployment to prevent actual deployment execution
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    samplesheet_hash = "abc123"
    mock_queryset_hash_for_rawreads.return_value = samplesheet_hash

    study_accession = "ERP136383"
    all_results = ["ERR10889188", "ERR10889197", "ERR10889214", "ERR10889221"]

    # mock ENA response
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name"
        f"&limit=10"
        f"&format=json"
        f"&dataPortal=metagenome",
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
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
        f"&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name"
        f"&limit=10"
        f"&format=json"
        f"&dataPortal=metagenome",
        match_headers={},  # public call should not find private study
        json=[],
        is_reusable=True,
        is_optional=True,
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_title%2Cstudy_description%2Ccenter_name%2Csecondary_study_accession%2Cstudy_name"
        f"&limit=10"
        f"&format=json"
        f"&dataPortal=metagenome",
        match_headers={},  # public call should not find private study
        json=[],
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
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
        json=[{"study_accession": study_accession}],
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
        match_headers={},  # public call should not find private study
        json=[],
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
        match_headers={},  # public call should not find private study
        json=[],
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
        match_headers={
            "Authorization": "Basic ZGNjX2Zha2U6bm90LWEtZGNjLXB3"
        },  # dcc_fake:not-a-dcc-pw
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
    rawreads_folder = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / Path(study_accession)
        / Path(
            f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
        )
        / Path(samplesheet_hash)
    )
    rawreads_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{rawreads_folder}/{EMG_CONFIG.rawreads_pipeline.completed_runs_csv}",
        "w",
    ) as file:
        for r in all_results:
            file.write(f"{r},all_results" + "\n")

    # Generate fake pipeline results for the successful raw-reads run
    for r in all_results:
        generate_fake_rawreads_pipeline_results(rawreads_folder, r)

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]
        library_strategy_policy: Optional[ENALibraryStrategyPolicy]
        webin_owner: Optional[str]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices["root.engineered"],
                watchers=[UserChoices[admin_user.username]],
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
                webin_owner="webin-1",
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
        analyses.models.Analysis.TaxonomySources.SSU
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.LSU
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    assert (
        analyses.models.Analysis.TaxonomySources.MOTUS
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.SSU]
    assert len(test_annotation) == 17
    assert (
        test_annotation[4]["organism"]
        == "sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium_breve"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.LSU]
    assert len(test_annotation) == 10
    assert (
        test_annotation[1]["organism"]
        == "sk__Bacteria;k__;p__Bacillota;c__Bacilli;o__Lactobacillales;f__Streptococcaceae;g__Streptococcus;s__Streptococcus_pneumoniae"
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.MOTUS]
    assert len(test_annotation) == 4
    assert (
        test_annotation[0]["organism"]
        == "k__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales;f__Lactobacillaceae;g__Lactobacillus;s__Lactobacillus gasseri"
    )

    assert (
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        in analysis_which_should_have_annotations_imported.annotations
    )
    assert (
        analyses.models.Analysis.FunctionalSources.PFAM
        in analysis_which_should_have_annotations_imported.annotations[
            analyses.models.Analysis.FUNCTIONAL_ANNOTATION
        ]
    )
    test_annotation = analysis_which_should_have_annotations_imported.annotations[
        analyses.models.Analysis.FUNCTIONAL_ANNOTATION
    ][analyses.models.Analysis.FunctionalSources.PFAM]
    assert len(test_annotation["read_count"]) == 63
    assert test_annotation["read_count"][2]["function"] == "PF17802.7"
    assert test_annotation["coverage_depth"][1]["coverage_depth"] == 0.926829268292683
    assert (
        test_annotation["coverage_breadth"][3]["coverage_breadth"] == 0.7142857142857143
    )

    # Check files
    workdir = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / Path(study_accession)
        / Path(
            f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
        )
    )
    assert workdir.is_dir()
    assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"

    # Check summaries
    summary_dir = (
        study.results_dir_path
        / f"{EMG_CONFIG.rawreads_pipeline.pipeline_name}_{EMG_CONFIG.rawreads_pipeline.pipeline_version}"
    )
    Directory(
        path=summary_dir,
        glob_rules=[GlobHasFilesCountRule[8]],  # 6 study summaries, 2 directories
    )

    with (
        summary_dir / "summaries" / f"{samplesheet_hash}_motus_study_summary.tsv"
    ).open("r") as summary:
        lines = summary.readlines()
        assert (
            lines[0] == "taxonomy\tERR10889188\tERR10889197\tERR10889214\tERR10889221\n"
        )
        assert (
            lines[1]
            == "k__Bacteria;p__Actinobacteria;c__Actinobacteria;o__Bifidobacteriales;f__Bifidobacteriaceae;g__Bifidobacterium;s__Bifidobacterium longum [Bifidobacterium longum CAG:69/Bifidobacterium longum]\t2\t2\t2\t2\n"
        )
        assert "g__Lactobacillus" in lines[-2]

    # manually remove the merged study summaries
    for file in Path(summary_dir).glob(f"{study.first_accession}*"):
        file.unlink()

    # test merging of study summaries again, with cleanup disabled
    merge_study_summaries(
        mgnify_study_accession=study.accession,
        cleanup_partials=False,
        analysis_type="rawreads",
    )
    Directory(
        path=summary_dir,
        glob_rules=[
            GlobHasFilesCountRule[
                8
            ],  # study ones generated, and partials left in place
            GlobRule(
                rule_name="All study level files are present",
                glob_pattern=f"{study.first_accession}*{STUDY_SUMMARY_TSV}",
                test=lambda f: len(list(f)) == 6,
            ),
        ],
    )

    study.refresh_from_db()
    assert len(study.downloads_as_objects) == 6

    # test merging of study summaries again – expect default bludgeon should overwrite the existing ones
    logged_run = run_flow_and_capture_logs(
        merge_study_summaries,
        mgnify_study_accession=study.accession,
        cleanup_partials=True,
        analysis_type="rawreads",
    )
    assert (
        logged_run.logs.count(
            f"Deleting {str(Path(summary_dir) / study.first_accession)}"
        )
        == 6
    )
    Directory(
        path=summary_dir,
        glob_rules=[
            GlobHasFilesCountRule[8],  # partials deleted, just merged ones
            GlobRule(
                rule_name="All files are study level",
                glob_pattern=f"{study.first_accession}*{STUDY_SUMMARY_TSV}",
                test=lambda f: len(list(f)) == 6,
            ),
        ],
    )

    study.refresh_from_db()
    assert len(study.downloads_as_objects) == 6
    assert study.features.has_v6_analyses

    assert study.is_private
    assert study.analyses.filter(is_private=True).count() == 4
    assert study.analyses.exclude(is_private=True).count() == 0
    assert study.runs.filter(is_private=True).count() == 4
    assert study.runs.exclude(is_private=True).count() == 0

    # Verify run_deployment was called for move operation with private results dir
    assert (
        mock_run_deployment.called
    ), "run_deployment should be called for move operation"

    # Check that at least one call used the private results directory
    move_to_private_found = False
    for call in mock_run_deployment.call_args_list:
        args, kwargs = call
        if "parameters" in kwargs and "target" in kwargs["parameters"]:
            target = kwargs["parameters"]["target"]
            if EMG_CONFIG.slurm.private_results_dir in target:
                move_to_private_found = True
                break

    assert (
        move_to_private_found
    ), "No move operation found targeting private results directory"
