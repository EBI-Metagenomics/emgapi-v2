"""
Shared fixtures and fake-pipeline-output generators for amplicon analysis tests.

The generators were moved here verbatim from
``workflows/tests/test_analysis_amplicon_study_flow.py`` so that the
characterisation tests for chunking, post-analysis state setting, importing and
the study-level tail can all build realistic output trees without importing from
a test module.
"""

import csv
import json
import os
from pathlib import Path
from textwrap import dedent
from typing import Mapping, Optional, Sequence

import django
import pytest
from django.conf import settings

from workflows.prefect_utils.testing_utils import write_empty_fasta_file

django.setup()

import analyses.models as mg_models
import ena.models as ena_models

EMG_CONFIG = settings.EMG_CONFIG


def generate_fake_pipeline_all_results(amplicon_run_folder: Path, run):
    # QC
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.qc_folder).mkdir(
        exist_ok=True, parents=True
    )
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.qc_folder
        / f"{run}_seqfu.tsv"
    ).touch()
    with (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.qc_folder
        / f"{run}.fastp.json"
    ).open("w") as fastp:
        json.dump({"summary": {"before_filtering": {"total_bases": 10}}}, fastp)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.qc_folder
        / f"{run}_multiqc_report.html"
    ).touch()

    # PRIMER IDENTIFICATION
    (
        amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.primer_identification_folder
    ).mkdir(exist_ok=True, parents=True)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.primer_identification_folder
        / f"{run}.cutadapt.json"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.primer_identification_folder
        / "fwd_primers.fasta"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.primer_identification_folder
        / "rev_primers.fasta"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.primer_identification_folder
        / f"{run}_primer_validation.tsv"
    ).touch()

    # AMPLIFIED REGION INFERENCE
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder
    ).mkdir(exist_ok=True, parents=True)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder
        / f"{run}.tsv"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder
        / f"{run}.16S.V3-V4.txt"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder
        / f"{run}.18S.V9.txt"
    ).touch()

    # ASV
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.asv_folder).mkdir(
        exist_ok=True, parents=True
    )
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.asv_folder / "16S-V3-V4").mkdir(
        exist_ok=True
    )
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.asv_folder / "18S-V9").mkdir(
        exist_ok=True
    )
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.asv_folder / "concat").mkdir(
        exist_ok=True
    )

    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.qc_folder
        / f"{run}_dada2_stats.tsv"
    ).touch()

    with open(
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.asv_folder
        / f"{run}_DADA2-SILVA_asv_tax.tsv",
        "w",
    ) as fw:
        fw.write(dedent("""\
        ASV	Superkingdom	Kingdom	Phylum	Class	Order	Family	Genus	Species
        seq_1	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Kitasatosporales	f__Streptomycetaceae	g__Streptomyces NA
        seq_2	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Microbacteriaceae    NA  NA
        seq_3	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Arthrobacter NA
        seq_4	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Pseudarthrobacter    NA
        seq_5	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Propionibacteriales	f__Nocardioidaceae	g__Aeromicrobium    NA
                """))

    with open(
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.asv_folder
        / f"{run}_DADA2-PR2_asv_tax.tsv",
        "w",
    ) as fw:
        fw.write(dedent("""\
        ASV	Domain	Supergroup	Division	Subdivision	Class	Order	Family	Genus	Species
        seq_1	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Chitinophagales	f__Chitinophagaceae	g__Chitinophaga	s__Chitinophaga_sp.
        seq_2	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Cytophagales	f__Spirosomaceae	g__Dyadobacter	s__Dyadobacter_sp.
        seq_3	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Flavobacteriales	f__Flavobacteriaceae	g__Flavobacterium	s__Flavobacterium_sp.
        seq_4	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Sphingobacteriales	f__Sphingobacteriaceae	g__Pedobacter	s__Pedobacter_sp.
        seq_5	d__Bacteria	sg__PANNAM	dv__Proteobacteria	sdv__Proteobacteria_X	c__Alphaproteobacteria	o__Caulobacterales	f__Caulobacteraceae	g__Caulobacter	s__Caulobacter_sp.
                """))

    with open(
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.asv_folder
        / f"{run}_asv_seqs.fasta",
        "w",
    ) as fw:
        fw.write(dedent("""\
        >seq_1
        TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCA
        >seq_2
        TGGGGAATCTTGCACAATGGGCGAAAGCCTGATGCA
        >seq_3
        TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCA
        >seq_4
        TCGAGAATTTTTCTCAATGGGGGAAACCCTGAAGGA
        >seq_5
        TGGGGAATCTTGCACAATGGACGAAAGTCTGATGCA
                """))

    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.asv_folder
        / f"{run}_asv_krona_counts.txt"
    ).touch()
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.asv_folder
        / "concat"
        / f"{run}_concat_asv_read_counts.tsv"
    ).touch()

    with (
        (
            amplicon_run_folder
            / EMG_CONFIG.amplicon_pipeline.asv_folder
            / "18S-V9"
            / f"{run}_18S-V9_asv_read_counts.tsv"
        ).open("w") as v9_tsv,
        (
            amplicon_run_folder
            / EMG_CONFIG.amplicon_pipeline.asv_folder
            / "16S-V3-V4"
            / f"{run}_16S-V3-V4_asv_read_counts.tsv"
        ).open("w") as v3v4_tsv,
    ):
        for tsv in [v9_tsv, v3v4_tsv]:
            tsv.write(dedent("""\
                    asv	count
                    seq_1	886
                    seq_10	1011
                    seq_100	160
                    """))

    # SEQUENCE CATEGORISATION
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder
    ).mkdir(exist_ok=True, parents=True)
    write_empty_fasta_file(
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder
        / f"{run}_SSU.fasta"
    )
    write_empty_fasta_file(
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder
        / f"{run}_ITS_rRNA.fasta"
    )
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder
        / f"{run}.tblout.deoverlapped"
    ).touch()
    with (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder
        / f"{run}_SSU_rRNA_bacteria.RF00177.fa"
    ).open("w") as ssu_rna_fasta:
        ssu_rna_fasta.write(dedent(f"""\
                >{run}.100-SSU_rRNA_archaea/5-421 100/1 merged_251_171
                ACTCGGTCTAAAGGGTCCGTAGCCGGTCCAGTAAGTCCCTGCTTAAATCCTACGGCTTAA
                """))

    # TAXONOMY SUMMARY
    (amplicon_run_folder / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder).mkdir(
        exist_ok=True, parents=True
    )
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "SILVA-SSU"
    ).mkdir(exist_ok=True)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "PR2"
    ).mkdir(exist_ok=True)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "DADA2-SILVA"
    ).mkdir(exist_ok=True)
    (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "DADA2-PR2"
    ).mkdir(exist_ok=True)

    ssu_dir = (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "SILVA-SSU"
    )
    (ssu_dir / f"{run}_SILVA-SSU.mseq").touch()
    with (
        (ssu_dir / f"{run}.html").open("w") as ssu_krona,
        (ssu_dir / f"{run}_SILVA-SSU.tsv").open("w") as ssu_tsv,
        (ssu_dir / f"{run}_SILVA-SSU.txt").open("w") as ssu_tax,
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
        ssu_tax.write(dedent("""\
            1	sk__Bacteria	k__	p__Bacillota	c__Bacilli
            2	sk__Bacteria	k__	p__Bacillota	c__Bacilli	o__Lactobacillales	f__Carnobacteriaceae	g__Trichococcus
            1	sk__Bacteria	k__	p__Bacillota	c__Clostridia	o__Eubacteriales	f__Eubacteriaceae	g__Eubacterium	s__Eubacterium_coprostanoligenes
            """))
    pr2_dir = (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "PR2"
    )
    (pr2_dir / f"{run}.html").touch()
    (pr2_dir / f"{run}_PR2.mseq").touch()
    (pr2_dir / f"{run}_PR2.tsv").touch()
    with ((pr2_dir / f"{run}_PR2.txt").open("w") as pr2_tax,):
        pr2_tax.write(dedent("""\
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
        """))
    dada2_silva_dir = (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "DADA2-SILVA"
    )
    (dada2_silva_dir / f"{run}_16S-V3-V4.html").touch()
    (dada2_silva_dir / f"{run}_DADA2-SILVA.mseq").write_text(
        "#query\tdbhit\nseq_1\thit_1\n"
    )
    (dada2_silva_dir / f"{run}_18S-V9.html").touch()
    (dada2_silva_dir / f"{run}_concat.html").touch()
    with (
        (dada2_silva_dir / f"{run}_16S-V3-V4_DADA2-SILVA_asv_krona_counts.txt").open(
            "w"
        ) as dada2_16s_krona_count,
        (dada2_silva_dir / f"{run}_18S-V9_DADA2-SILVA_asv_krona_counts.txt").open(
            "w"
        ) as dada2_18s_krona_count,
        (dada2_silva_dir / f"{run}_concat_DADA2-SILVA_asv_krona_counts.txt").open(
            "w"
        ) as dada2_concat_krona_count,
    ):
        for file in [
            dada2_16s_krona_count,
            dada2_18s_krona_count,
            dada2_concat_krona_count,
        ]:
            file.write(dedent("""\
            54	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Kitasatosporales	f__Streptomycetaceae	g__Streptomyces
            20	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Microbacteriaceae
            9	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Arthrobacter
            28	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Micrococcales	f__Micrococcaceae	g__Pseudarthrobacter
            100	sk__Bacteria	k__	p__Actinomycetota	c__Actinomycetes	o__Propionibacteriales	f__Nocardioidaceae	g__Aeromicrobium
            """))
    with open(dada2_silva_dir / f"{run}_16S-V3-V4_DADA2-SILVA.mseq", "w") as fw:
        fw.write(dedent("""\
        #query	dbhit	bitscore	identity	matches	mismatches	gaps	query_start	query_end	dbhit_start	dbhit_end	strand		SILVA
        seq_1	EF019080.1.1347	400	0.9975124597549438	401	1	0	0	402	331	733	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Kitasatosporales;f__Streptomycetaceae;g__Streptomyces
        seq_2	HE818674.1.1466	402	1	402	0	0	0	402	284	686	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Microbacteriaceae
        seq_3	GQ339135.1.1517	424	0.9953271150588989	426	2	0	0	428	360	788	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Micrococcaceae;g__Arthrobacter
        seq_4	LN563935.1.1317	400	0.9975124597549438	401	1	0	0	402	298	700	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Micrococcaceae;g__Pseudarthrobacter
        seq_5	EF018928.1.1401	424	0.9953271150588989	426	2	0	0	428	358	786	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Propionibacteriales;f__Nocardioidaceae;g__Aeromicrobium
                """))

    with open(dada2_silva_dir / f"{run}_18S-V9_DADA2-SILVA.mseq", "w") as fw:
        fw.write(dedent("""\
        #query	dbhit	bitscore	identity	matches	mismatches	gaps	query_start	query_end	dbhit_start	dbhit_end	strand		SILVA
        seq_1	EF019080.1.1347	400	0.9975124597549438	401	1	0	0	402	331	733	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Kitasatosporales;f__Streptomycetaceae;g__Streptomyces
        seq_2	HE818674.1.1466	402	1	402	0	0	0	402	284	686	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Microbacteriaceae
        seq_3	GQ339135.1.1517	424	0.9953271150588989	426	2	0	0	428	360	788	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Micrococcaceae;g__Arthrobacter
        seq_4	LN563935.1.1317	400	0.9975124597549438	401	1	0	0	402	298	700	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Micrococcales;f__Micrococcaceae;g__Pseudarthrobacter
        seq_5	EF018928.1.1401	424	0.9953271150588989	426	2	0	0	428	358	786	+	sk__Bacteria;k__;p__Actinomycetota;c__Actinomycetes;o__Propionibacteriales;f__Nocardioidaceae;g__Aeromicrobium
                """))

    dada2_pr2_dir = (
        amplicon_run_folder
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder
        / "DADA2-PR2"
    )
    (dada2_pr2_dir / f"{run}_16S-V3-V4.html").touch()
    (dada2_pr2_dir / f"{run}_DADA2-PR2.mseq").write_text(
        "#query\tdbhit\nseq_1\thit_1\n"
    )
    (dada2_pr2_dir / f"{run}_18S-V9.html").touch()
    (dada2_pr2_dir / f"{run}_concat.html").touch()
    with (
        (dada2_pr2_dir / f"{run}_16S-V3-V4_DADA2-PR2_asv_krona_counts.txt").open(
            "w"
        ) as dada2_16s_pr2_krona_count,
        (dada2_pr2_dir / f"{run}_18S-V9_DADA2-PR2_asv_krona_counts.txt").open(
            "w"
        ) as dada2_18s_pr2_krona_count,
        (dada2_pr2_dir / f"{run}_concat_DADA2-PR2_asv_krona_counts.txt").open(
            "w"
        ) as dada2_concat_pr2_krona_count,
    ):
        for file in [
            dada2_16s_pr2_krona_count,
            dada2_18s_pr2_krona_count,
            dada2_concat_pr2_krona_count,
        ]:
            file.write(dedent("""\
            2776	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Chitinophagales	f__Chitinophagaceae	g__Chitinophaga	s__Chitinophaga_sp.
            1566	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Cytophagales	f__Spirosomaceae	g__Dyadobacter	s__Dyadobacter_sp.
            135696	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Flavobacteriales	f__Flavobacteriaceae	g__Flavobacterium	s__Flavobacterium_sp.
            10847	d__Bacteria	sg__FCB	dv__Bacteroidetes	sdv__Bacteroidetes_X	c__Bacteroidia	o__Sphingobacteriales	f__Sphingobacteriaceae	g__Pedobacter	s__Pedobacter_sp.
            192	d__Bacteria	sg__PANNAM	dv__Proteobacteria	sdv__Proteobacteria_X	c__Alphaproteobacteria	o__Caulobacterales	f__Caulobacteraceae	g__Caulobacter	s__Caulobacter_sp.
            """))
    with open(dada2_pr2_dir / f"{run}_16S-V3-V4_DADA2-PR2.mseq", "w") as fw:
        fw.write(dedent("""\
        #query	dbhit	bitscore	identity	matches	mismatches	gaps	query_start	query_end	dbhit_start	dbhit_end	strand		SILVA
        seq_1	EF019080.1.1347	400	0.9975124597549438	401	1	0	0	402	331	733	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Chitinophagales;f__Chitinophagaceae;g__Chitinophaga;s__Chitinophaga_sp.
        seq_2	HE818674.1.1466	402	1	402	0	0	0	402	284	686	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Cytophagales;f__Spirosomaceae;g__Dyadobacter;s__Dyadobacter_sp.
        seq_3	GQ339135.1.1517	424	0.9953271150588989	426	2	0	0	428	360	788	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Flavobacteriales;f__Flavobacteriaceae;g__Flavobacterium;s__Flavobacterium_sp.
        seq_4	LN563935.1.1317	400	0.9975124597549438	401	1	0	0	402	298	700	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Sphingobacteriales;f__Sphingobacteriaceae;g__Pedobacter;s__Pedobacter_sp.
        seq_5	EF018928.1.1401	424	0.9953271150588989	426	2	0	0	428	358	786	+	d__Bacteria;sg__PANNAM;dv__Proteobacteria;sdv__Proteobacteria_X;c__Alphaproteobacteria;o__Caulobacterales;f__Caulobacteraceae;g__Caulobacter;s__Caulobacter_sp.
                """))

    with open(dada2_pr2_dir / f"{run}_18S-V9_DADA2-PR2.mseq", "w") as fw:
        fw.write(dedent("""\
        #query	dbhit	bitscore	identity	matches	mismatches	gaps	query_start	query_end	dbhit_start	dbhit_end	strand		SILVA
        seq_1	EF019080.1.1347	400	0.9975124597549438	401	1	0	0	402	331	733	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Chitinophagales;f__Chitinophagaceae;g__Chitinophaga;s__Chitinophaga_sp.
        seq_2	HE818674.1.1466	402	1	402	0	0	0	402	284	686	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Cytophagales;f__Spirosomaceae;g__Dyadobacter;s__Dyadobacter_sp.
        seq_3	GQ339135.1.1517	424	0.9953271150588989	426	2	0	0	428	360	788	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Flavobacteriales;f__Flavobacteriaceae;g__Flavobacterium;s__Flavobacterium_sp.
        seq_4	LN563935.1.1317	400	0.9975124597549438	401	1	0	0	402	298	700	+	d__Bacteria;sg__FCB;dv__Bacteroidetes;sdv__Bacteroidetes_X;c__Bacteroidia;o__Sphingobacteriales;f__Sphingobacteriaceae;g__Pedobacter;s__Pedobacter_sp.
        seq_5	EF018928.1.1401	424	0.9953271150588989	426	2	0	0	428	358	786	+	d__Bacteria;sg__PANNAM;dv__Proteobacteria;sdv__Proteobacteria_X;c__Alphaproteobacteria;o__Caulobacterales;f__Caulobacteraceae;g__Caulobacter;s__Caulobacter_sp.
                """))


def generate_fake_pipeline_no_asvs(amplicon_run_folder, run):
    # QC
    os.makedirs(
        f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}",
        exist_ok=True,
    )
    with (
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_seqfu.tsv",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}.fastp.json",
            "w",
        ) as fastp,
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.qc_folder}/{run}_multiqc_report.html",
            "w",
        ),
    ):
        json.dump({}, fastp)  # to get valid json

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
    with (
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU.fasta",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}.tblout.deoverlapped",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}/{run}_SSU_rRNA_bacteria.RF00177.fa",
            "w",
        ) as fasta,
    ):
        fasta.write(dedent(f"""\
                >{run}.100-SSU_rRNA_archaea/5-421 100/1 merged_251_171
                ACTCGGTCTAAAGGGTCCGTAGCCGGTCCAGTAAGTCCCTGCTTAAATCCTACGGCTTAA
                """))

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
    with (
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}.html",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.mseq",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.tsv",
            "w",
        ) as ssu_tsv,
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/SILVA-SSU/{run}_SILVA-SSU.txt",
            "w",
        ),
    ):
        ssu_tsv.writelines(
            [
                "# OTU ID\tSSU\ttaxonomy\ttaxid\n",
                "36901\t1.0\tsk__Bacteria;k__;p__Bacillota;c__Bacilli\t91061\n",
            ]
        )
    with (
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}.html",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.mseq",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.tsv",
            "w",
        ),
        open(
            f"{amplicon_run_folder}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}/PR2/{run}_PR2.txt",
            "w",
        ),
    ):
        pass


def write_amplicon_run_reports(
    outdir: Path,
    completed: Optional[Mapping[str, str]] = None,
    failed: Optional[Mapping[str, str]] = None,
):
    """
    Write the amplicon pipeline's two top-level end-of-run reports.

    Giving ``None`` for either mapping omits that CSV entirely, which is a
    distinct case from an empty mapping (empty file).

    :param outdir: Nextflow outdir for one pipeline execution (chunk/batch).
    :param completed: ``{run_accession: info}`` rows for the completed-runs CSV.
    :param failed: ``{run_accession: reason}`` rows for the failed-runs CSV.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    for filename, rows in (
        (EMG_CONFIG.amplicon_pipeline.completed_runs_csv, completed),
        (EMG_CONFIG.amplicon_pipeline.failed_runs_csv, failed),
    ):
        if rows is None:
            continue
        with (outdir / filename).open("w", newline="") as file_handle:
            csv.writer(file_handle).writerows(rows.items())


@pytest.fixture
def amplicon_analyses_factory(raw_reads_mgnify_study, top_level_biomes):
    """
    Make N amplicon runs (one sample each) and a v6 Analysis for each, in a study.

    Returns a callable ``(count, prefix="SRR90000") -> list[Analysis]`` ordered by
    analysis id, which is the order ``get_analyses_to_attempt`` returns them in.
    """

    def _make(count: int, prefix: str = "SRR90000") -> Sequence[mg_models.Analysis]:
        created = []
        for i in range(count):
            run_accession = f"{prefix}{i}"
            ena_sample, _ = ena_models.Sample.objects.get_or_create(
                study=raw_reads_mgnify_study.ena_study,
                accession=f"SAMEA9000{i}",
            )
            sample, _ = mg_models.Sample.objects.get_or_create(
                ena_sample=ena_sample,
                ena_study=raw_reads_mgnify_study.ena_study,
            )
            sample.studies.add(raw_reads_mgnify_study)
            sample.inherit_accessions_from_related_ena_object("ena_sample")
            run, _ = mg_models.Run.objects.get_or_create(
                ena_accessions=[run_accession],
                study=raw_reads_mgnify_study,
                ena_study=raw_reads_mgnify_study.ena_study,
                sample=sample,
                experiment_type=mg_models.Run.ExperimentTypes.AMPLICON,
                metadata={
                    mg_models.Run.CommonMetadataKeys.FASTQ_FTPS: [
                        f"ftp://example.org/{run_accession}_1.fastq.gz",
                        f"ftp://example.org/{run_accession}_2.fastq.gz",
                    ],
                },
            )
            analysis, _ = mg_models.Analysis.objects.get_or_create(
                study=raw_reads_mgnify_study,
                ena_study=raw_reads_mgnify_study.ena_study,
                sample=sample,
                run=run,
                experiment_type=mg_models.Analysis.ExperimentTypes.AMPLICON,
                pipeline_version=mg_models.Analysis.PipelineVersions.v6,
            )
            created.append(analysis.id)
        # Re-queried in id order, which is the order get_analyses_to_attempt uses.
        return list(mg_models.Analysis.objects.filter(id__in=created).order_by("id"))

    return _make
