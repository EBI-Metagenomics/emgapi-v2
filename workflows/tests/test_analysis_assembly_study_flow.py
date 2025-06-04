import gzip
import json
import logging
import os
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import List
from unittest.mock import patch

import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
from workflows.data_io_utils.file_rules.nodes import create_directory
from workflows.flows.analysis_assembly_study import analysis_assembly_study
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

EMG_CONFIG = settings.EMG_CONFIG


@pytest.fixture
def sample_fasta_records():
    """Sample FASTA records for testing"""
    return [
        ("contig_1", "ATCGATCGATCGATCG"),
        ("contig_2", "GCTAGCTAGCTAGCTA"),
        ("contig_3", "TTTTAAAACCCCGGGG"),
    ]


@pytest.fixture
def sample_gff_records():
    """Sample GFF records for testing"""
    return [
        "##gff-version 3",
        "contig_1\tPyrodigal\tCDS\t1\t300\t.\t+\t0\tID=cds1;Name=hypothetical protein",
        "contig_2\tPyrodigal\tCDS\t50\t500\t.\t-\t0\tID=cds2;Name=putative transporter",
        "contig_3\tPyrodigal\tCDS\t100\t400\t.\t+\t0\tID=cds3;Name=DNA-binding protein",
    ]


def generate_fake_assembly_pipeline_results(
    study_dir, assembly_dir, assembly_accession, sample_fasta_records
):
    """
    Generate fake assembly pipeline results for testing.

    Based on the directory structure provided in the issue description.

    :param assembly_dir: Directory to create the fake results in
    :param assembly_accession: Assembly accession to use in file names
    :param sample_fasta_records: Sample FASTA records for creating files with content
    """
    # Create the main directory
    os.makedirs(assembly_dir, exist_ok=True)

    # CDS folder
    cds_dir = f"{assembly_dir}/cds"
    os.makedirs(cds_dir, exist_ok=True)

    # Create the predicted files with content
    with gzip.open(f"{cds_dir}/{assembly_accession}_predicted_cds.faa.gz", "wt") as f:
        for header, seq in sample_fasta_records:
            f.write(f">{header}\n{seq}\n")

    with gzip.open(f"{cds_dir}/{assembly_accession}_predicted_cds.gff.gz", "wt") as f:
        f.write(
            "\n".join(
                [
                    "##gff-version 3",
                    "contig_1\tProdigal\tCDS\t1\t300\t.\t+\t0\tID=cds1;Name=hypothetical protein",
                    "contig_2\tProdigal\tCDS\t50\t500\t.\t-\t0\tID=cds2;Name=putative transporter",
                    "contig_3\tProdigal\tCDS\t100\t400\t.\t+\t0\tID=cds3;Name=DNA-binding protein",
                ]
            )
        )

    with gzip.open(f"{cds_dir}/{assembly_accession}_predicted_orf.ffn.gz", "wt") as f:
        for header, seq in sample_fasta_records:
            f.write(f">{header}_orf\n{seq}\n")

    # Create functional-annotation directory and subdirectories
    func_annot_dir = f"{assembly_dir}/functional-annotation"

    dbcan_dir = f"{func_annot_dir}/dbcan"
    os.makedirs(dbcan_dir, exist_ok=True)
    with gzip.open(f"{dbcan_dir}/{assembly_accession}_dbcan_cgc.gff.gz", "wt") as f:
        f.write(
            "\n".join(
                [
                    "##gff-version 3",
                    "contig_1\tdbCAN\tCAZy\t1\t300\t.\t+\t0\tID=cazy1;Name=GH13;Note=alpha-amylase",
                    "contig_2\tdbCAN\tCAZy\t50\t500\t.\t-\t0\tID=cazy2;Name=GT2;Note=glycosyltransferase",
                ]
            )
        )

    with gzip.open(
        f"{dbcan_dir}/{assembly_accession}_dbcan_overview.tsv.gz", "wt"
    ) as f:
        f.write("contig_id\tcazy_family\tsubstrate\n")
        f.write("contig_1\tGH13\talpha-amylase\n")
        f.write("contig_2\tGT2\tglycoside\n")

    with gzip.open(
        f"{dbcan_dir}/{assembly_accession}_dbcan_standard_out.tsv.gz", "wt"
    ) as f:
        f.write("contig_id\tcazy_hit\te_value\tscore\n")
        f.write("contig_1\tGH13\t1e-50\t500\n")
        f.write("contig_2\tGT2\t1e-40\t400\n")

    with gzip.open(f"{dbcan_dir}/{assembly_accession}_dbcan_sub_hmm.tsv.gz", "wt") as f:
        f.write("hmm_id\tsubstrate\n")
        f.write("GH13\talpha-amylase\n")
        f.write("GT2\tglycoside\n")

    with gzip.open(
        f"{dbcan_dir}/{assembly_accession}_dbcan_substrates.tsv.gz", "wt"
    ) as f:
        f.write("contig_id\tsubstrate\n")
        f.write("contig_1\talpha-amylase\n")
        f.write("contig_2\tglycoside\n")

    eggnog_dir = f"{func_annot_dir}/eggnog"
    os.makedirs(eggnog_dir, exist_ok=True)
    with gzip.open(
        f"{eggnog_dir}/{assembly_accession}_emapper_annotations.tsv.gz", "wt"
    ) as f:
        f.write(
            "query\tseed_ortholog\tevalue\tscore\ttaxonomic_group\tprotein_name\tGO_terms\tEC\tKEGG_ko\tKEGG_Pathway\tKEGG_Module\tKEGG_Reaction\tKEGG_rclass\tBRITE\tKEGG_TC\tCAZy\tBiGG_Reaction\ttax_scope\tOG\tbestOG\tCOG_cat\tdescription\n"
        )
        f.write(
            "contig_1\t1234.ENSP00000123456\t1e-50\t500\tEukaryota\tProtein A\tGO:0003824\t2.7.7.7\tK00123\tko00123\tM00123\tR00123\tRC00123\tbr00123\t1.A.1.1.1\tGH13\tRXN-123\t2759\tOG123\tOG123\tJ\tHypothetical protein\n"
        )
        f.write(
            "contig_2\t5678.ENSP00000567890\t1e-40\t400\tBacteria\tProtein B\tGO:0016491\t1.1.1.1\tK00456\tko00456\tM00456\tR00456\tRC00456\tbr00456\t2.A.1.1.1\tGT2\tRXN-456\t2\tOG456\tOG456\tK\tPutative enzyme\n"
        )

    with gzip.open(
        f"{eggnog_dir}/{assembly_accession}_emapper_seed_orthologs.tsv.gz", "wt"
    ) as f:
        f.write(
            "query_name\tseed_eggNOG_ortholog\tseed_ortholog_evalue\tseed_ortholog_score\n"
        )
        f.write("contig_1\t1234.ENSP00000123456\t1e-50\t500\n")
        f.write("contig_2\t5678.ENSP00000567890\t1e-40\t400\n")

    # go
    go_dir = f"{func_annot_dir}/go"
    os.makedirs(go_dir, exist_ok=True)
    with gzip.open(
        f"{go_dir}/{assembly_accession}_goslim_summary.tsv.gz", "wt"
    ) as goslim:
        goslim.write(
            dedent(
                """\
                go\tterm\tcategory\tcount
                GO:0003824\tcatalytic activity\tmolecular_function\t2145
                GO:0003674\tDNA binding\tmolecular_function\t6125
                GO:0055085\ttransmembrane transport\tbiological_process\t144
                GO:0016491\toxidoreductase activity\tmolecular_function\t1513
                """
            )
        )

    # Create a simple gzi index file
    with open(f"{go_dir}/{assembly_accession}_goslim_summary.tsv.gz.gzi", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    with gzip.open(f"{go_dir}/{assembly_accession}_go_summary.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            go\tterm\tcategory\tcount
            GO:0003824\tcatalytic activity\tmolecular_function\t2145
            GO:0003674\tDNA binding\tmolecular_function\t6125
            GO:0055085\ttransmembrane transport\tbiological_process\t144
            GO:0016491\toxidoreductase activity\tmolecular_function\t1513
            GO:0005215\ttransporter activity\tmolecular_function\t987
            GO:0005975\tcarbohydrate metabolic process\tbiological_process\t456
            GO:0006508\tproteolysis\tbiological_process\t789
            GO:0008152\tmetabolic process\tbiological_process\t3456
            GO:0016020\tmembrane\tcellular_component\t2345
            GO:0016021\tintegral component of membrane\tcellular_component\t1234
            """
            )
        )

    # Create a simple gzi index file
    with open(f"{go_dir}/{assembly_accession}_go_summary.tsv.gz.gzi", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # interpro
    interpro_dir = f"{func_annot_dir}/interpro"
    os.makedirs(interpro_dir, exist_ok=True)

    with gzip.open(
        f"{interpro_dir}/{assembly_accession}_interproscan.tsv.gz", "wt"
    ) as f:
        f.write(
            dedent(
                """\
            contig_1\tERZ999999_1\t300\tPfam\tPF00001\tSeven transmembrane receptor (rhodopsin family)\t1\t280\t1.3E-40\t-\tT\t25-04-2023\tIPR000276\tG protein-coupled receptor, rhodopsin-like\tGO:0004930|GO:0016021
            contig_2\tERZ999999_2\t450\tPfam\tPF00005\tABC transporter\t50\t200\t2.5E-30\t-\tT\t25-04-2023\tIPR003439\tABC transporter-like\tGO:0005524|GO:0016887
            contig_3\tERZ999999_3\t250\tPfam\tPF00106\tshort chain dehydrogenase\t10\t200\t1.1E-25\t-\tT\t25-04-2023\tIPR002198\tShort-chain dehydrogenase/reductase SDR\tGO:0016491
            """
            )
        )

    with gzip.open(
        f"{interpro_dir}/{assembly_accession}_interpro_summary.tsv.gz", "wt"
    ) as f:
        f.write(
            dedent(
                """\
            interpro_accession\tdescription\tcount
            IPR000276\tG protein-coupled receptor, rhodopsin-like\t15
            IPR003439\tABC transporter-like\t23
            IPR002198\tShort-chain dehydrogenase/reductase SDR\t42
            IPR001128\tCytochrome P450\t18
            IPR001789\tSignal transduction response regulator, receiver domain\t31
            """
            )
        )

    # Create a simple gzi index file
    with open(
        f"{interpro_dir}/{assembly_accession}_interpro_summary.tsv.gz.gzi", "wb"
    ) as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # kegg
    kegg_dir = f"{func_annot_dir}/kegg"
    os.makedirs(kegg_dir, exist_ok=True)
    with gzip.open(f"{kegg_dir}/{assembly_accession}_ko_summary.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            ko\tdescription\tcount
            K00001\talcohol dehydrogenase [EC:1.1.1.1]\t25
            K00002\talcohol dehydrogenase (NADP+) [EC:1.1.1.2]\t18
            K00003\thomoserine dehydrogenase [EC:1.1.1.3]\t12
            K00004\t(R,R)-butanediol dehydrogenase [EC:1.1.1.4]\t8
            K00005\tglycerol dehydrogenase [EC:1.1.1.6]\t15
            """
            )
        )

    # Create a simple gzi index file
    with open(f"{kegg_dir}/{assembly_accession}_ko_summary.tsv.gz.gzi", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # pfam
    pfam_dir = f"{func_annot_dir}/pfam"
    os.makedirs(pfam_dir, exist_ok=True)
    with gzip.open(f"{pfam_dir}/{assembly_accession}_pfam_summary.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            pfam\tdescription\tcount
            PF00001\t7 transmembrane receptor (rhodopsin family)\t15
            PF00005\tABC transporter\t23
            PF00106\tshort chain dehydrogenase\t42
            PF00067\tCytochrome P450\t18
            PF00072\tResponse regulator receiver domain\t31
            """
            )
        )

    # Create a simple gzi index file
    with open(f"{pfam_dir}/{assembly_accession}_pfam_summary.tsv.gz.gzi", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    rhea_dir = f"{func_annot_dir}/rhea-reactions"
    os.makedirs(rhea_dir, exist_ok=True)
    with gzip.open(f"{rhea_dir}/{assembly_accession}_proteins2rhea.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            rhea_id\trhea_direction\trhea_reaction\tcount
            RHEA:10000\tLR\tATP + H2O = ADP + phosphate + H+\t45
            RHEA:10001\tLR\tATP + H2O + small molecule1 = ADP + phosphate + small molecule2\t32
            RHEA:10002\tLR\tNADH + H+ + acceptor = NAD+ + reduced acceptor\t28
            RHEA:10003\tLR\tNADPH + H+ + acceptor = NADP+ + reduced acceptor\t19
            RHEA:10004\tLR\tpyruvate + CoA + NAD+ = acetyl-CoA + CO2 + NADH + H+\t15
            """
            )
        )

    # Create a simple gzi index file
    with open(f"{rhea_dir}/{assembly_accession}_proteins2rhea.tsv.gz.gzi", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    pathways_dir = f"{assembly_dir}/pathways-and-systems"

    antismash_dir = f"{pathways_dir}/antismash"
    os.makedirs(antismash_dir, exist_ok=True)

    with open(f"{antismash_dir}/{assembly_accession}_antismash.gbk.gz", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    with open(f"{antismash_dir}/{assembly_accession}_antismash.gff.gz", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    with gzip.open(
        f"{antismash_dir}/{assembly_accession}_antismash_summary.tsv.gz", "wt"
    ) as f:
        f.write(
            dedent(
                """\
            label\tdescription\tcount
            NRPS\tNonribosomal peptide synthetase\t1
            PKS-I\tType I Polyketide synthase\t1
            RiPP\tRibosomally synthesized and post-translationally modified peptide\t1
            Terpene\tTerpene biosynthetic gene cluster\t1
            Other\tOther biosynthetic gene cluster\t1
            """
            )
        )

    # Create a simple gzi index file
    with open(
        f"{antismash_dir}/{assembly_accession}_antismash_summary.tsv.gz.gzi", "wb"
    ) as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    dram_dir = f"{pathways_dir}/dram-distill"
    os.makedirs(dram_dir, exist_ok=True)
    with gzip.open(f"{dram_dir}/{assembly_accession}_dram.html.gz", "wt") as f:
        f.write(
            "<html><body><h1>DRAM Distill Report</h1><p>Sample report content</p></body></html>"
        )

    with gzip.open(f"{dram_dir}/{assembly_accession}_dram.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            genome\tcompleteness\tcontamination\tmetabolism_type\tprimary_metabolism\tsecondary_metabolism
            contig_1\t95.5\t2.1\taerobic\theterotrophic\tantibiotics
            contig_2\t87.3\t1.5\tanaerobic\tautotrophic\tsiderophores
            contig_3\t92.8\t0.8\tfacultative\tmixotrophic\ttoxins
            """
            )
        )

    with gzip.open(f"{dram_dir}/{assembly_accession}_genome_stats.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            genome\tcompleteness\tcontamination\tGC_content\tN50\tsize_bp\tnum_contigs
            contig_1\t95.5\t2.1\t0.65\t10000\t3500000\t350
            contig_2\t87.3\t1.5\t0.45\t8000\t2800000\t420
            contig_3\t92.8\t0.8\t0.55\t12000\t4200000\t280
            """
            )
        )

    with gzip.open(
        f"{dram_dir}/{assembly_accession}_metabolism_summary.xlsx.gz", "wb"
    ) as f:
        # Just write some binary data to simulate an Excel file
        f.write(
            b"PK\x03\x04\x14\x00\x00\x00\x08\x00\x00\x00!\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        )

    # genome-properties
    gp_dir = f"{pathways_dir}/genome-properties"
    os.makedirs(gp_dir, exist_ok=True)
    with gzip.open(f"{gp_dir}/{assembly_accession}_gp.json.gz", "wt") as f:
        f.write(
            '{"property_id": "GenProp0001", "name": "chorismate biosynthesis", "count": 15}'
        )

    with gzip.open(f"{gp_dir}/{assembly_accession}_gp.tsv.gz", "wt") as f:
        f.write(
            dedent(
                """\
            property_id\tname\ttype\tcount
            GenProp0001\tchorismate biosynthesis\tPATHWAY\t15
            GenProp0002\ttryptophan biosynthesis from chorismate\tPATHWAY\t12
            GenProp0003\tTCA cycle\tPATHWAY\t18
            GenProp0004\ttype IV pilus\tSYSTEM\t8
            GenProp0005\tflagellar motility\tSYSTEM\t22
            """
            )
        )

    with gzip.open(f"{gp_dir}/{assembly_accession}_gp.txt.gz", "wt") as f:
        f.write(
            dedent(
                """\
            GenProp0001: chorismate biosynthesis
            GenProp0002: tryptophan biosynthesis from chorismate
            GenProp0003: TCA cycle
            GenProp0004: type IV pilus
            GenProp0005: flagellar motility
            """
            )
        )

    kegg_modules_dir = f"{pathways_dir}/kegg-modules"
    os.makedirs(kegg_modules_dir, exist_ok=True)

    with gzip.open(
        f"{kegg_modules_dir}/{assembly_accession}_kegg_modules_summary.tsv.gz", "wt"
    ) as f:
        f.write(
            dedent(
                """\
            module_accession\tcompleteness\tpathway_name\tpathway_class
            M00001\t0.9\tGlycolysis (Embden-Meyerhof pathway)\tPathway modules; Energy metabolism; Carbohydrate metabolism
            M00002\t1.0\tGlycolysis, core module\tPathway modules; Energy metabolism; Carbohydrate metabolism
            M00003\t0.8\tGluconeogenesis\tPathway modules; Energy metabolism; Carbohydrate metabolism
            M00004\t0.7\tPentose phosphate pathway (Pentose phosphate cycle)\tPathway modules; Energy metabolism; Carbohydrate metabolism
            M00005\t1.0\tPentose phosphate pathway, oxidative phase\tPathway modules; Energy metabolism; Carbohydrate metabolism
            """
            )
        )

    # Create a simple gzi index file
    with open(
        f"{kegg_modules_dir}/{assembly_accession}_kegg_modules_summary.tsv.gz.gzi", "wb"
    ) as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # Create kegg_modules_per_contigs.tsv.gz with the specified header
    with gzip.open(
        f"{kegg_modules_dir}/{assembly_accession}_kegg_modules_per_contigs.tsv.gz", "wt"
    ) as f:
        f.write(
            "contig\tmodule_accession\tcompleteness\tpathway_name\tpathway_class\tmatching_ko\tmissing_ko\n"
        )

    # Create a simple gzi index file for kegg_modules_per_contigs
    with open(
        f"{kegg_modules_dir}/{assembly_accession}_kegg_modules_per_contigs.tsv.gz.gzi",
        "wb",
    ) as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # sanntis
    sanntis_dir = f"{pathways_dir}/sanntis"
    os.makedirs(sanntis_dir, exist_ok=True)
    with gzip.open(f"{sanntis_dir}/{assembly_accession}_sanntis.gff.gz", "wt") as f:
        f.write(
            dedent(
                """\
            ##gff-version 3
            contig_1\tSanntis\tBGC\t1000\t25000\t.\t+\t.\tID=BGC0000001;Name=BGC0000001;bgc_type=NRPS;product=surfactin
            contig_2\tSanntis\tBGC\t5000\t35000\t.\t-\t.\tID=BGC0000002;Name=BGC0000002;bgc_type=PKS-I;product=erythromycin
            contig_3\tSanntis\tBGC\t10000\t20000\t.\t+\t.\tID=BGC0000003;Name=BGC0000003;bgc_type=RiPP;product=lantipeptide
            """
            )
        )

    with gzip.open(
        f"{sanntis_dir}/{assembly_accession}_sanntis_summary.tsv.gz", "wt"
    ) as f:
        f.write(
            dedent(
                """\
            nearest_mibig\tnearest_mibig_class\tdescription\tcount
            BGC0000001\tNRPS\tSurfactin biosynthetic gene cluster\t1
            BGC0000002\tPKS-I\tErythromycin biosynthetic gene cluster\t1
            BGC0000003\tRiPP\tLantipeptide biosynthetic gene cluster\t1
            """
            )
        )

    # Create a simple gzi index file
    with open(
        f"{sanntis_dir}/{assembly_accession}_sanntis_summary.tsv.gz.gzi", "wb"
    ) as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    # Create qc directory and subdirectories
    qc_dir = f"{assembly_dir}/qc"
    os.makedirs(qc_dir, exist_ok=True)
    os.makedirs(f"{qc_dir}/multiqc_data", exist_ok=True)

    with open(f"{qc_dir}/multiqc_data/multiqc.log", "w") as f:
        pass

    with open(f"{qc_dir}/{assembly_accession}.tsv", "w") as f:
        f.write(
            dedent(
                """\
            contig_id\tlength\tGC_content\tcoverage
            contig_1\t1000\t0.55\t120.5
            contig_2\t1500\t0.48\t85.3
            contig_3\t800\t0.62\t150.8
            """
            )
        )

    # Create filtered_contigs.fasta.gz with content from sample_fasta_records
    with gzip.open(
        f"{qc_dir}/{assembly_accession}_filtered_contigs.fasta.gz", "wt"
    ) as f:
        for header, seq in sample_fasta_records:
            f.write(f">{header}\n{seq}\n")

    with open(f"{qc_dir}/{assembly_accession}.tsv", "w") as f:
        f.write(
            dedent(
                """\
            contig_id\tlength\tGC_content\tcoverage
            contig_1\t1000\t0.55\t120.5
            contig_2\t1500\t0.48\t85.3
            contig_3\t800\t0.62\t150.8
            """
            )
        )

    with open(f"{qc_dir}/multiqc_report.html", "w") as f:
        f.write(
            "<html><body><h1>MultiQC Report</h1><p>Sample report content</p></body></html>"
        )

    # Create taxonomy directory
    tax_dir = f"{assembly_dir}/taxonomy"
    os.makedirs(tax_dir, exist_ok=True)

    with open(f"{tax_dir}/{assembly_accession}_contigs_taxonomy.tsv.gz", "wb") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    with open(f"{tax_dir}/{assembly_accession}.html", "w") as f:
        f.write(
            dedent(
                """\
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Krona Chart</title>
                <script>var data = {"krona": {"attributes": {"magnitude": "count"}, "datasets": 1, "node": {"name": "all", "children": [{"name": "Archaea", "count": 10}, {"name": "Bacteria", "count": 115}, {"name": "unclassified", "count": 46}]}}}</script>
            </head>
            <body>
                <div id="krona">Krona chart would be displayed here</div>
            </body>
            </html>
            """
            )
        )

    with gzip.open(f"{tax_dir}/{assembly_accession}.krona.txt.gz", "wt") as tax_file:
        # with open(f"{tax_dir}/{assembly_accession}.krona.txt", "wt") as tax_file:
        tax_file.write(
            dedent(
                """\
                7	sk__Archaea	k__Thermoproteati	p__Nitrososphaerota	c__Nitrososphaeria	o__Nitrosopumilales	f__Nitrosopumilaceae	g__Nitrosopumilus	s__Candidatus Nitrosopumilus koreensis
                3	sk__Archaea	k__Thermoproteati	p__Nitrososphaerota	c__Nitrososphaeria	o__Nitrosotaleales	f__Nitrosotaleaceae	g__Nitrosotalea	s__Nitrosotalea devaniterrae
                98	sk__Bacteria
                2	sk__Bacteria	k__Bacillati
                3	sk__Bacteria	k__Bacillati	p__Chloroflexota	c__Chloroflexia	o__	f__	g__	s__Chloroflexia bacterium
                2	sk__Bacteria	k__Bacillati	p__Chloroflexota	c__Dehalococcoidia
                8	sk__Bacteria	k__Bacillati	p__Actinomycetota	c__Acidimicrobiia	o__Acidimicrobiales
                1	sk__Bacteria	k__Bacillati	p__Actinomycetota	c__Actinomycetes
                1	sk__Bacteria	k__Pseudomonadati	p__Bacteroidota	c__Bacteroidia
                46	unclassified
                """
            )
        )

    # Create LSU and SSU fasta files with content
    with gzip.open(f"{tax_dir}/{assembly_accession}_LSU.fasta.gz", "wt") as f:
        f.write(
            dedent(
                """\
            >LSU_contig_1 LSU rRNA gene
            ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG
            ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG
            >LSU_contig_2 LSU rRNA gene
            GCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTA
            GCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTA
            """
            )
        )

    with gzip.open(f"{tax_dir}/{assembly_accession}_SSU.fasta.gz", "wt") as f:
        f.write(
            dedent(
                """\
            >SSU_contig_1 SSU rRNA gene
            TTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGG
            TTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGG
            >SSU_contig_2 SSU rRNA gene
            GGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTT
            GGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTT
            """
            )
        )

    # Create annotation-summary directory
    annotation_summary_dir = f"{assembly_dir}/annotation-summary"
    os.makedirs(annotation_summary_dir, exist_ok=True)

    # Create annotation_summary.gff.gz with content from sample_gff_records
    with gzip.open(
        f"{annotation_summary_dir}/{assembly_accession}_annotation_summary.gff.gz", "wt"
    ) as f:
        f.write(
            "\n".join(
                [
                    "##gff-version 3",
                    "contig_1\tProdigal\tCDS\t1\t300\t.\t+\t0\tID=cds1;Name=hypothetical protein",
                    "contig_2\tProdigal\tCDS\t50\t500\t.\t-\t0\tID=cds2;Name=putative transporter",
                    "contig_3\tProdigal\tCDS\t100\t400\t.\t+\t0\tID=cds3;Name=DNA-binding protein",
                ]
            )
        )

    # Create downstream_samplesheets directory
    downstream_samplesheets_dir = f"{study_dir}/downstream_samplesheets"
    os.makedirs(downstream_samplesheets_dir, exist_ok=True)

    # Create virify_samplesheet.csv with required columns
    with open(f"{downstream_samplesheets_dir}/virify_samplesheet.csv", "w") as f:
        f.write("id,assembly,fastq_1,fastq_2,proteins\n")
        f.write(
            f"{assembly_accession},{assembly_dir}/qc/{assembly_accession}_filtered_contigs.fasta.gz,,,{cds_dir}/{assembly_accession}_predicted_cds.faa.gz\n"
        )

    # Create virify results directory with the specific path mentioned in the issue description
    virify_dir = "/tmp/PRJEB25958_virify/xyz_flowrun_id_virify/001"
    gff_dir = f"{virify_dir}/{EMG_CONFIG.virify_pipeline.final_gff_folder}"
    os.makedirs(gff_dir, exist_ok=True)

    # Create a mock GFF file
    with open(f"{gff_dir}/viral_contigs.gff", "w") as f:
        f.write(
            "\n".join(
                [
                    "##gff-version 3",
                    "contig_1\tVIRify\tviral_sequence\t1\t1000\t.\t+\t.\tID=viral_1;Name=Viral contig 1",
                    "contig_2\tVIRify\tviral_sequence\t1\t1500\t.\t-\t.\tID=viral_2;Name=Viral contig 2",
                ]
            )
        )


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.next_enumerated_subdir"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_virify_pipeline_via_samplesheet.flow_run"
)
@patch(
    "workflows.flows.analyse_study_tasks.run_assembly_pipeline_via_samplesheet.flow_run"
)
@patch("workflows.flows.analyse_study_tasks.make_samplesheet_assembly.queryset_hash")
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_assembly_study"], indirect=True
)
def test_prefect_analyse_assembly_flow(
    mock_queryset_hash_for_assembly,
    mock_flow_run_context_assembly,
    mock_flow_run_context_virify,
    mock_next_enumerated_subdir,
    httpx_mock,
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_ena_study,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
    sample_fasta_records,
    sample_gff_records,
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for assembly runs and launch it with samplesheet.
    One assembly has all results, one assembly failed
    """
    mock_queryset_hash_for_assembly.return_value = "abc123"

    # Set the same flow run ID for both assembly and virify pipelines
    mock_flow_run_context_assembly.root_flow_run_id = "xyz_flowrun_id"

    def enumerated_subdir(parent_dir: Path, mkdirs: bool = False, pad: int = 4):
        return parent_dir / Path("001")

    mock_next_enumerated_subdir.side_effect = enumerated_subdir

    mock_flow_run_context_virify.root_flow_run_id = "xyz_flowrun_id_virify"

    study_accession = "PRJEB25958"
    assembly_all_results = "ERZ1049444"
    assembly_failed = "ERZ1049445"
    assemblies = [
        assembly_all_results,
        assembly_failed,
    ]

    # mock ENA response for assemblies
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=analysis"
        f"&query=%22%28study_accession={study_accession}+OR+secondary_study_accession={study_accession}%29%22"
        f"&limit=10000"
        f"&format=json"
        f"&fields=sample_accession%2Csample_title%2Csecondary_sample_accession%2Crun_accession%2Canalysis_accession%2Ccompleteness_score%2Ccontamination_score%2Cscientific_name%2Clocation%2Clat%2Clon%2Cgenerated_ftp"
        f"&dataPortal=metagenome",
        json=[
            {
                "sample_accession": "SAMN08514017",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514017",
                "run_accession": "SRR123456",
                "analysis_accession": assembly_all_results,
                "completeness_score": "95.0",
                "contamination_score": "1.2",
                "scientific_name": "metagenome",
                "location": "hinxton",
                "lat": "52",
                "lon": "0",
                "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{assembly_all_results}/contig.fa.gz",
            },
            # TODO: handle results with partial files
            # {
            #     "sample_accession": "SAMN08514018",
            #     "sample_title": "my data",
            #     "secondary_sample_accession": "SAMN08514018",
            #     "run_accession": "SRR123457",
            #     "analysis_accession": assembly_failed,
            #     "completeness_score": "85.0",
            #     "contamination_score": "5.2",
            #     "scientific_name": "metagenome",
            #     "location": "hinxton",
            #     "lat": "52",
            #     "lon": "0",
            #     "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{assembly_failed}/contig.fa.gz",
            # },
        ],
    )

    # mock ENA response for runs
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22%28study_accession={study_accession}+OR+secondary_study_accession={study_accession}%29%22"
        f"&limit=10000"
        f"&format=json"
        f"&fields=run_accession%2Csample_accession%2Csample_title%2Csecondary_sample_accession%2Cfastq_md5%2Cfastq_ftp%2Clibrary_layout%2Clibrary_strategy%2Clibrary_source%2Cscientific_name%2Chost_tax_id%2Chost_scientific_name%2Cinstrument_platform%2Cinstrument_model%2Clocation%2Clat%2Clon"
        f"&dataPortal=metagenome",
        json=[
            {
                "sample_accession": "SAMN08514017",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514017",
                "run_accession": "SRR123456",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR123456/SRR123456_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR123456/SRR123456_2.fastq.gz",
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
                "sample_accession": "SAMN08514018",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514018",
                "run_accession": "SRR123457",
                "fastq_md5": "123;abc",
                "fastq_ftp": "ftp.sra.example.org/vol/fastq/SRR123457/SRR123457_1.fastq.gz;ftp.sra.example.org/vol/fastq/SRR123457/SRR123457_2.fastq.gz",
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

    # create fake results
    study_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_assembly_v6/xyz_flowrun_id/abc123"
    )
    study_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{study_folder}/{EMG_CONFIG.assembly_analysis_pipeline.completed_assemblies_csv}",
        "w",
    ) as file:
        file.write(f"{assembly_all_results},success" + "\n")

    with open(
        f"{study_folder}/{EMG_CONFIG.assembly_analysis_pipeline.qc_failed_assemblies}",
        "w",
    ) as file:
        file.write(
            f"{assembly_failed},insufficient_contigs_after_length_filtering" + "\n"
        )

    # Generate fake pipeline results for the successful assembly
    generate_fake_assembly_pipeline_results(
        study_folder,
        f"{study_folder}/{assembly_all_results}",
        assembly_all_results,
        sample_fasta_records,
    )

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices["root.engineered"],
                watchers=[UserChoices[admin_user.username]],
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # RUN MAIN FLOW
    analysis_assembly_study(study_accession=study_accession)

    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()
    mock_suspend_flow_run.assert_called()

    assembly_samplesheet_table = Artifact.get("assembly-v6-initial-sample-sheet")
    assert assembly_samplesheet_table.type == "table"
    table_data = json.loads(assembly_samplesheet_table.data)
    assert len(table_data) == len(assemblies)
    assert table_data[0]["sample"] in [assembly_all_results, assembly_failed]
    assert table_data[0]["assembly_fasta"].endswith("contig.fa.gz")

    study = analyses.models.Study.objects.get_or_create_for_ena_study(study_accession)
    assert (
        study.analyses.filter(
            assembly__ena_accessions__contains=[assembly_all_results]
        ).count()
        == 1
    )

    # check biome and watchers where set correctly
    assert study.biome.biome_name == "Engineered"
    assert admin_user == study.watchers.first()

    # check completed assemblies
    assert study.analyses.filter(status__analysis_completed=True).count() == 1

    # TOOD: adjust the code to support the qc_failed on
    # check failed assemblies
    # assert study.analyses.filter(status__analysis_qc_failed=True).count() == 1

    assert (
        study.analyses.filter(status__analysis_completed_reason="success").count() == 1
    )
    assert (
        study.analyses.filter(status__analysis_qc_failed_reason="failed").count() == 1
    )

    # Check that the study has v6 analyses
    study.refresh_from_db()
    assert study.features.has_v6_analyses

    # Check taxonomies were imported
    analysis_which_should_have_taxonomies_imported: analyses.models.Analysis = (
        analyses.models.Analysis.objects_and_annotations.get(
            assembly__ena_accessions__contains=[assembly_all_results]
        )
    )
    assert (
        analyses.models.Analysis.TAXONOMIES
        in analysis_which_should_have_taxonomies_imported.annotations
    )
    assert (
        analyses.models.Analysis.TaxonomySources.UNIREF.value
        in analysis_which_should_have_taxonomies_imported.annotations[
            analyses.models.Analysis.TAXONOMIES
        ]
    )
    contig_taxa = analysis_which_should_have_taxonomies_imported.annotations[
        analyses.models.Analysis.TAXONOMIES
    ][analyses.models.Analysis.TaxonomySources.UNIREF.value]
    assert len(contig_taxa) == 10
    assert (
        contig_taxa[0]["organism"]
        == "sk__Archaea;k__Thermoproteati;p__Nitrososphaerota;c__Nitrososphaeria;o__Nitrosopumilales;f__Nitrosopumilaceae;g__Nitrosopumilus;s__Candidatus Nitrosopumilus koreensis"
    )

    # Check functions were imported
    go_slims = analysis_which_should_have_taxonomies_imported.annotations[
        analyses.models.Analysis.GO_SLIMS
    ]

    logging.warning(analysis_which_should_have_taxonomies_imported.annotations)

    assert len(go_slims) == 4
    assert go_slims[0]["go"] == "GO:0003824"
    assert go_slims[0]["category"] == "molecular_function"
    assert go_slims[0]["count"] == 2145

    # Check files
    workdir = Path(f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6")
    assert workdir.is_dir()

    assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"

    create_directory(
        base_dir=Path(study.results_dir),
        files=[
            f"{study_accession}_go_summary.tsv",
            f"{study_accession}_goslim_summary.tsv",
            f"{study_accession}_interpro_summary.tsv",
            f"{study_accession}_ko_summary.tsv",
            f"{study_accession}_kegg_modules_summary.tsv",
            f"{study_accession}_pfam_summary.tsv",
            f"{study_accession}_sanntis_summary.tsv",
            f"{study_accession}_antismash_summary.tsv",
            f"{study_accession}_taxonomy_summary.tsv",
            f"{study_accession}_go_summary.tsv",
            "abc123_goslim_summary.tsv",
            "abc123_interpro_summary.tsv",
            "abc123_ko_summary.tsv",
            "abc123_kegg_modules_summary.tsv",
            "abc123_pfam_summary.tsv",
            "abc123_sanntis_summary.tsv",
            "abc123_antismash_summary.tsv",
            "abc123_taxonomy_summary.tsv",
        ],
    )
