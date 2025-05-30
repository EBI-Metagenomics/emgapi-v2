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
from workflows.data_io_utils.file_rules.base_rules import FileRule
from workflows.data_io_utils.file_rules.common_rules import GlobHasFilesCountRule
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.flows.analysis_assembly_study import analysis_assembly_study
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

EMG_CONFIG = settings.EMG_CONFIG


def generate_fake_assembly_pipeline_results(assembly_dir, assembly_accession):
    """
    Generate fake assembly pipeline results for testing.

    Based on the directory structure provided in the issue description.

    :param assembly_dir: Directory to create the fake results in
    :param assembly_accession: Assembly accession to use in file names
    """
    # Create the main directory
    os.makedirs(assembly_dir, exist_ok=True)

    # Create the predicted files
    with open(f"{assembly_dir}/{assembly_accession}_predicted_cds.faa.gz", "w"):
        pass
    with open(f"{assembly_dir}/{assembly_accession}_predicted_cds.gff.gz", "w"):
        pass
    with open(f"{assembly_dir}/{assembly_accession}_predicted_orf.ffn.gz", "w"):
        pass

    # Create functional-annotation directory and subdirectories
    func_annot_dir = f"{assembly_dir}/functional-annotation"

    dbcan_dir = f"{func_annot_dir}/dbcan"
    os.makedirs(dbcan_dir, exist_ok=True)
    with open(f"{dbcan_dir}/{assembly_accession}_dbcan_cgc.gff.gz", "w"):
        pass
    with open(f"{dbcan_dir}/{assembly_accession}_dbcan_overview.tsv.gz", "w"):
        pass
    with open(f"{dbcan_dir}/{assembly_accession}_dbcan_standard_out.tsv.gz", "w"):
        pass
    with open(f"{dbcan_dir}/{assembly_accession}_dbcan_sub_hmm.tsv.gz", "w"):
        pass
    with open(f"{dbcan_dir}/{assembly_accession}_dbcan_substrates.tsv.gz", "w"):
        pass

    eggnog_dir = f"{func_annot_dir}/eggnog"
    os.makedirs(eggnog_dir, exist_ok=True)
    with open(f"{eggnog_dir}/{assembly_accession}_emapper_annotations.tsv.gz", "w"):
        pass
    with open(f"{eggnog_dir}/{assembly_accession}_emapper_seed_orthologs.tsv.gz", "w"):
        pass

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
    with open(f"{go_dir}/{assembly_accession}_goslim_summary.tsv.gz.gzi", "w"):
        pass
    with open(f"{go_dir}/{assembly_accession}_go_summary.tsv.gz", "w"):
        pass
    with open(f"{go_dir}/{assembly_accession}_go_summary.tsv.gz.gzi", "w"):
        pass

    # interpro
    interpro_dir = f"{func_annot_dir}/interpro"
    os.makedirs(interpro_dir, exist_ok=True)
    with open(f"{interpro_dir}/{assembly_accession}_interproscan.tsv.gz", "w"):
        pass
    with open(f"{interpro_dir}/{assembly_accession}_interpro_summary.tsv.gz", "w"):
        pass
    with open(f"{interpro_dir}/{assembly_accession}_interpro_summary.tsv.gz.gzi", "w"):
        pass

    # kegg
    kegg_dir = f"{func_annot_dir}/kegg"
    os.makedirs(kegg_dir, exist_ok=True)
    with open(f"{kegg_dir}/{assembly_accession}_ko_summary.tsv.gz", "w"):
        pass
    with open(f"{kegg_dir}/{assembly_accession}_ko_summary.tsv.gz.gzi", "w"):
        pass

    # pfam
    pfam_dir = f"{func_annot_dir}/pfam"
    os.makedirs(pfam_dir, exist_ok=True)
    with open(f"{pfam_dir}/{assembly_accession}_pfam_summary.tsv.gz", "w"):
        pass
    with open(f"{pfam_dir}/{assembly_accession}_pfam_summary.tsv.gz.gzi", "w"):
        pass

    rhea_dir = f"{func_annot_dir}/rhea-reactions"
    os.makedirs(rhea_dir, exist_ok=True)
    with open(f"{rhea_dir}/{assembly_accession}_proteins2rhea.tsv.gz", "w"):
        pass
    with open(f"{rhea_dir}/{assembly_accession}_proteins2rhea.tsv.gz.gzi", "w"):
        pass

    pathways_dir = f"{assembly_dir}/pathways-and-systems"

    antismash_dir = f"{pathways_dir}/antismash"
    os.makedirs(antismash_dir, exist_ok=True)
    with open(
        f"{antismash_dir}/{assembly_accession}_antismash_summary.tsv.gz", "w"
    ):  # TODO is this file real?
        pass

    dram_dir = f"{pathways_dir}/dram-distill"
    os.makedirs(dram_dir, exist_ok=True)
    with open(f"{dram_dir}/{assembly_accession}_dram.html.gz", "w"):
        pass
    with open(f"{dram_dir}/{assembly_accession}_dram.tsv.gz", "w"):
        pass
    with open(f"{dram_dir}/{assembly_accession}_genome_stats.tsv.gz", "w"):
        pass
    with open(f"{dram_dir}/{assembly_accession}_metabolism_summary.xlsx.gz", "w"):
        pass

    # genome-properties
    gp_dir = f"{pathways_dir}/genome-properties"
    os.makedirs(gp_dir, exist_ok=True)
    with open(f"{gp_dir}/{assembly_accession}_gp.json.gz", "w"):
        pass
    with open(f"{gp_dir}/{assembly_accession}_gp.tsv.gz", "w"):
        pass
    with open(f"{gp_dir}/{assembly_accession}_gp.txt.gz", "w"):
        pass

    kegg_modules_dir = f"{pathways_dir}/kegg-modules"
    os.makedirs(kegg_modules_dir, exist_ok=True)
    with open(
        f"{kegg_modules_dir}/{assembly_accession}_kegg_modules_summary.tsv.gz", "w"
    ):
        pass

    # sanntis
    sanntis_dir = f"{pathways_dir}/sanntis"
    os.makedirs(sanntis_dir, exist_ok=True)
    with open(f"{sanntis_dir}/{assembly_accession}_sanntis.gff.gz", "w"):
        pass
    with open(f"{sanntis_dir}/{assembly_accession}_sanntis_summary.tsv.gz", "w"):
        pass

    # Create qc directory and subdirectories
    qc_dir = f"{assembly_dir}/qc"
    os.makedirs(qc_dir, exist_ok=True)
    with open(f"{qc_dir}/{assembly_accession}_filtered_contigs.fasta.gz", "w"):
        pass
    with open(f"{qc_dir}/{assembly_accession}.tsv", "w"):
        pass
    with open(f"{qc_dir}/multiqc_report.html", "w"):
        pass

    # Create taxonomy directory
    tax_dir = f"{assembly_dir}/taxonomy"
    os.makedirs(tax_dir, exist_ok=True)
    with open(f"{tax_dir}/{assembly_accession}.html", "w"):
        pass
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
    with open(f"{tax_dir}/{assembly_accession}_LSU.fasta.gz", "w"):
        pass
    with open(f"{tax_dir}/{assembly_accession}_SSU.fasta.gz", "w"):
        pass


MockFileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty (unit test mock)",
    test=lambda f: True,  # allows empty files created by mocks
)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch(
    "workflows.flows.analyse_study_tasks.run_assembly_pipeline_via_samplesheet.flow_run"
)
@patch("workflows.flows.analyse_study_tasks.make_samplesheet_assembly.queryset_hash")
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.assembly.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_assembly_study"], indirect=True
)
def test_prefect_analyse_assembly_flow(
    mock_queryset_hash_for_assembly,
    mock_flow_run_context,
    prefect_harness,
    httpx_mock,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    assembly_ena_study,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for assembly runs and launch it with samplesheet.
    One assembly has all results, one assembly failed
    """
    mock_queryset_hash_for_assembly.return_value = "abc123"

    mock_flow_run_context.root_flow_run_id = "xyz_flowrun_id"

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
            {
                "sample_accession": "SAMN08514018",
                "sample_title": "my data",
                "secondary_sample_accession": "SAMN08514018",
                "run_accession": "SRR123457",
                "analysis_accession": assembly_failed,
                "completeness_score": "85.0",
                "contamination_score": "5.2",
                "scientific_name": "metagenome",
                "location": "hinxton",
                "lat": "52",
                "lon": "0",
                "generated_ftp": f"ftp.sra.ebi.ac.uk/vol1/sequence/{assembly_failed}/contig.fa.gz",
            },
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
    assembly_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_assembly_v6/xyz_flowrun_id/abc123"
    )
    assembly_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{assembly_folder}/{EMG_CONFIG.assembly_analysis_pipeline.completed_assemblies_csv}",
        "w",
    ) as file:
        file.write(f"{assembly_all_results},success" + "\n")
    with open(
        f"{assembly_folder}/{EMG_CONFIG.assembly_analysis_pipeline.failed_assemblies_csv}",
        "w",
    ) as file:
        file.write(f"{assembly_failed},failed")

    # Generate fake pipeline results for the successful assembly
    generate_fake_assembly_pipeline_results(
        f"{assembly_folder}/{assembly_all_results}", assembly_all_results
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

    # check biome and watchers were set correctly
    assert study.biome.biome_name == "Engineered"
    assert admin_user == study.watchers.first()

    # check completed assemblies
    assert study.analyses.filter(status__analysis_completed=True).count() == 1
    # check failed assemblies
    assert study.analyses.filter(status__analysis_qc_failed=True).count() == 1
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
    assert go_slims[0] == "GO:0003824"

    # Check files
    workdir = Path(f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6")
    assert workdir.is_dir()

    assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"

    Directory(
        path=study.results_dir,
        glob_rules=[
            GlobHasFilesCountRule[4]
        ],  # taxonomy + goslim for the samplesheet, same two for the "merge" = 4
    )
