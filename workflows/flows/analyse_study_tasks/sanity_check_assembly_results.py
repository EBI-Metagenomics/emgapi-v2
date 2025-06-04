from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
)
from workflows.data_io_utils.file_rules.common_rules import (
    FileIfExistsIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import create_directory, Directory

from pathlib import Path

from prefect import task
from prefect.tasks import task_input_hash

from activate_django_first import EMG_CONFIG

import analyses.models


@task(
    cache_key_fn=task_input_hash,
)
def sanity_check_assembly_analysis_results(
    assembly_analysis_current_outdir: Path, analysis: analyses.models.Analysis
):
    create_assembly_analysis_schema(
        assembly_analysis_current_outdir,
        analysis.assembly.first_accession,
    )


def create_assembly_analysis_schema(
    assembly_current_outdir: Path, assembly_id: str
) -> Directory:
    """
    Create a schema for the assembly pipeline output.

    This function creates a Directory object that represents the assembly analysis schema,
    with all the subdirectories and files that are expected in the assembly pipeline output.

    The schema is created top-to-bottom, starting with the base directory and then adding
    subdirectories to it.

    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param assembly_id: Assembly ID (e.g. ERZ123456)
    :return: Directory object representing the assembly analysis schema
    """
    # Create the base directory first
    base_dir = create_directory(assembly_current_outdir / assembly_id)

    # QC folder validation
    qc_folder = EMG_CONFIG.assembly_analysis_pipeline.qc_folder
    qc_dir = create_directory(
        base_dir.path / qc_folder,
        files=[
            f"{assembly_id}_filtered_contigs.fasta.gz",
            f"{assembly_id}.tsv",
            "multiqc_report.html",
        ],
    )

    # Add multiqc_data subdirectory to qc_dir
    multiqc_data_dir = create_directory(base_dir.path / qc_folder / "multiqc_data")
    qc_dir.files.append(multiqc_data_dir)

    # Add qc_dir to base_dir
    base_dir.files.append(qc_dir)

    # CDS folder validation
    cds_dir = create_directory(
        base_dir.path / EMG_CONFIG.assembly_analysis_pipeline.cds_folder,
        files=[
            f"{assembly_id}_predicted_orf.ffn.gz",
            f"{assembly_id}_predicted_cds.faa.gz",
            f"{assembly_id}_predicted_cds.gff.gz",
        ],
    )

    # Add cds_dir to base_dir
    base_dir.files.append(cds_dir)

    # Taxonomy folder validation
    tax_dir = create_directory(
        base_dir.path / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder,
        files=[
            f"{assembly_id}_contigs_taxonomy.tsv.gz",
            f"{assembly_id}.krona.txt.gz",
            f"{assembly_id}.html",
            # SSU and LSU files are optional, so we don't validate them
        ],
        glob_rules=[GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule],
    )

    # Add tax_dir to base_dir
    base_dir.files.append(tax_dir)

    # Functional annotation folder validation
    func_annot_folder = (
        EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
    )

    # Create the main functional annotation directory
    func_annot_dir = create_directory(base_dir.path / func_annot_folder)

    # Add func_annot_dir to base_dir
    base_dir.files.append(func_annot_dir)

    # Create subdirectories with their files and add them to func_annot_dir
    interpro_dir = create_directory(
        base_dir.path / func_annot_folder / "interpro",
        files=[
            f"{assembly_id}_interproscan.tsv.gz",
            f"{assembly_id}_interpro_summary.tsv.gz",
            f"{assembly_id}_interpro_summary.tsv.gz.gzi",
        ],
    )
    func_annot_dir.files.append(interpro_dir)

    pfam_dir = create_directory(
        base_dir.path / func_annot_folder / "pfam",
        files=[
            f"{assembly_id}_pfam_summary.tsv.gz",
            f"{assembly_id}_pfam_summary.tsv.gz.gzi",
        ],
    )
    func_annot_dir.files.append(pfam_dir)

    go_dir = create_directory(
        base_dir.path / func_annot_folder / "go",
        files=[
            f"{assembly_id}_go_summary.tsv.gz",
            f"{assembly_id}_go_summary.tsv.gz.gzi",
            f"{assembly_id}_goslim_summary.tsv.gz",
            f"{assembly_id}_goslim_summary.tsv.gz.gzi",
        ],
    )
    func_annot_dir.files.append(go_dir)

    eggnog_dir = create_directory(
        base_dir.path / func_annot_folder / "eggnog",
        files=[
            f"{assembly_id}_emapper_seed_orthologs.tsv.gz",
            f"{assembly_id}_emapper_annotations.tsv.gz",
        ],
    )
    func_annot_dir.files.append(eggnog_dir)

    kegg_dir = create_directory(
        base_dir.path / func_annot_folder / "kegg",
        files=[
            f"{assembly_id}_ko_summary.tsv.gz",
            f"{assembly_id}_ko_summary.tsv.gz.gzi",
        ],
    )
    func_annot_dir.files.append(kegg_dir)

    rhea_dir = create_directory(
        base_dir.path / func_annot_folder / "rhea-reactions",
        files=[
            f"{assembly_id}_proteins2rhea.tsv.gz",
            f"{assembly_id}_proteins2rhea.tsv.gz.gzi",
        ],
    )
    func_annot_dir.files.append(rhea_dir)

    dbcan_dir = create_directory(
        base_dir.path / func_annot_folder / "dbcan",
        files=[
            f"{assembly_id}_dbcan_cgc.gff.gz",
            f"{assembly_id}_dbcan_standard_out.tsv.gz",
            f"{assembly_id}_dbcan_overview.tsv.gz",
            f"{assembly_id}_dbcan_sub_hmm.tsv.gz",
            f"{assembly_id}_dbcan_substrates.tsv.gz",
        ],
    )
    func_annot_dir.files.append(dbcan_dir)

    # Pathways and systems folder validation
    pathways_folder = EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder

    # Create the main pathways directory
    pathways_dir = create_directory(base_dir.path / pathways_folder)

    # Add pathways_dir to base_dir
    base_dir.files.append(pathways_dir)

    # Create subdirectories with their files and add them to pathways_dir
    antismash_dir = create_directory(
        base_dir.path / pathways_folder / "antismash",
        files=[
            f"{assembly_id}_antismash.gbk.gz",
            f"{assembly_id}_antismash.gff.gz",
            # f"{assembly_id}_merged.json", // TODO: Do we need this file?
            f"{assembly_id}_antismash_summary.tsv.gz",
            f"{assembly_id}_antismash_summary.tsv.gz.gzi",
        ],
    )
    pathways_dir.files.append(antismash_dir)

    # For sanntis, we use FileIfExistsIsNotEmptyRule
    sanntis_dir = create_directory(
        base_dir.path / pathways_folder / "sanntis",
        files=[
            (
                f"{assembly_id}_sanntis_concatenated.gff.gz",
                [FileIfExistsIsNotEmptyRule],
            ),
            (f"{assembly_id}_sanntis_summary.tsv.gz", [FileIfExistsIsNotEmptyRule]),
            (f"{assembly_id}_sanntis_summary.tsv.gz.gzi", [FileIfExistsIsNotEmptyRule]),
        ],
    )
    pathways_dir.files.append(sanntis_dir)

    genome_properties_dir = create_directory(
        base_dir.path / pathways_folder / "genome-properties",
        files=[
            f"{assembly_id}_gp.json.gz",
            f"{assembly_id}_gp.tsv.gz",
            f"{assembly_id}_gp.txt.gz",
        ],
    )
    pathways_dir.files.append(genome_properties_dir)

    kegg_modules_dir = create_directory(
        base_dir.path / pathways_folder / "kegg-modules",
        files=[
            f"{assembly_id}_kegg_modules_per_contigs.tsv.gz",
            f"{assembly_id}_kegg_modules_per_contigs.tsv.gz.gzi",
            f"{assembly_id}_kegg_modules_summary.tsv.gz",
            f"{assembly_id}_kegg_modules_summary.tsv.gz.gzi",
        ],
    )
    pathways_dir.files.append(kegg_modules_dir)

    dram_distill_dir = create_directory(
        base_dir.path / pathways_folder / "dram-distill",
        files=[
            f"{assembly_id}_dram.tsv.gz",
            f"{assembly_id}_dram.html.gz",
            f"{assembly_id}_genome_stats.tsv.gz",
            f"{assembly_id}_metabolism_summary.xlsx.gz",
        ],
    )
    pathways_dir.files.append(dram_distill_dir)

    # Annotation summary folder validation
    annotation_summary_dir = create_directory(
        base_dir.path / EMG_CONFIG.assembly_analysis_pipeline.annotation_summary_folder,
        files=[
            f"{assembly_id}_annotation_summary.gff.gz",
        ],
    )

    # Add annotation_summary_dir to base_dir
    base_dir.files.append(annotation_summary_dir)

    # Return the base directory with all subdirectories
    return base_dir
