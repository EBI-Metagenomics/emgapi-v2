from pathlib import Path

from pydantic import BaseModel

from activate_django_first import EMG_CONFIG

from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
    FileIfExistsIsNotEmptyRule,
    GlobHasFilesRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File


class AssemblyAnalysisResultsSchema(BaseModel):
    """
    Top-level schema for the whole of the results of the assembly analysis pipeline.
    This schema can be used to validate the assembly pipeline output structure and
    to generate a JSON schema for the output.
    """

    qc_dir: Directory
    cds_dir: Directory
    tax_dir: Directory
    func_annot_dir: Directory
    pathways_dir: Directory
    annotation_summary_dir: Directory

    @classmethod
    def create_schema(
        cls, assembly_current_outdir: Path, assembly_id: str
    ) -> "AssemblyAnalysisResultsSchema":
        """
        Create a schema for the assembly pipeline output.

        :param assembly_current_outdir: Path to the directory containing the pipeline output
        :param assembly_id: Assembly ID (e.g. ERZ123456)
        :return: AssemblyAnalysisResultsSchema instance
        """
        # QC folder validation
        qc_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.qc_folder,
            rules=[DirectoryExistsRule],
            files=[
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
                    / f"{assembly_id}_filtered_contigs.fasta.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
                    / f"{assembly_id}.tsv",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
                    / "multiqc_report.html",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
                    / "multiqc_data",
                    rules=[DirectoryExistsRule],
                    glob_rules=[GlobHasFilesRule],
                ),
            ],
            glob_rules=[GlobHasFilesRule],
        )

        # CDS folder validation
        cds_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.cds_folder,
            rules=[DirectoryExistsRule],
            files=[
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
                    / f"{assembly_id}_predicted_orf.ffn.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
                    / f"{assembly_id}_predicted_cds.faa.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
                    / f"{assembly_id}_predicted_cds.gff.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
            ],
            glob_rules=[GlobHasFilesRule],
        )

        # Taxonomy folder validation
        tax_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder,
            rules=[DirectoryExistsRule],
            files=[
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder
                    / f"{assembly_id}_contigs_taxonomy.tsv.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder
                    / f"{assembly_id}.krona.txt",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder
                    / f"{assembly_id}.html",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                ),
                # SSU and LSU files are optional, so we don't validate them
            ],
            glob_rules=[GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule],
        )

        # Functional annotation folder validation
        func_annot_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder,
            rules=[DirectoryExistsRule],
            files=[
                # interpro subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "interpro",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "interpro"
                            / f"{assembly_id}_interproscan.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "interpro"
                            / f"{assembly_id}_interpro_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "interpro"
                            / f"{assembly_id}_interpro_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # pfam subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "pfam",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "pfam"
                            / f"{assembly_id}_pfam_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "pfam"
                            / f"{assembly_id}_pfam_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # go subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "go",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "go"
                            / f"{assembly_id}_go_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "go"
                            / f"{assembly_id}_go_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "go"
                            / f"{assembly_id}_goslim_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "go"
                            / f"{assembly_id}_goslim_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # eggnog subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "eggnog",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "eggnog"
                            / f"{assembly_id}_emapper_seed_orthologs.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "eggnog"
                            / f"{assembly_id}_emapper_annotations.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # kegg subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "kegg",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "kegg"
                            / f"{assembly_id}_ko_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "kegg"
                            / f"{assembly_id}_ko_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # rhea-reactions subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "rhea-reactions",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "rhea-reactions"
                            / f"{assembly_id}_proteins2rhea.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "rhea-reactions"
                            / f"{assembly_id}_proteins2rhea.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # dbcan subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                    / "dbcan",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "dbcan"
                            / f"{assembly_id}_dbcan_cgc.gff.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "dbcan"
                            / f"{assembly_id}_dbcan_standard_out.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "dbcan"
                            / f"{assembly_id}_dbcan_overview.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "dbcan"
                            / f"{assembly_id}_dbcan_sub_hmm.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder
                            / "dbcan"
                            / f"{assembly_id}_dbcan_substrates.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
            ],
            glob_rules=[GlobHasFilesRule],
        )

        # Pathways and systems folder validation
        pathways_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder,
            rules=[DirectoryExistsRule],
            files=[
                # antismash subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                    / "antismash",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "antismash"
                            / f"{assembly_id}_antismash.gbk.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "antismash"
                            / f"{assembly_id}_antismash.gff.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "antismash"
                            / f"{assembly_id}_merged.json",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "antismash"
                            / f"{assembly_id}_antismash_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "antismash"
                            / f"{assembly_id}_antismash_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # sanntis subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                    / "sanntis",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "sanntis"
                            / f"{assembly_id}_sanntis_concatenated.gff.gz",
                            rules=[FileIfExistsIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "sanntis"
                            / f"{assembly_id}_sanntis_summary.tsv.gz",
                            rules=[FileIfExistsIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "sanntis"
                            / f"{assembly_id}_sanntis_summary.tsv.gz.gzi",
                            rules=[FileIfExistsIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # genome-properties subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                    / "genome-properties",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "genome-properties"
                            / f"{assembly_id}_gp.json.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "genome-properties"
                            / f"{assembly_id}_gp.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "genome-properties"
                            / f"{assembly_id}_gp.txt.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # kegg-modules subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                    / "kegg-modules",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "kegg-modules"
                            / f"{assembly_id}_kegg_modules_per_contigs.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "kegg-modules"
                            / f"{assembly_id}_kegg_modules_per_contigs.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "kegg-modules"
                            / f"{assembly_id}_kegg_modules_summary.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "kegg-modules"
                            / f"{assembly_id}_kegg_modules_summary.tsv.gz.gzi",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
                # dram-distill subdirectory
                Directory(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                    / "dram-distill",
                    rules=[DirectoryExistsRule],
                    files=[
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "dram-distill"
                            / f"{assembly_id}_dram.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "dram-distill"
                            / f"{assembly_id}_dram.html.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "dram-distill"
                            / f"{assembly_id}_genome_stats.tsv.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                        File(
                            path=assembly_current_outdir
                            / EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
                            / "dram-distill"
                            / f"{assembly_id}_metabolism_summary.xlsx.gz",
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        ),
                    ],
                    glob_rules=[GlobHasFilesRule],
                ),
            ],
            glob_rules=[GlobHasFilesRule],
        )

        # Annotation summary folder validation
        annotation_summary_dir = Directory(
            path=assembly_current_outdir
            / EMG_CONFIG.assembly_analysis_pipeline.annotation_summary_folder,
            rules=[DirectoryExistsRule],
            files=[
                File(
                    path=assembly_current_outdir
                    / EMG_CONFIG.assembly_analysis_pipeline.annotation_summary_folder
                    / f"{assembly_id}_annotation_summary.gff.gz",
                    rules=[FileExistsRule, FileIsNotEmptyRule],
                )
            ],
            glob_rules=[GlobHasFilesRule],
        )

        return cls(
            qc_dir=qc_dir,
            cds_dir=cds_dir,
            tax_dir=tax_dir,
            func_annot_dir=func_annot_dir,
            pathways_dir=pathways_dir,
            annotation_summary_dir=annotation_summary_dir,
        )
