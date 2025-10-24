from activate_django_first import EMG_CONFIG
import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFileType,
    DownloadType,
)

# Maybe we should put these in a separate schema with stuff
from mgnify_pipelines_toolkit.schemas.dataframes import (
    InterProSummarySchema,
    PFAMSummarySchema,
    GOSummarySchema,
    KOSummarySchema,
    AntismashSummarySchema,
    SanntisSummarySchema,
    KEGGModulesSummarySchema,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
    FileIfExistsIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule,
)
from .base import (
    PipelineFileSchema,
    PipelineDirectorySchema,
    PipelineResultSchema,
    DownloadFileMetadata,
)


class AssemblyResultSchema(PipelineResultSchema):
    """
    Assembly pipeline result schema with a predefined structure.

    This class defines the complete schema for Assembly pipeline v6 results,
    including all expected directories and files.
    """

    def __init__(self):
        """
        Initialize the Assembly pipeline result schema.

        Creates the complete schema definition for Assembly pipeline v6 results,
        including all expected directories and files with their validation rules.
        """
        # QC Directory
        qc_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.qc_folder,
            validation_rules=[DirectoryExistsRule],
            files=[
                PipelineFileSchema(
                    filename_template="{identifier}_filtered_contigs.fasta.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.FASTA,
                        download_type=DownloadType.QUALITY_CONTROL,
                        download_group="quality_control",
                        short_description="Filtered contigs FASTA file",
                        long_description="Filtered contigs FASTA file used in downstream pipelines",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="{identifier}.tsv",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.QUALITY_CONTROL,
                        download_group="quality_control",
                        short_description="Assembly QC metrics",
                        long_description="Quality control metrics for the assembly",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="multiqc_report.html",
                    validation_rules=[FileExistsRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.HTML,
                        download_type=DownloadType.QUALITY_CONTROL,
                        download_group="quality_control",
                        short_description="MultiQC quality control report",
                        long_description="MultiQC webpage showing quality control steps and metrics",
                    ),
                ),
            ],
        )

        # CDS Directory
        cds_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.cds_folder,
            validation_rules=[DirectoryExistsRule],
            files=[
                PipelineFileSchema(
                    filename_template="{identifier}_predicted_orf.ffn.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.FASTA,
                        download_type=DownloadType.CODING_SEQUENCES,
                        download_group=analyses.models.Analysis.CODING_SEQUENCES,
                        short_description="Predicted ORF nucleotide sequences",
                        long_description="Predicted ORF nucleotide sequences",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="{identifier}_predicted_cds.faa.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.FASTA,
                        download_type=DownloadType.CODING_SEQUENCES,
                        download_group=analyses.models.Analysis.CODING_SEQUENCES,
                        short_description="Predicted CDS proteins",
                        long_description="Predicted CDS proteins",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="{identifier}_predicted_cds.gff.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.CODING_SEQUENCES,
                        download_group=analyses.models.Analysis.CODING_SEQUENCES,
                        short_description="Predicted CDS",
                        long_description="Predicted CDS annotations",
                    ),
                ),
            ],
        )

        # Taxonomy Directory
        taxonomy_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.taxonomy_folder,
            validation_rules=[DirectoryExistsRule],
            glob_rules=[GlobOfTaxonomyFolderHasHtmlAndKronaTxtRule],
            files=[
                PipelineFileSchema(
                    filename_template="{identifier}_contigs_taxonomy.tsv.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.TAXONOMIC_ANALYSIS,
                        download_group="taxonomy",
                        short_description="Contig taxonomy table",
                        long_description="Taxonomic assignments for contigs",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="{identifier}.krona.txt.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.TSV,
                        download_type=DownloadType.TAXONOMIC_ANALYSIS,
                        download_group="taxonomy",
                        short_description="Krona taxonomy table",
                        long_description="Taxonomy table for Krona visualization",
                    ),
                ),
                PipelineFileSchema(
                    filename_template="{identifier}.html",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.HTML,
                        download_type=DownloadType.TAXONOMIC_ANALYSIS,
                        download_group="taxonomy",
                        short_description="Krona taxonomy visualization",
                        long_description="Interactive Krona taxonomy chart",
                    ),
                ),
            ],
        )

        # Functional Annotation Directory with subdirectories
        functional_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.functional_annotation_folder,
            validation_rules=[DirectoryExistsRule],
            subdirectories=[
                # InterPro subdirectory
                PipelineDirectorySchema(
                    folder_name="interpro",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_interproscan.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.interpro",
                                short_description="InterProScan results",
                                long_description="InterProScan functional annotation results",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_interpro_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.interpro",
                                short_description="InterPro Identifier counts",
                                long_description="Table with counts for each InterPro identifier found",
                            ),
                            content_validator=InterProSummarySchema,
                        ),
                    ],
                ),
                # Pfam subdirectory
                PipelineDirectorySchema(
                    folder_name="pfam",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_pfam_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.pfams",
                                short_description="Pfam accession counts",
                                long_description="Table with counts for each Pfam accession found",
                            ),
                            content_validator=PFAMSummarySchema,
                        ),
                    ],
                ),
                # GO subdirectory
                PipelineDirectorySchema(
                    folder_name="go",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_go_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.go_slims",
                                short_description="GO Term counts",
                                long_description="Table with counts for each Gene Ontology (GO) Term found",
                            ),
                            content_validator=GOSummarySchema,
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_goslim_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.go_slims",
                                short_description="GO-Slim Term counts",
                                long_description="Table with counts for each Gene Ontology (GO)-Slim Term found",
                            ),
                            content_validator=GOSummarySchema,
                        ),
                    ],
                ),
                # Rhea subdirectory
                PipelineDirectorySchema(
                    folder_name="rhea-reactions",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_proteins2rhea.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.rhea_reactions",
                                short_description="Rhea reaction counts",
                                long_description="Table with counts of each Rhea reaction found",
                            ),
                        ),
                    ],
                ),
                # EggNOG subdirectory
                PipelineDirectorySchema(
                    folder_name="eggnog",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_emapper_seed_orthologs.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.eggnog",
                                short_description="EggNOG seed orthologs",
                                long_description="EggNOG seed ortholog assignments",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_emapper_annotations.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.eggnog",
                                short_description="EggNOG annotations",
                                long_description="EggNOG functional annotations",
                            ),
                        ),
                    ],
                ),
                # KEGG subdirectory
                PipelineDirectorySchema(
                    folder_name="kegg",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_ko_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg",
                                short_description="KEGG KO summary",
                                long_description="KEGG Orthology assignments summary",
                            ),
                            content_validator=KOSummarySchema,
                        ),
                    ],
                ),
                # dbCAN subdirectory
                PipelineDirectorySchema(
                    folder_name="dbcan",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_dbcan_cgc.gff.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.GFF,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                                short_description="dbCAN CGC annotations",
                                long_description="dbCAN carbohydrate-active enzyme gene cluster annotations",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_dbcan_standard_out.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                                short_description="dbCAN standard output",
                                long_description="dbCAN standard analysis output",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_dbcan_overview.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                                short_description="dbCAN overview",
                                long_description="dbCAN analysis overview",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_dbcan_sub_hmm.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                                short_description="dbCAN sub-HMM results",
                                long_description="dbCAN sub-HMM analysis results",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_dbcan_substrates.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dbcan",
                                short_description="dbCAN substrates",
                                long_description="dbCAN predicted substrates",
                            ),
                        ),
                    ],
                ),
            ],
        )

        # Pathways and Systems Directory with subdirectories
        pathways_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder,
            validation_rules=[DirectoryExistsRule],
            subdirectories=[
                # antiSMASH subdirectory
                PipelineDirectorySchema(
                    folder_name="antismash",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_antismash.gbk.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.OTHER,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.antismash",
                                short_description="antiSMASH GenBank output",
                                long_description="antiSMASH biosynthetic gene cluster predictions in GenBank format",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_antismash.gff.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.GFF,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.antismash",
                                short_description="antiSMASH GFF output",
                                long_description="antiSMASH biosynthetic gene cluster predictions in GFF format",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_antismash_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.antismash",
                                short_description="antiSMASH BGC counts",
                                long_description="Table with counts for each BGC found",
                            ),
                            content_validator=AntismashSummarySchema,
                        ),
                    ],
                ),
                # SanntiS subdirectory
                PipelineDirectorySchema(
                    folder_name="sanntis",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_sanntis_concatenated.gff.gz",
                            validation_rules=[FileIfExistsIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.GFF,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.sanntis",
                                short_description="SanntiS GFF output",
                                long_description="SanntiS biosynthetic gene cluster predictions",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_sanntis_summary.tsv.gz",
                            validation_rules=[FileIfExistsIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.sanntis",
                                short_description="SanntiS BGC counts",
                                long_description="Table with counts for each BGC found",
                            ),
                            content_validator=SanntisSummarySchema,
                        ),
                    ],
                ),
                # Genome Properties subdirectory
                PipelineDirectorySchema(
                    folder_name="genome-properties",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_gp.json.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.OTHER,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.genome_properties",
                                short_description="Genome Properties JSON",
                                long_description="Genome Properties results in JSON format",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_gp.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.genome_properties",
                                short_description="Genome Properties counts",
                                long_description="Table with counts for each Genome Property found",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_gp.txt.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.OTHER,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.genome_properties",
                                short_description="Genome Properties text output",
                                long_description="Genome Properties results in text format",
                            ),
                        ),
                    ],
                ),
                # KEGG Modules subdirectory
                PipelineDirectorySchema(
                    folder_name="kegg-modules",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_kegg_modules_per_contigs.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg_modules",
                                short_description="KEGG Modules per contig",
                                long_description="KEGG Modules found per contig",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_kegg_modules_summary.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.kegg_modules",
                                short_description="KEGG Modules counts",
                                long_description="Table with counts for each KEGG Module found",
                            ),
                            content_validator=KEGGModulesSummarySchema,
                        ),
                    ],
                ),
                # DRAM Distill subdirectory
                PipelineDirectorySchema(
                    folder_name="dram-distill",
                    validation_rules=[DirectoryExistsRule],
                    files=[
                        PipelineFileSchema(
                            filename_template="{identifier}_dram.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                                short_description="DRAM Distill results",
                                long_description="Table with DRAM Distill results",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_dram.html.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.HTML,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                                short_description="DRAM Distill HTML report",
                                long_description="DRAM Distill HTML visualization",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_genome_stats.tsv.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.TSV,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                                short_description="DRAM genome statistics",
                                long_description="DRAM genome statistics table",
                            ),
                        ),
                        PipelineFileSchema(
                            filename_template="{identifier}_metabolism_summary.xlsx.gz",
                            validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                            download_metadata=DownloadFileMetadata(
                                file_type=DownloadFileType.OTHER,
                                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                                download_group=f"{analyses.models.Analysis.FUNCTIONAL_ANNOTATION}.dram_distill",
                                short_description="DRAM metabolism summary",
                                long_description="DRAM metabolism summary spreadsheet",
                            ),
                        ),
                    ],
                ),
            ],
        )

        # Annotation Summary Directory
        annotation_summary_dir = PipelineDirectorySchema(
            folder_name=EMG_CONFIG.assembly_analysis_pipeline.annotation_summary_folder,
            validation_rules=[DirectoryExistsRule],
            files=[
                PipelineFileSchema(
                    filename_template="{identifier}_annotation_summary.gff.gz",
                    validation_rules=[FileExistsRule, FileIsNotEmptyRule],
                    download_metadata=DownloadFileMetadata(
                        file_type=DownloadFileType.GFF,
                        download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                        download_group="annotation_summary",
                        short_description="Annotation summary GFF",
                        long_description="Comprehensive annotation summary in GFF format",
                    ),
                ),
            ],
        )

        # Initialize parent class
        super().__init__(
            pipeline_name="Assembly",
            pipeline_version="v6",
            directories=[
                qc_dir,
                cds_dir,
                taxonomy_dir,
                functional_dir,
                pathways_dir,
                annotation_summary_dir,
            ],
        )
