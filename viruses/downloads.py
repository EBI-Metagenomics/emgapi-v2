"""Curated catalogue downloads and the source files used by visualisations."""

from pathlib import Path

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileIndexFile,
    DownloadFileType,
    DownloadType,
)

# Match the catalogue designs; raw tool outputs remain available through FTP.
# Each tuple contains a release-relative path, a card label, and a description.
DOWNLOAD_GROUPS = [
    (
        "viruses.coding_sequences",
        DownloadType.CODING_SEQUENCES,
        [
            (
                "viruses/cluster_representatives/viruses.fasta.gz",
                "Viral representative nucleotide sequences",
                "Nucleotide sequence of each viral cluster representative.",
            ),
            (
                "viruses/cluster_representatives/viruses.faa.gz",
                "Viral representative proteins",
                "Proteins predicted on the viral cluster representatives.",
            ),
            (
                "viruses/cluster_representatives/viruses_final.gff.gz",
                "Annotated viral representative genes",
                "Gene coordinates with taxonomy and functional annotations.",
            ),
        ],
    ),
    (
        "viruses.metadata",
        DownloadType.STATISTICS,
        [
            (
                "viruses/viruses-all-metadata.tsv.gz",
                "Viral sequence metadata",
                "All viral sequences and prophages, including cluster membership, quality and source lineage.",
            ),
            (
                "viruses/viruses-cluster-stats.tsv.gz",
                "Viral cluster sizes",
                "Cluster sizes and representative identifiers for the QC chart.",
            ),
        ],
    ),
    (
        "viruses.viral_taxonomy",
        DownloadType.TAXONOMIC_ANALYSIS,
        [
            (
                "viruses/cluster_representatives/taxonomy/genomad/viruses.tsv",
                "geNomad viral taxonomy",
                "Taxonomic assignments of the viral cluster representatives.",
            ),
            (
                "viruses/cluster_representatives/taxonomy/viphogs/viruses_reps_annotation_taxonomy.tsv",
                "ViPhOG viral taxonomy",
                "Taxonomic assignments from ViPhOG protein profiles.",
            ),
            (
                "viruses/cluster_representatives/taxonomy/genomad/viruses_genomad_taxonomy_counts.tsv",
                "geNomad taxonomy counts",
                "Lineage counts used by the viral taxonomy Sankey.",
            ),
            (
                "viruses/cluster_representatives/taxonomy/viphogs/viruses_viphogs_taxonomy_counts.tsv",
                "ViPhOG taxonomy counts",
                "Lineage counts used by the viral taxonomy Sankey.",
            ),
            (
                "viruses/cluster_representatives/taxonomy/vitap/viruses_vitap_taxonomy_counts.tsv",
                "VITAP taxonomy counts",
                "Lineage counts used by the viral taxonomy Sankey.",
            ),
        ],
    ),
    (
        "viruses.host_taxonomy",
        DownloadType.TAXONOMIC_ANALYSIS,
        [
            (
                "viruses/cluster_representatives/host_detection/iphop/iphop_genome.csv",
                "iPHoP host genomes",
                "Host genome predictions with confidence scores.",
            ),
            (
                "viruses/cluster_representatives/host_detection/iphop/iphop_genus.csv",
                "iPHoP host genera",
                "Host genus predictions with confidence scores.",
            ),
            (
                "viruses/cluster_representatives/host_detection/spacepharer/custom_chosen_host.tsv",
                "CRISPR predicted hosts",
                "SpacePHARER host predictions from CRISPR spacer matches.",
            ),
        ],
    ),
    (
        "viruses.functional_annotation",
        DownloadType.FUNCTIONAL_ANALYSIS,
        [
            (
                "viruses/cluster_representatives/functional_annotation/amr/viruses.tsv",
                "Viral antimicrobial resistance",
                "AMR genes from AMRFinderPlus, DeepARG and RGI.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/VOG_summary.tsv.gz",
                "VOG annotation summary",
                "HMMER hits against VOG profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/PHROG_summary.tsv.gz",
                "PHROG annotation summary",
                "HMMER hits against PHROG profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/PFAM_summary.tsv.gz",
                "Pfam annotation summary",
                "HMMER hits against Pfam profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/COG_summary.tsv.gz",
                "COG annotation summary",
                "HMMER hits against COG profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/PVOG_summary.tsv.gz",
                "pVOG annotation summary",
                "HMMER hits against pVOG profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/hmmer/CAZy_summary.tsv.gz",
                "CAZy annotation summary",
                "HMMER hits against CAZy profiles.",
            ),
            (
                "viruses/cluster_representatives/functional_annotation/lifestyle/viruses.bacphlip.gz",
                "Viral lifestyle",
                "Virulent or temperate lifestyle predicted with BACPHLIP.",
            ),
        ],
    ),
    (
        "viruses.protein_catalogue",
        DownloadType.FUNCTIONAL_ANALYSIS,
        [
            (
                "viruses/cluster_representatives/protein_clusters_phammseqs/viruses_phams.tsv",
                "Viral protein families",
                "Protein family membership and functions from PhaMMseqs.",
            ),
        ],
    ),
    (
        "plasmids.coding_sequences",
        DownloadType.CODING_SEQUENCES,
        [
            (
                "plasmids/cluster_representatives/plasmids.fasta.gz",
                "Plasmid representative nucleotide sequences",
                "Nucleotide sequence of each plasmid cluster representative.",
            ),
            (
                "plasmids/cluster_representatives/plasmids.faa.gz",
                "Plasmid representative proteins",
                "Proteins predicted on the plasmid cluster representatives.",
            ),
            (
                "plasmids/cluster_representatives/plasmids_annotated.gff",
                "Annotated plasmid representative genes",
                "Gene coordinates with AMR and mobility annotations.",
            ),
        ],
    ),
    (
        "plasmids.metadata",
        DownloadType.STATISTICS,
        [
            (
                "plasmids/plasmids-all-metadata.tsv.gz",
                "Plasmid sequence metadata",
                "All plasmids with source genomes, samples, biomes and clusters.",
            ),
        ],
    ),
    (
        "plasmids.functional_annotation",
        DownloadType.FUNCTIONAL_ANALYSIS,
        [
            (
                "plasmids/cluster_representatives/functional_annotation/amr/plasmids.tsv",
                "Plasmid antimicrobial resistance",
                "AMR genes from AMRFinderPlus, DeepARG and RGI.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/mobsuite-typer/mobsuite_typer_report.txt",
                "MOB-suite mobility typing",
                "Replicon, relaxase and mate-pair formation types and predicted mobility.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/mobsuite-typer/plasmids_biomarker_report.txt",
                "MOB-suite biomarkers",
                "Replicon, relaxase, mate-pair formation and oriT biomarkers.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/mobsuite-typer/plasmids_mge_report.txt",
                "MOB-suite mobile genetic elements",
                "Other mobile genetic elements identified on plasmids.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/plasquid/final_report_per_contig.tsv",
                "plaSquid contig annotations",
                "RIP domains, MOB groups and Inc groups summarised per plasmid.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/plasquid/final_report_per_protein.tsv",
                "plaSquid protein annotations",
                "RIP domains, MOB groups and Inc groups for proteins with hits.",
            ),
            (
                "plasmids/cluster_representatives/functional_annotation/plasquid/mobility_classification.tsv",
                "plaSquid mobility classes",
                "Mobility classification from plaSquid evidence.",
            ),
        ],
    ),
]


def catalogue_downloads(directory: Path) -> list[dict]:
    """Register labelled release exports without exposing intermediate files.

    :param directory: The catalogue release root.
    :return: Available curated downloads with relative paths.
    """
    downloads = []
    for group, download_type, files in DOWNLOAD_GROUPS:
        for relative, label, description in files:
            path = directory / relative
            if not path.is_file() or path.is_symlink():
                continue
            suffix = Path(path.name.removesuffix(".gz")).suffix.lower()
            file_type = {
                ".tsv": DownloadFileType.TSV,
                ".csv": DownloadFileType.CSV,
                ".txt": DownloadFileType.TSV,
                ".bacphlip": DownloadFileType.TSV,
                ".gff": DownloadFileType.GFF,
                ".fasta": DownloadFileType.FASTA,
                ".faa": DownloadFileType.FASTA,
            }[suffix]
            indexes = [
                DownloadFileIndexFile(path=relative + "." + kind, index_type=kind)
                for kind in ("gzi", "fai")
                if path.with_name(path.name + "." + kind).is_file()
            ]
            downloads.append(
                DownloadFile(
                    path=relative,
                    alias=relative,
                    file_type=file_type,
                    download_type=download_type,
                    download_group=group,
                    short_description=label,
                    long_description=description,
                    file_size_bytes=path.stat().st_size,
                    index_file=indexes or None,
                ).model_dump(exclude={"parent_identifier"})
            )
    return downloads
