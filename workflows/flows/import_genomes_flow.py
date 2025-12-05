import os
import re
from pathlib import Path

from django.db import close_old_connections
from prefect import task, flow, get_run_logger

from activate_django_first import EMG_CONFIG

genome_config = EMG_CONFIG.genomes

from analyses.models import Biome
from genomes.management.lib.genome_util import (
    find_genome_results,
    sanity_check_genome_output_proks,
    sanity_check_catalogue_dir,
    apparent_accession_of_genome_dir,
    sanity_check_genome_output_euks,
    read_json,
    get_genome_result_path,
    upload_cog_results,
    upload_kegg_class_results,
    upload_kegg_module_results,
    upload_antismash_geneclusters,
    upload_genome_files,
)
from genomes.models import GenomeCatalogue, Genome


def validate_pipeline_version(version: str) -> int:
    match = re.match(r"^v?([1-3])(?:\..*)?(?:[a-zA-Z0-9\-]*)?$", version)
    if not match:
        raise ValueError(f"Invalid pipeline version: {version}")
    return int(match.group(1))


def parse_options(options):
    if not os.path.exists(options["results_directory"]):
        raise FileNotFoundError(
            f"Results dir {options['results_directory']} does not exist"
        )
    options["catalogue_dir"] = os.path.join(options["results_directory"], "website")

    options["catalogue_name"] = options["catalogue_name"].strip()
    options["catalogue_version"] = options["catalogue_version"].strip()
    options["gold_biome"] = options["gold_biome"].strip()
    options["pipeline_version"] = options["pipeline_version"].strip()
    options["catalogue_type"] = options["catalogue_type"].strip()
    options["catalogue_biome_label"] = (
        options.get("catalogue_biome_label", "").strip() or options["catalogue_name"]
    )

    return options


def get_catalogue(options):
    path = Biome.lineage_to_path(options["gold_biome"])
    biome = Biome.objects.filter(path=path).first()
    if not biome:
        raise Biome.DoesNotExist()

    catalogue_dirname = os.path.basename(
        os.path.dirname(os.path.normpath(options["results_directory"]))
    )
    # TODO: Update when transfer service URL root is finalised
    # results_path_to_save = f"{EMG_CONFIG.service_urls.transfer_services_url_root}/genomes/{catalogue_dirname}/{options['catalogue_version']}"
    results_path_to_save = (
        f"/genomes/{catalogue_dirname}/{options['catalogue_version']}"
    )
    logger = get_run_logger()
    logger.info(f"Catalogue results path to save: {results_path_to_save}")

    catalogue_id = f"{options['catalogue_name'].replace(' ', '-')}-v{options['catalogue_version'].replace('.', '-')}".lower()
    catalogue, _ = GenomeCatalogue.objects.get_or_create(
        catalogue_id=catalogue_id,
        defaults={
            "version": options["catalogue_version"],
            "name": f"{options['catalogue_name']} v{options['catalogue_version']}",
            "biome": biome,
            "result_directory": results_path_to_save,
            "ftp_url": genome_config.mags_ftp_site,
            "pipeline_version_tag": options["pipeline_version"],
            "catalogue_biome_label": options["catalogue_biome_label"],
            "catalogue_type": options["catalogue_type"],
        },
    )
    return catalogue


def gather_genome_dirs(catalogue_dir, catalogue_type):
    genome_dirs = find_genome_results(catalogue_dir)

    sanity_check_map = {
        "eukaryotes": sanity_check_genome_output_euks,
        "prokaryotes": sanity_check_genome_output_proks,
    }
    sanity_check = sanity_check_map.get(catalogue_type)
    if sanity_check:
        for d in genome_dirs:
            sanity_check(d)

    sanity_check_catalogue_dir(catalogue_dir)
    return genome_dirs


@task
def process_genome_dir(catalogue, genome_dir):
    accession = apparent_accession_of_genome_dir(genome_dir)
    logger = get_run_logger()
    logger.info(f"Processing genome: {accession}")

    genome_data = read_json(os.path.join(genome_dir, f"{accession}.json"))
    has_pangenome = "pangenome" in genome_data

    path = Biome.lineage_to_path(genome_data["gold_biome"])

    genome_data["catalogue"] = catalogue
    genome_results_path = get_genome_result_path(genome_dir)
    genome_data["result_directory"] = (
        f"{genome_config.results_directory_root}/{genome_results_path.replace('/website/', '/')}"
    )
    genome_data["biome"] = Biome.objects.filter(path=path).first()

    genome_data = Genome.clean_data(genome_data)
    genome_data = Genome.clean_data(genome_data)

    close_old_connections()
    genome, _ = Genome.objects.update_or_create(
        accession=accession, defaults=genome_data
    )

    logger.info(f"Uploaded genome and metadata for {accession}")

    upload_cog_results(genome, genome_dir)
    upload_kegg_class_results(genome, genome_dir)
    upload_kegg_module_results(genome, genome_dir)
    upload_antismash_geneclusters(genome, genome_dir)
    upload_genome_files(genome, genome_dir, has_pangenome)

    return accession


@flow(name="import_genomes_flow")
def import_genomes_flow(
    results_directory: str,
    catalogue_name: str,
    catalogue_version: str,
    gold_biome: str,
    pipeline_version: str,
    catalogue_type: str,
    catalogue_biome_label: str = None,
):
    # Reconstruct options dictionary for backward compatibility with existing functions
    options = {
        "results_directory": results_directory,
        "catalogue_name": catalogue_name,
        "catalogue_version": catalogue_version,
        "gold_biome": gold_biome,
        "pipeline_version": pipeline_version,
        "catalogue_type": catalogue_type,
        "catalogue_biome_label": catalogue_biome_label,
    }

    options = parse_options(options)
    catalogue = get_catalogue(options)
    upload_catalogue_summary(catalogue, options["catalogue_dir"])
    upload_catalogue_files(catalogue, options["catalogue_dir"])
    genome_dirs = gather_genome_dirs(
        options["catalogue_dir"], options["catalogue_type"]
    )
    genome_accessions = []
    for genome_dir in genome_dirs:
        close_old_connections()
        genome_accession = process_genome_dir(catalogue, genome_dir)
        genome_accessions.append(genome_accession)
    logger = get_run_logger()
    logger.info(
        f"Processed {len(genome_accessions)} genomes in catalogue {catalogue.name}"
    )
    validate_import_summary(catalogue)


@task
def upload_catalogue_summary(catalogue, catalogue_dir):
    logger = get_run_logger()
    summary_file = Path(catalogue_dir) / "catalogue_summary.json"
    if summary_file.is_file():
        catalogue.other_stats = read_json(summary_file)
        logger.info(f"Uploaded catalogue summary from {summary_file}")
    else:
        catalogue.other_stats = {}
        logger.warning(f"No catalogue summary found at {summary_file}")
        # logger.error(f"No catalogue summary found at {summary_file}")
    catalogue.save()


@task
def upload_genome_downloads(genome, genome_dir, has_pangenome):
    logger = get_run_logger()
    from analyses.base_models.with_downloads_models import (
        DownloadFile,
        DownloadType,
        DownloadFileType,
    )

    genome_file_specs = [
        (
            "Predicted CDS (aa)",
            "fasta",
            f"{genome.accession}.faa",
            "Genome analysis",
            "genome",
            True,
        ),
        (
            "Nucleic Acid Sequence",
            "fasta",
            f"{genome.accession}.fna",
            "Genome analysis",
            "genome",
            True,
        ),
        (
            "Nucleic Acid Sequence index",
            "fai",
            f"{genome.accession}.fna.fai",
            "Genome analysis",
            "genome",
            True,
        ),
        (
            "Genome Annotation",
            "gff",
            f"{genome.accession}.gff",
            "Genome analysis",
            "genome",
            True,
        ),
    ]

    if has_pangenome:
        genome_file_specs.append(
            (
                "Pangenome core genes list",
                "tab",
                "core_genes.txt",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            )
        )

    for (
        desc_label,
        file_format,
        filename,
        group_type,
        subdir,
        required,
    ) in genome_file_specs:
        filepath = os.path.join(genome_dir, subdir, filename)
        if not (os.path.isfile(filepath) and os.path.getsize(filepath) > 0):
            if required:
                logger.error(f"Required file missing or empty: {filepath}")
            continue
        file_type_map = {
            "fasta": DownloadFileType.FASTA,
            "fna": DownloadFileType.FASTA,
            "fai": DownloadFileType.OTHER,
            "gff": DownloadFileType.OTHER,
            "tsv": DownloadFileType.TSV,
            "csv": DownloadFileType.CSV,
            "json": DownloadFileType.JSON,
            "tab": DownloadFileType.TSV,
            "nwk": DownloadFileType.TREE,
        }
        download_file = DownloadFile(
            path=os.path.join(subdir, filename),
            alias=filename,
            download_type=DownloadType.GENOME_ANALYSIS,
            file_type=file_type_map.get(file_format, DownloadFileType.OTHER),
            long_description=desc_label,
            short_description=desc_label,
            download_group=group_type,
        )
        try:
            genome.add_download(download_file)
            logger.info(f"Attached {filename} to genome {genome.accession}")
        except FileExistsError:
            logger.warning(
                f"Download file already exists for {filename} on genome {genome.accession}"
            )


@task
def upload_catalogue_files(catalogue, catalogue_dir):
    logger = get_run_logger()
    from analyses.base_models.with_downloads_models import (
        DownloadFile,
        DownloadType,
        DownloadFileType,
    )

    summary_path = Path(catalogue_dir) / "phylo_tree.json"
    if summary_path.is_file():
        download_file = DownloadFile(
            path=str(summary_path.relative_to(catalogue_dir)),
            alias="phylo_tree.json",
            download_type=DownloadType.GENOME_ANALYSIS,
            file_type=DownloadFileType.JSON,
            long_description="Phylogenetic tree of catalogue genomes",
            short_description="Phylogenetic tree",
            download_group="catalogue",
        )
        try:
            catalogue.add_download(download_file)
            logger.info("Catalogue phylogenetic tree file uploaded.")
        except FileExistsError:
            logger.warning(
                "Duplicate phylogenetic tree file detected. Skipping upload."
            )

    else:
        logger.warning("No phylogenetic tree file found in catalogue directory.")


def validate_import_summary(catalogue):
    logger = get_run_logger()
    genomes = Genome.objects.filter(catalogue=catalogue)
    total = genomes.count()
    with_annot = sum(1 for g in genomes if g.annotations)
    with_files = sum(1 for g in genomes if g.downloads)
    logger.info(
        f"Final Report: {total} genomes imported. {with_annot} with annotations, {with_files} with downloads."
    )
    if total != with_annot:
        logger.error("Some genomes are missing annotations!")
    if total != with_files:
        logger.error("Some genomes are missing downloads!")
