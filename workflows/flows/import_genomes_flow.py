import os
import re
import shlex
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

from activate_django_first import EMG_CONFIG

genome_config = EMG_CONFIG.genomes

from analyses.models import Biome
from genomes.management.lib.genome_util import (
    apparent_accession_of_genome_dir,
    find_genome_results,
    read_json,
    sanity_check_catalogue_dir,
    sanity_check_genome_output_euks,
    sanity_check_genome_output_proks,
    upload_antismash_geneclusters,
    upload_cog_results,
    upload_genome_files,
    upload_kegg_class_results,
    upload_kegg_module_results,
)
from genomes.models import (
    CatalogueGenome,
    Genome,
    GenomeCatalogue,
    GenomeCatalogueSeries,
)
from workflows.data_io_utils.filenames import trailing_slash_ensured_dir
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy


def shell_join(commands: list[str]) -> str:
    return " && ".join(commands)


def shell_quote(value) -> str:
    return shlex.quote(str(value))


def catalogue_dirname_from_results_directory(results_directory: str) -> str:
    return os.path.basename(os.path.dirname(os.path.normpath(results_directory)))


def catalogue_slug_from_options(options: dict) -> str:
    return f"{options['catalogue_name'].replace(' ', '-')}-v{options['catalogue_version'].replace('.', '-')}".lower()


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
        options.get("catalogue_biome_label") or ""
    ).strip() or options["catalogue_name"]
    options["destination_dir_name"] = (
        options.get("destination_dir_name") or ""
    ).strip() or catalogue_dirname_from_results_directory(options["results_directory"])
    options["catalogue_slug"] = (
        options.get("catalogue_slug") or ""
    ).strip() or catalogue_slug_from_options(options)

    return options


def get_catalogue(options):
    biome = Biome.objects.get(path=Biome.lineage_to_path(options["gold_biome"]))

    destination_dir_name = options.get(
        "destination_dir_name"
    ) or catalogue_dirname_from_results_directory(options["results_directory"])
    catalogue_slug = options.get("catalogue_slug") or catalogue_slug_from_options(
        options
    )
    results_path_to_save = f"/{genome_config.genomes_ftp_results_subpath}/{destination_dir_name}/{options['catalogue_version']}"
    catalogue_id = catalogue_slug

    series, _ = GenomeCatalogueSeries.objects.get_or_create(
        catalogue_biome_label=options["catalogue_biome_label"],
        catalogue_type=options["catalogue_type"],
        defaults={
            "name": options["catalogue_name"],
            "biome": biome,
        },
    )
    if series.biome_id != biome.pk:
        raise ValueError(f"Catalogue series {series} already uses a different biome")

    catalogue, _ = GenomeCatalogue.objects.get_or_create(
        catalogue_id=catalogue_id,
        defaults={
            "series": series,
            "version": options["catalogue_version"],
            "name": f"{options['catalogue_name']} v{options['catalogue_version']}",
            "result_directory": results_path_to_save,
            "ftp_url": genome_config.mags_ftp_site,
            "pipeline_version_tag": options["pipeline_version"],
        },
    )
    if catalogue.status in {
        GenomeCatalogue.Status.PUBLISHED,
        GenomeCatalogue.Status.RETIRED,
    }:
        raise ValueError(f"Released catalogue {catalogue} cannot be re-imported")
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
def move_catalogue_files_to_web_results(options: dict, timeout: int = 28800):
    logger = get_run_logger()
    source = trailing_slash_ensured_dir(
        str(Path(options["results_directory"]) / "website")
    )
    target = str(
        Path(EMG_CONFIG.slurm.ftp_results_dir)
        / genome_config.genomes_ftp_results_subpath
        / options["destination_dir_name"]
        / options["catalogue_version"]
    )
    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "move_command": cli_command(["rsync", "-av"]),
            "source": source,
            "target": target,
            "make_target": True,
        },
        job_variables={"partition": EMG_CONFIG.slurm.datamover_partition},
        timeout=timeout,
    )
    logger.info(f"Web results mover flowrun is {flowrun}")


@task
def move_catalogue_files_to_ftp(options: dict, timeout: int = 86400):
    logger = get_run_logger()
    source = trailing_slash_ensured_dir(str(Path(options["results_directory"]) / "ftp"))
    target = str(
        Path(genome_config.ftp_genomes_root)
        / options["destination_dir_name"]
        / f"v{options['catalogue_version']}"
    )
    flowrun = run_deployment(
        name="move-data/move_data_deployment",
        parameters={
            "move_command": cli_command(["rsync", "-av"]),
            "source": source,
            "target": target,
            "make_target": True,
        },
        job_variables={"partition": EMG_CONFIG.slurm.datamover_partition},
        timeout=timeout,
    )
    logger.info(f"FTP mover flowrun is {flowrun}")


@task
def make_cobs_index(options: dict):
    catalogue_slug = options["catalogue_slug"]
    catalogue_dir = (
        Path(genome_config.genome_search_project_dir) / "catalogues" / catalogue_slug
    )
    command = shell_join(
        [
            f"mkdir -p {shell_quote(catalogue_dir)}",
            f"cd {shell_quote(catalogue_dir)}",
            f"singularity run {genome_config.genome_search_singularity_image} -c index create {shell_quote(Path(options['results_directory']) / 'website')} {shell_quote(catalogue_slug)} --fasta_glob_filter '**/MGYG*.fna'",
        ]
    )
    return run_cluster_job(
        name=f"Make COBS index for {catalogue_slug}",
        command=command,
        expected_time=timedelta(hours=24),
        memory="32G",
        working_dir=catalogue_dir,
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
    )


@task
def make_sourmash_sketches(options: dict):
    catalogue_slug = options["catalogue_slug"]
    catalogue_dir = (
        Path(genome_config.genome_search_project_dir) / "catalogues" / catalogue_slug
    )
    sketch_dir = catalogue_dir / "sourmash_sketches"
    command = shell_join(
        [
            "mitload miniconda",
            f"mkdir -p {shell_quote(sketch_dir)}",
            f"cd {shell_quote(catalogue_dir)}",
            f"conda activate {shell_quote(genome_config.sourmash_conda_environment)}",
            f"find {shell_quote(Path(options['results_directory']) / 'website')} -path '*/genome/MGYG*.fna' > sourmash_sketches/all_fasta.txt",
            "sourmash sketch dna --from-file sourmash_sketches/all_fasta.txt --outdir sourmash_sketches --name-from-first",
        ]
    )
    return run_cluster_job(
        name=f"Make Sourmash sketches for {catalogue_slug}",
        command=command,
        expected_time=timedelta(hours=24),
        memory="32G",
        working_dir=catalogue_dir,
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        cpus_per_task=8,
    )


@task
def make_sourmash_index(options: dict):
    catalogue_slug = options["catalogue_slug"]
    catalogue_dir = (
        Path(genome_config.genome_search_project_dir) / "catalogues" / catalogue_slug
    )
    command = shell_join(
        [
            "mitload miniconda",
            f"cd {shell_quote(catalogue_dir)}",
            f"conda activate {shell_quote(genome_config.sourmash_conda_environment)}",
            "cd sourmash_sketches",
            "sourmash index -k 31 --scaled 1000 --dna genome_index *.sig",
        ]
    )
    return run_cluster_job(
        name=f"Make Sourmash index for {catalogue_slug}",
        command=command,
        expected_time=timedelta(hours=24),
        memory="32G",
        working_dir=catalogue_dir,
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        cpus_per_task=8,
    )


@task
def place_cobs_index_on_embassy(options: dict):
    # TODO: move COBS to k8s
    catalogue_slug = options["catalogue_slug"]
    project_dir = Path(genome_config.genome_search_project_dir)
    key = shell_quote(project_dir / genome_config.cobs_search_ssh_key)
    local_index = shell_quote(
        project_dir / "catalogues" / catalogue_slug / f"{catalogue_slug}.cobs_compact"
    )
    host = shell_quote(genome_config.cobs_search_host)
    remote_index = f"{host}:{shell_quote(Path(genome_config.cobs_remote_index_dir) / f'{catalogue_slug}.cobs_compact')}"
    remote_command = shell_join(
        [
            f"grep -qxF '  {catalogue_slug}: {Path(genome_config.cobs_remote_index_dir) / f'{catalogue_slug}.cobs_compact'}' {shell_quote(genome_config.cobs_remote_config_path)} || echo '  {catalogue_slug}: {Path(genome_config.cobs_remote_index_dir) / f'{catalogue_slug}.cobs_compact'}' >> {shell_quote(genome_config.cobs_remote_config_path)}",
            f"sudo systemctl restart {shell_quote(genome_config.cobs_remote_service)}",
            "echo 'COBS was restarted'",
        ]
    )
    command = shell_join(
        [
            f"scp -i {key} {local_index} {remote_index}",
            f"ssh -i {key} {host} {shell_quote(remote_command)}",
        ]
    )
    return run_cluster_job(
        name=f"Place COBS index for {catalogue_slug} on Embassy",
        command=command,
        expected_time=timedelta(hours=2),
        memory="1G",
        working_dir=project_dir,
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
    )


@task
def place_sourmash_signatures(options: dict):
    catalogue_slug = options["catalogue_slug"]
    source = (
        Path(genome_config.genome_search_project_dir)
        / "catalogues"
        / catalogue_slug
        / "sourmash_sketches"
    )
    target = Path(genome_config.sourmash_public_signatures_dir) / catalogue_slug
    command = shell_join(
        [
            f"mkdir -p {shell_quote(target)}",
            f"rsync -av {shell_quote(trailing_slash_ensured_dir(str(source)))} {shell_quote(target)}",
            f"cd {shell_quote(target)}",
            "unzip -o genome_index.sbt.zip",
        ]
    )
    return run_cluster_job(
        name=f"Place Sourmash signatures for {catalogue_slug}",
        command=command,
        expected_time=timedelta(hours=24),
        memory="1G",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        partition=EMG_CONFIG.slurm.datamover_partition,
    )


@task
def release_rnacentral_json(options: dict):
    command = (
        f"cp {shell_quote(Path(options['results_directory']) / 'additional_data' / 'rnacentral')}/*json "
        f"{shell_quote(genome_config.rnacentral_ftp_dir)}"
    )
    return run_cluster_job(
        name=f"Release RNA Central JSON for {options['catalogue_slug']}",
        command=command,
        expected_time=timedelta(hours=8),
        memory="1G",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        partition=EMG_CONFIG.slurm.datamover_partition,
    )


@task
def release_uniprot(options: dict):
    catalogue_slug = options["catalogue_slug"]
    source = Path(options["results_directory"]) / "additional_data" / "uniprot"
    target = Path(genome_config.uniprot_ftp_dir) / catalogue_slug
    command = shell_join(
        [
            f"mkdir -p {shell_quote(target)}",
            f"cp -r {shell_quote(source / 'uniprot-files')} {shell_quote(target)}",
            f"cp {shell_quote(source)}/*metadata.tsv {shell_quote(target)}",
            f"cp {shell_quote(source / 'VERSION.txt')} {shell_quote(target)}",
        ]
    )
    return run_cluster_job(
        name=f"Release UniProt for {catalogue_slug}",
        command=command,
        expected_time=timedelta(hours=8),
        memory="1G",
        environment={},
        resubmit_policy=ResubmitAlwaysPolicy,
        partition=EMG_CONFIG.slurm.datamover_partition,
    )


def run_genome_release_tasks(
    options: dict,
    *,
    run_genome_search_tasks: bool = True,
):
    move_catalogue_files_to_web_results(options)
    move_catalogue_files_to_ftp(options)
    if not run_genome_search_tasks:
        return
    make_cobs_index(options)
    make_sourmash_sketches(options)
    make_sourmash_index(options)
    place_cobs_index_on_embassy(options)
    place_sourmash_signatures(options)


@task
def process_genome_dir(catalogue, genome_dir):
    accession = apparent_accession_of_genome_dir(genome_dir)
    logger = get_run_logger()
    logger.info(f"Processing genome: {accession}")

    genome_data = read_json(os.path.join(genome_dir, f"{accession}.json"))
    has_pangenome = "pangenome" in genome_data

    genome_data["result_directory"] = Path(catalogue.result_directory) / accession
    genome_data["biome"] = Biome.objects.get(
        path=Biome.lineage_to_path(genome_data["gold_biome"])
    )

    identity_data = {
        field.name: genome_data.pop(field.name)
        for field in Genome._meta.fields
        if field.editable and field.name != "accession" and field.name in genome_data
    }
    genome, identity_created = Genome.objects.get_or_create(
        accession=accession, defaults=identity_data
    )
    if not identity_created:
        changed_fields = []
        for field, value in identity_data.items():
            existing = getattr(genome, field)
            if not value or value == existing:
                continue
            if existing:
                raise ValueError(
                    f"Genome {accession} has conflicting identity field {field}: "
                    f"{existing!r} != {value!r}"
                )
            setattr(genome, field, value)
            changed_fields.append(field)
        if changed_fields:
            genome.save(update_fields=changed_fields + ["updated_at"])

    genome_data = CatalogueGenome.clean_data(genome_data)

    catalogue_genome, _ = CatalogueGenome.objects.update_or_create(
        catalogue=catalogue,
        genome=genome,
        defaults=genome_data,
    )

    logger.info(f"Uploaded genome and metadata for {accession}")

    upload_cog_results(catalogue_genome, genome_dir)
    upload_kegg_class_results(catalogue_genome, genome_dir)
    upload_kegg_module_results(catalogue_genome, genome_dir)
    upload_antismash_geneclusters(catalogue_genome, genome_dir)
    upload_genome_files(catalogue_genome, genome_dir, has_pangenome)


@flow(name="import_genomes_flow")
def import_genomes_flow(
    results_directory: str,
    catalogue_name: str,
    catalogue_version: str,
    gold_biome: str,
    pipeline_version: str,
    catalogue_type: str,
    catalogue_biome_label: str | None = None,
    destination_dir_name: str | None = None,
    catalogue_slug: str | None = None,
    run_release_tasks: bool | None = None,
    run_genome_search_tasks: bool = True,
    release_third_party_data: bool = True,
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
        "destination_dir_name": destination_dir_name,
        "catalogue_slug": catalogue_slug,
    }

    options = parse_options(options)

    catalogue = get_catalogue(options)
    catalogue.genomes.all().delete()
    catalogue.downloads = []
    catalogue.other_stats = {}
    catalogue.status = GenomeCatalogue.Status.DRAFT
    catalogue.save(update_fields=["downloads", "other_stats", "status"])

    if run_release_tasks:
        run_genome_release_tasks(
            options,
            run_genome_search_tasks=run_genome_search_tasks,
        )

    upload_catalogue_summary(catalogue, options["catalogue_dir"])
    upload_catalogue_files(catalogue, options["catalogue_dir"])
    genome_dirs = gather_genome_dirs(
        options["catalogue_dir"], options["catalogue_type"]
    )
    for genome_dir in genome_dirs:
        process_genome_dir(catalogue, genome_dir)
    get_run_logger().info(
        f"Processed {len(genome_dirs)} genomes in catalogue {catalogue.name}"
    )
    validate_import_summary(catalogue, expected_count=len(genome_dirs))

    if run_release_tasks and release_third_party_data:
        release_rnacentral_json(options)
        release_uniprot(options)

    catalogue.status = GenomeCatalogue.Status.READY
    catalogue.save(update_fields=["status"])


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
    catalogue.save()


@task
def upload_catalogue_files(catalogue, catalogue_dir):
    logger = get_run_logger()
    from analyses.base_models.with_downloads_models import (
        DownloadFile,
        DownloadFileType,
        DownloadType,
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


def validate_import_summary(catalogue, expected_count):
    logger = get_run_logger()
    genomes = CatalogueGenome.objects.filter(catalogue=catalogue)
    total = genomes.count()
    with_files = genomes.exclude(downloads=[]).count()
    logger.info(f"Final Report: {total} genomes imported. {with_files} with downloads.")
    validation_errors = []
    if total != expected_count:
        validation_errors.append(
            f"Expected {expected_count} genomes but imported {total}."
        )
    if total != with_files:
        validation_errors.append("Some genomes are missing downloads.")
    if validation_errors:
        message = " ".join(validation_errors)
        logger.error(message)
        raise ValueError(message)
