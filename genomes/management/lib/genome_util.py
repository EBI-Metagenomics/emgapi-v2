import os
import glob
import logging
import json
import csv

from analyses.base_models.with_downloads_models import DownloadFileIndexFile

logger = logging.getLogger(__name__)

EXPECTED_CATALOGUE_FILES = {"phylo_tree.json"}


def get_expected_genome_files(accession):
    prefix = accession + "_"
    return {
        prefix + "annotation_coverage.tsv",
        prefix + "cazy_summary.tsv",
        prefix + "cog_summary.tsv",
        prefix + "kegg_classes.tsv",
        prefix + "kegg_modules.tsv",
        accession + ".faa",
        accession + ".fna",
        accession + ".gff",
        prefix + "eggNOG.tsv",
        prefix + "InterProScan.tsv",
    }


EXPECTED_PANGENOME_FILES = {
    "genes_presence-absence.Rtab",
    "pan-genome.fna",
}


def sanity_check_catalogue_dir(d):
    errors = set()
    files = find_catalogue_files(d)
    for f in files:
        try:
            logging.info("Loading file {}".format(f))
            f = os.path.basename(f)
            EXPECTED_CATALOGUE_FILES.remove(f)
        except KeyError:
            logging.warning("Unexpected file {} found in catalogue dir".format(f))

    for f in EXPECTED_CATALOGUE_FILES:
        errors.add("Catalogue file {} is missing from dir".format(f))

    if errors:
        for e in errors:
            logger.error(e)
        logger.error("Validation failed, see errors above")
        # TODO: replace with raise CommandError
        # sys.exit(1)


REQUIRED_JSON_FIELDS_PROKS = {
    "accession",
    "completeness",
    "contamination",
    "eggnog_coverage",
    "gc_content",
    "gold_biome",
    "ipr_coverage",
    "length",
    "n_50",
    "nc_rnas",
    "num_contigs",
    "num_proteins",
    "rna_16s",
    "rna_23s",
    "rna_5s",
    "taxon_lineage",
    "trnas",
    "type",
}

REQUIRED_JSON_FIELDS_EUKS = {
    "accession",
    "completeness",
    "contamination",
    "busco_completeness",
    "eggnog_coverage",
    "gc_content",
    "gold_biome",
    "ipr_coverage",
    "length",
    "n_50",
    "nc_rnas",
    "num_contigs",
    "num_proteins",
    "rna_18s",
    "rna_28s",
    "rna_5s",
    "rna_5.8s",
    "taxon_lineage",
    "trnas",
    "type",
}

REQUIRED_JSON_PANGENOME_FIELDS_PROKS = {
    "pangenome_accessory_size",
    "pangenome_core_size",
    "num_genomes_total",
    "pangenome_size",
}

REQUIRED_JSON_PANGENOME_FIELDS_EUKS = {
    "num_genomes_total",
}


def sanity_check_genome_json_proks(data):
    keys = data.keys()
    missing_req_keys = set(REQUIRED_JSON_FIELDS_PROKS).difference(set(keys))
    if len(missing_req_keys):
        raise ValueError(
            "{} JSON is missing fields: {}".format(
                data.get("accession"), " ".join(missing_req_keys)
            )
        )

    if "pangenome_stats" in data:
        p_data = data["pangenome_stats"]
        p_keys = set(p_data.keys())
        missing_preq_keys = set(REQUIRED_JSON_PANGENOME_FIELDS_PROKS).difference(
            set(p_keys)
        )
        if len(missing_preq_keys):
            raise ValueError(
                "{} JSON is missing fields: {}".format(
                    data["accession"], " ".join(missing_preq_keys)
                )
            )


def sanity_check_genome_json_euks(data):
    keys = data.keys()
    missing_req_keys = set(REQUIRED_JSON_FIELDS_EUKS).difference(set(keys))
    if len(missing_req_keys):
        raise ValueError(
            "{} JSON is missing fields: {}".format(
                data.get("accession"), " ".join(missing_req_keys)
            )
        )

    if "pangenome_stats" in data:
        p_data = data["pangenome_stats"]
        p_keys = set(p_data.keys())
        missing_preq_keys = set(REQUIRED_JSON_PANGENOME_FIELDS_EUKS).difference(
            set(p_keys)
        )
        if len(missing_preq_keys):
            raise ValueError(
                "{} JSON is missing fields: {}".format(
                    data["accession"], " ".join(missing_preq_keys)
                )
            )


def sanity_check_genome_dir(
    accession, d, expected_files_factory=get_expected_genome_files
):
    expected_files = expected_files_factory(accession)
    fs = os.listdir(d)
    missing = expected_files.difference(fs)
    if len(missing):
        missing_files = ", ".join(missing)
        raise ValueError("Files missing in directory {}: {}".format(d, missing_files))


def sanity_check_pangenome_dir(d):
    fs = os.listdir(d)
    missing = EXPECTED_PANGENOME_FILES.difference(fs)
    if len(missing):
        missing_files = ", ".join(missing)
        raise ValueError("Files missing in directory {}: {}".format(d, missing_files))


def apparent_accession_of_genome_dir(d):
    """
    Gets the apparent accession of a genome directory, based on the folder name
    :param d: genome directory
    :return: e.g. MGYG0000000001
    """
    return os.path.basename(os.path.normpath(d))


def sanity_check_genome_output_any(d):
    apparent_accession = apparent_accession_of_genome_dir(d)
    if not os.path.isdir(os.path.join(d, "genome")):
        raise ValueError(f"genome/ directory missing from {d}")
    if not os.path.exists(os.path.join(d, f"{apparent_accession}.json")):
        raise ValueError(f"{apparent_accession}.json missing from {d}")
    return apparent_accession


def sanity_check_genome_output_proks(d):
    apparent_accession = apparent_accession_of_genome_dir(d)
    json_file = os.path.join(d, f"{apparent_accession}.json")
    json_data = read_json(json_file)
    sanity_check_genome_json_proks(json_data)

    genome_dir = os.path.join(d, "genome")
    sanity_check_genome_dir(json_data["accession"], genome_dir)


def sanity_check_genome_output_euks(d):
    apparent_accession = apparent_accession_of_genome_dir(d)
    json_file = os.path.join(d, f"{apparent_accession}.json")
    json_data = read_json(json_file)
    sanity_check_genome_json_euks(json_data)

    genome_dir = os.path.join(d, "genome")
    sanity_check_genome_dir(json_data["accession"], genome_dir)


def read_json(fs):
    with open(fs) as f:
        return json.load(f)


def read_csv_w_headers(fs):
    return read_sep_f(fs, ",")


def read_tsv_w_headers(fs):
    return read_sep_f(fs, "\t")


def read_sep_f(fs, sep=None):
    with open(fs) as f:
        reader = csv.reader(f, skipinitialspace=True, delimiter=sep)
        header = next(reader)
        data = [dict(zip(header, row)) for row in reader]
        return data


def find_genome_results(catalogue_dir):
    listdir = glob.glob(os.path.join(catalogue_dir, "*"))
    return list(filter(os.path.isdir, listdir))


def find_catalogue_files(catalogue_dir):
    listdir = glob.glob(os.path.join(catalogue_dir, "*"))
    return list(filter(os.path.isfile, listdir))


def get_genome_result_path(result_dir):
    return os.path.join("genomes", result_dir.split("genomes/")[-1])


def _ensure_annotations(genome):
    """
    Ensure that the genome has an annotations attribute.

    Args:
        genome: The genome object to check
    """
    if not hasattr(genome, "annotations") or genome.annotations is None:
        genome.annotations = genome.default_annotations()


def _parse_cog_row(row):
    """
    Parse a row from a COG summary file.

    Args:
        row: A dictionary representing a row from a COG summary file

    Returns:
        A dictionary with name and count fields
    """
    return {"name": row["COG_category"], "count": int(row["Counts"])}


def _parse_kegg_class_row(row):
    """
    Parse a row from a KEGG class file.

    Args:
        row: A dictionary representing a row from a KEGG class file

    Returns:
        A dictionary with class_id and count fields
    """
    return {"class_id": row["KEGG_class"], "count": int(row["Counts"])}


def _parse_kegg_module_row(row):
    """
    Parse a row from a KEGG module file.

    Args:
        row: A dictionary representing a row from a KEGG module file

    Returns:
        A dictionary with name and count fields
    """
    return {"name": row["KEGG_module"], "count": int(row["Counts"])}


def _parse_antismash_row(row):
    """
    Parse a row from an antiSMASH geneclusters file.

    Args:
        row: A string representing a row from an antiSMASH geneclusters file

    Returns:
        A dictionary with name, count, and features fields, or None if the row is invalid
    """
    parts = row.strip().split("\t")
    if len(parts) < 3:
        return None

    cluster = parts[-3]
    features = parts[-2]
    count_val = len(features.split(";")) if features else 0

    return {"name": cluster, "count": count_val, "features": features or ""}


def _upload_annotation_file(
    genome,
    directory,
    filename_template,
    subfolder,
    row_parser,
    annotation_key,
    database="default",
):
    """
    Upload an annotation file to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the annotation file
        filename_template: A template for the filename, with {accession} as a placeholder
        subfolder: The subfolder within the directory containing the file
        row_parser: A function to parse each row of the file
        annotation_key: The key to use in the genome's annotations dictionary
        database: The database to use for saving the genome
    """
    filepath = os.path.join(
        directory, subfolder, filename_template.format(accession=genome.accession)
    )
    if not os.path.isfile(filepath):
        logger.warning(f"Annotation file not found: {filepath}")
        return

    _ensure_annotations(genome)
    genome.annotations[annotation_key] = []

    for row in read_tsv_w_headers(filepath):
        genome.annotations[annotation_key].append(row_parser(row))

    genome.save(using=database)
    logger.info(f"Loaded {annotation_key} for {genome.accession}")


def upload_cog_results(genome, directory, database="default"):
    """
    Upload COG results to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the COG results
        database: The database to use for saving the genome
    """
    _upload_annotation_file(
        genome,
        directory,
        filename_template="{accession}_cog_summary.tsv",
        subfolder="genome",
        row_parser=_parse_cog_row,
        annotation_key="cog_categories",
        database=database,
    )


def upload_kegg_class_results(genome, directory, database="default"):
    """
    Upload KEGG class results to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the KEGG class results
        database: The database to use for saving the genome
    """
    _upload_annotation_file(
        genome,
        directory,
        filename_template="{accession}_kegg_classes.tsv",
        subfolder="genome",
        row_parser=_parse_kegg_class_row,
        annotation_key="kegg_classes",
        database=database,
    )


def upload_kegg_module_results(genome, directory, database="default"):
    """
    Upload KEGG module results to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the KEGG module results
        database: The database to use for saving the genome
    """
    _upload_annotation_file(
        genome,
        directory,
        filename_template="{accession}_kegg_modules.tsv",
        subfolder="genome",
        row_parser=_parse_kegg_module_row,
        annotation_key="kegg_modules",
        database=database,
    )


def upload_antismash_geneclusters(genome, directory, database="default"):
    """
    Upload antiSMASH geneclusters to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the antiSMASH geneclusters
        database: The database to use for saving the genome
    """
    file = os.path.join(directory, "genome", "geneclusters.txt")
    if not os.path.exists(file):
        logger.warning(
            f"Genome {genome.accession} does not have antiSMASH geneclusters"
        )
        return

    _ensure_annotations(genome)
    genome.annotations[genome.ANTISMASH_GENE_CLUSTERS] = []

    with open(file, "rt") as tsv:
        for line in tsv:
            parsed = _parse_antismash_row(line)
            if not parsed:
                continue

            genome.annotations[genome.ANTISMASH_GENE_CLUSTERS].append(parsed)

    genome.save(using=database)
    logger.info(f"Loaded Genome AntiSMASH geneclusters for {genome.accession}")


def upload_genome_files(genome, directory, has_pangenome, database="default"):
    """
    Upload genome files to a genome.

    Args:
        genome: The genome object to update
        directory: The directory containing the genome files
        has_pangenome: Whether the genome has a pangenome
        database: The database to use for saving the genome
    """

    logger.info(f"Uploading genome files for {genome.accession}...")
    files_to_upload = [
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
        (
            "Genome antiSMASH Annotation",
            "gff",
            f"{genome.accession}_antismash.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome VIRify Annotation",
            "gff",
            f"{genome.accession}_virify.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome VIRify Regions",
            "tsv",
            f"{genome.accession}_virify_metadata.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome SanntiS Annotation",
            "gff",
            f"{genome.accession}_sanntis.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "EggNog annotation",
            "tsv",
            f"{genome.accession}_eggNOG.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "InterProScan annotation",
            "tsv",
            f"{genome.accession}_InterProScan.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome rRNA Sequence",
            "fasta",
            f"{genome.accession}_rRNAs.fasta",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome AMRFinderPlus Annotation",
            "tsv",
            f"{genome.accession}_amrfinderplus.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome CRISPRCasFinder Annotation",
            "gff",
            f"{genome.accession}_crisprcasfinder.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome CRISPRCasFinder Additional Records",
            "tsv",
            f"{genome.accession}_crisprcasfinder.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome Mobilome Annotation",
            "gff",
            f"{genome.accession}_mobilome.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome dbCAN Annotation",
            "gff",
            f"{genome.accession}_dbcan.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome Defense Finder Annotation",
            "gff",
            f"{genome.accession}_defense_finder.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "Genome GECCO Annotation",
            "gff",
            f"{genome.accession}_gecco.gff",
            "Genome analysis",
            "genome",
            False,
        ),
        (
            "KEGG Pathway Completeness",
            "tsv",
            f"{genome.accession}_kegg_pathways.tsv",
            "Genome analysis",
            "genome",
            False,
        ),
    ]

    if has_pangenome:
        files_to_upload += [
            (
                "Pangenome core genes list",
                "tab",
                "core_genes.txt",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            ),
            (
                "Pangenome DNA sequence",
                "fasta",
                "pan-genome.fna",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            ),
            (
                "Gene Presence / Absence matrix",
                "tsv",
                "gene_presence_absence.Rtab",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            ),
            (
                "Gene Presence / Absence list",
                "csv",
                "gene_presence_absence.csv",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            ),
            (
                "Pairwise Mash distances of conspecific genomes",
                "nwk",
                "mashtree.nwk",
                "Pan-Genome analysis",
                "pan-genome",
                False,
            ),
        ]

    for (
        desc_label,
        file_format,
        filename,
        group_type,
        subdir,
        require,
    ) in files_to_upload:
        upload_file(
            genome,
            desc_label,
            file_format,
            filename,
            group_type=group_type,
            subdir=subdir,
            directory=directory,
            require_existent_and_non_empty=require,
            database=database,
        )


def prepare_downloadable_file(
    desc_label, file_format, file_name, group_type=None, subdir=None
):
    """
    Prepare a downloadable file object.

    Args:
        desc_label: The description label for the file
        file_format: The format of the file
        file_name: The name of the file
        group_type: The group type for the file
        subdir: The subdirectory containing the file

    Returns:
        A DownloadFile object
    """
    from analyses.base_models.with_downloads_models import (
        DownloadFile,
        DownloadType,
        DownloadFileType,
    )

    # File type mapping
    file_type_mapping = {
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
    file_type = file_type_mapping.get(file_format, DownloadFileType.OTHER)

    # Download type mapping
    download_type_mapping = {
        "Genome analysis": DownloadType.GENOME_ANALYSIS,
        "Pan-Genome analysis": DownloadType.GENOME_ANALYSIS,
        "Catalogue summary": DownloadType.GENOME_ANALYSIS,
        "Phylogenetic tree of catalogue genomes": DownloadType.GENOME_ANALYSIS,
    }

    key = group_type or desc_label
    download_type = download_type_mapping.get(key, DownloadType.OTHER)
    alias = os.path.basename(file_name)
    relative_path = os.path.join(subdir or "", file_name)
    download_file = DownloadFile(
        path=relative_path,
        alias=alias,
        download_type=download_type,
        file_type=file_type,
        long_description=desc_label,
        short_description=desc_label,
        download_group=group_type or "catalogue",
    )
    is_fasta = (
        file_type == DownloadFileType.FASTA
        or file_format == "fna"
        or file_format == "faa"
    )

    if file_format == DownloadFileType.FASTA or file_format == DownloadFileType.OTHER:
        if not relative_path.endswith((".fasta", ".fna", ".gff", ".faa")):
            raise ValueError(
                f"Invalid file format for {relative_path}. Expected .fasta, .fna, or .faa"
            )
        download_file.index_file = DownloadFileIndexFile(
            index_type="fai" if is_fasta else "gzi",
            path=f"{relative_path}.fai" if is_fasta else f"{relative_path}.gzi",
        )

    return download_file

    # return DownloadFile(
    #     path=relative_path,
    #     alias=alias,
    #     download_type=download_type,
    #     file_type=file_type,
    #     long_description=desc_label,
    #     short_description=desc_label,
    #     download_group=group_type or "catalogue",
    # )


def upload_file(
    model_instance,
    desc_label,
    file_format,
    file_name,
    *,
    group_type=None,
    subdir=None,
    directory=None,
    require_existent_and_non_empty=False,
    database="default",
):
    """
    Upload a file to a model instance.

    Args:
        model_instance: The model instance to update
        desc_label: The description label for the file
        file_format: The format of the file
        file_name: The name of the file
        group_type: The group type for the file
        subdir: The subdirectory containing the file
        directory: The directory containing the file
        require_existent_and_non_empty: Whether the file is required to exist and be non-empty
        database: The database to use for saving the model instance
    """
    path_parts = (
        [directory, subdir, file_name]
        if directory
        else [model_instance.result_directory, file_name]
    )
    path = os.path.join(*(p for p in path_parts if p))

    if not (os.path.isfile(path) and os.path.getsize(path) > 0):
        if require_existent_and_non_empty:
            raise FileNotFoundError(f"Required file at {path} either missing or empty")
        logger.warning(
            f"File not found or empty at {path}. This is allowable, but will not be uploaded."
        )
        return

    download_file = prepare_downloadable_file(
        desc_label, file_format, file_name, group_type, subdir
    )
    try:
        model_instance.add_download(download_file)
    except FileExistsError:
        logger.warning(
            f"Duplicate download alias found for {download_file.alias}. Updating."
        )
        model_instance.downloads = [
            d
            for d in (model_instance.downloads or [])
            if d.get("alias") != download_file.alias
        ]
        model_instance.save(using=database)
        model_instance.add_download(download_file)
