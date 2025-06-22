import logging
import os
import re
from pathlib import Path

from django.core.management import BaseCommand, CommandError
from django.utils.text import slugify

from analyses.models import Biome
from ..lib.genome_util import (
    sanity_check_genome_output_euks,
    sanity_check_genome_output_proks,
    sanity_check_catalogue_dir,
    find_genome_results,
    get_genome_result_path,
    read_tsv_w_headers,
    read_json,
    apparent_accession_of_genome_dir,
)
from ...models import GenomeCatalogue, GeographicLocation, Genome

logger = logging.getLogger(__name__)

cog_cache = {}
ipr_cache = {}

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)


def validate_pipeline_version(version):
    pattern = r"^v[1-3](\.\d+)*([a-zA-Z0-9\-]+)?$"
    if not re.match(pattern, version):
        raise CommandError(
            f"Invaild pipeline version: {version}. Expected something like 'v1', 'v2.0', 'v3.0.0dev', or 'v3.1.4'"
        )
    return version


class Command(BaseCommand):
    obj_list = list()
    results_directory = None
    genome_folders = None
    catalogue_obj = None
    catalogue_dir = None

    database = None

    def add_arguments(self, parser):
        parser.add_argument(
            "results_directory",
            action="store",
            type=str,
        )
        parser.add_argument(
            "catalogue_directory",
            action="store",
            type=str,
            help="The folder within `results_directory` where the results files are. "
            'e.g. "genomes/skin/1.0/"',
        )
        parser.add_argument(
            "catalogue_name",
            action="store",
            type=str,
            help='The name of this catalogue (without any version label), e.g. "Human Skin"',
        )
        parser.add_argument(
            "catalogue_version",
            action="store",
            type=str,
            help='The version label. E.g. "1.0" or "2021-01"',
        )
        parser.add_argument(
            "gold_biome",
            action="store",
            type=str,
            help="Primary biome for the catalogue, as a GOLD lineage. "
            "E.g. root:Host-Associated:Human:Digestive\\ System:Large\\ intestine",
        )
        parser.add_argument(
            "pipeline_version",
            action="store",
            type=validate_pipeline_version,
            help='Pipeline version tag that catalogue was produced by. E.g. "1.2.1"',
        )
        parser.add_argument(
            "catalogue_type",
            action="store",
            type=str,
            choices=[choice for choice, _ in GenomeCatalogue.CATALOGUE_TYPE_CHOICES],
            help="The type of genomes in the catalogue, e.g. prokaryotes, eukaryotes, viruses",
        )
        parser.add_argument(
            "--update-metadata-only",
            dest="update_metadata_only",
            action="store_true",
            default=False,
            help="Only update the metadata of genomes in an existing catalogue; "
            "i.e. reparse the MGYG*.json files.",
        )
        parser.add_argument("--database", type=str, default="default")
        parser.add_argument(
            "--catalogue_biome_label",
            type=str,
            default="",
            help="A catalogue biome label (e.g. Mouse Gut) which can be used to group together related "
            "catalogues of different types. If none, the catalogue name is used.",
        )

    def handle(self, *args, **options):

        version_str = options["pipeline_version"].strip()
        try:
            major_version_number = int(version_str.lstrip("v").split(".")[0])
        except (ValueError, IndexError):
            raise CommandError(
                f"Invalid pipeline version '{version_str}'. The expected format is v1.x, v2.x, or v3.x"
            )
        if major_version_number not in {1, 2, 3}:
            raise CommandError(
                f"Unsupported pipeline version: v{major_version_number}. Must be 1, 2, or 3."
            )
        self._handle_versioned_catalogue(options, version=major_version_number)

    def _parse_common_options(self, options):
        self.results_directory = os.path.realpath(options["results_directory"].strip())
        if not os.path.exists(self.results_directory):
            raise FileNotFoundError(
                f"Results dir {self.results_directory} does not exist"
            )

        self.catalogue_dir = os.path.join(
            self.results_directory, options["catalogue_directory"].strip()
        )
        self.catalogue_name = options["catalogue_name"].strip()
        self.catalogue_version = options["catalogue_version"].strip()
        self.gold_biome = options["gold_biome"].strip()
        self.pipeline_version = options["pipeline_version"].strip()
        self.catalogue_type = options["catalogue_type"].strip()
        self.catalogue_biome_label = (
            options.get("catalogue_biome_label", "").strip() or self.catalogue_name
        )
        self.database = options.get("database", "default")

        if options.get("update_metadata_only") and self.pipeline_version.startswith(
            "v2"
        ):
            assert GenomeCatalogue.objects.filter(
                catalogue_id=self.make_slug(self.catalogue_name, self.catalogue_version)
            ).exists()

    def _handle_versioned_catalogue(self, options, version):
        self._parse_common_options(options)

        self.catalogue_obj = self.get_catalogue(
            self.catalogue_name,
            self.catalogue_version,
            self.gold_biome,
            self.catalogue_dir,
            self.pipeline_version,
            self.catalogue_type,
            self.catalogue_biome_label,
        )
        genomes = Genome.objects.filter(catalogue=self.catalogue_obj)
        total = genomes.count()
        logger.info(f"IMPORT COMMAND Final Report: {total} genomes imported.")

        # return False

        logger.info("CLI %r" % options)
        genome_dirs = find_genome_results(self.catalogue_dir)
        logger.info(f"Found {len(genome_dirs)} genome dirs to upload")

        sanity_check_map = {
            "eukaryotes": sanity_check_genome_output_euks,
            "prokaryotes": sanity_check_genome_output_proks,
        }
        sanity_check = sanity_check_map.get(self.catalogue_type)
        if sanity_check:
            [sanity_check(genome_dir) for genome_dir in genome_dirs]

        sanity_check_catalogue_dir(self.catalogue_dir)

        for genome_dir in genome_dirs:
            self.upload_dir(
                genome_dir,
                update_metadata_only=options.get("update_metadata_only", False),
            )

        self.upload_catalogue_files()

        if version == 3:
            self._load_catalogue_summary_json()

        self.catalogue_obj.calculate_genome_count()
        self.catalogue_obj.save()

    def _load_catalogue_summary_json(self):
        summary_file = Path(self.catalogue_dir) / "catalogue_summary.json"
        self.catalogue_obj.other_stats = (
            read_json(summary_file) if summary_file.is_file() else {}
        )
        if not summary_file.is_file():
            logger.warning(f"No catalogue summary found at {summary_file}.")
        self.catalogue_obj.save()
        self._prettify_catalogue_summary_field_name()

    def _prettify_catalogue_summary_field_name(self):
        if not isinstance(self.catalogue_obj.other_stats, dict):
            return
        if self.catalogue_obj.catalogue_type == GenomeCatalogue.EUKS:
            if "Clusters with pan-genomes" in self.catalogue_obj.other_stats:
                self.catalogue_obj.other_stats["Clusters with multiple genomes"] = (
                    self.catalogue_obj.other_stats.pop("Clusters with pan-genomes")
                )
        self.catalogue_obj.save()

    def make_slug(self, catalogue_name, catalogue_version):
        return slugify(
            "{0}-v{1}".format(catalogue_name, catalogue_version).replace(".", "-")
        )

    def _ensure_annotations(self, genome):
        if not hasattr(genome, "annotations") or genome.annotations is None:
            genome.annotations = genome.default_annotations()

    def get_catalogue(
        self,
        catalogue_name,
        catalogue_version,
        gold_biome,
        catalogue_dir,
        pipeline_version_tag,
        catalogue_type,
        catalogue_biome_label,
    ):
        logging.warning("GOLD")
        logging.warning(gold_biome)
        biome = self.get_gold_biome(gold_biome)

        catalogue, _ = GenomeCatalogue.objects.using(self.database).get_or_create(
            catalogue_id=self.make_slug(catalogue_name, catalogue_version),
            defaults={
                "version": catalogue_version,
                "name": "{0} v{1}".format(catalogue_name, catalogue_version),
                "biome": biome,
                "result_directory": catalogue_dir,
                "ftp_url": "http://ftp.ebi.ac.uk/pub/databases/metagenomics/mgnify_genomes/",
                "pipeline_version_tag": pipeline_version_tag,
                "catalogue_biome_label": catalogue_biome_label,
                "catalogue_type": catalogue_type,
            },
        )
        return catalogue

    def upload_dir(self, directory, update_metadata_only=False):
        logger.info("Uploading dir: {}".format(directory))
        genome, has_pangenome = self.create_genome(directory)
        if update_metadata_only:
            return
        self.upload_cog_results(genome, directory)
        self.upload_kegg_class_results(genome, directory)
        self.upload_kegg_module_results(genome, directory)
        self.upload_antismash_geneclusters(genome, directory)
        self.upload_genome_files(genome, directory, has_pangenome)

    def get_gold_biome(self, lineage):
        logger.info(f"Getting gold biome for lineage: {lineage}")
        path = Biome.lineage_to_path(lineage)
        biome = Biome.objects.using(self.database).filter(path=path).first()
        logger.info(f"Found gold biome: {biome}")

        if not biome:
            raise Biome.DoesNotExist()
        return biome

    def prepare_genome_data(self, genome_dir):
        genome_dir = read_json(
            os.path.join(
                genome_dir, f"{apparent_accession_of_genome_dir(genome_dir)}.json"
            )
        )

        has_pangenome = "pangenome" in genome_dir
        genome_dir["biome"] = self.get_gold_biome(genome_dir["gold_biome"])

        if "annotations" not in genome_dir:
            genome_dir["annotations"] = Genome.default_annotations()

        if has_pangenome:
            genome_dir.update(genome_dir["pangenome"])
            del genome_dir["pangenome"]
        genome_dir.setdefault("num_genomes_total", 1)

        genome_dir.pop("num_genomes_non_redundant", None)

        if "geographic_origin" in genome_dir:
            genome_dir["geo_origin"] = self.get_geo_location(
                genome_dir["geographic_origin"]
            )
            del genome_dir["geographic_origin"]

        genome_dir.pop("gold_biome", None)

        if "rna_5.8s" in genome_dir:
            genome_dir["rna_5_8s"] = genome_dir["rna_5.8s"]
            del genome_dir["rna_5.8s"]

        return genome_dir, has_pangenome

    def get_geo_location(self, location):
        return GeographicLocation.objects.using(self.database).get_or_create(
            name=location
        )[0]

    def attach_geo_location(self, genome, location):
        genome.pangenome_geographic_range.add(self.get_geo_location(location))

    def create_genome(self, genome_dir):
        data, has_pangenome = self.prepare_genome_data(genome_dir)

        geo_locations = data.get("geographic_range")
        data.pop("geographic_range", None)
        data.pop("genome_accession", None)

        data["result_directory"] = get_genome_result_path(genome_dir)
        data["catalogue"] = self.catalogue_obj
        g, created = Genome.objects.using(self.database).update_or_create(
            accession=data["accession"], defaults=data
        )
        g.save(using=self.database)

        if g.pangenome_geographic_range.exists():
            g.pangenome_geographic_range.clear()

        if geo_locations:
            [self.attach_geo_location(g, loc) for loc in geo_locations]

        return g, has_pangenome

    def _upload_annotation_file(
        self,
        genome,
        directory,
        filename_template,
        subfolder,
        row_parser,
        annotation_key,
    ):
        filepath = os.path.join(
            directory, subfolder, filename_template.format(accession=genome.accession)
        )
        if not os.path.isfile(filepath):
            logger.warning(f"Annotation file not found: {filepath}")
            return

        self._ensure_annotations(genome)
        genome.annotations[annotation_key] = []

        for row in read_tsv_w_headers(filepath):
            genome.annotations[annotation_key].append(row_parser(row))

        genome.save(using=self.database)
        logger.info(f"Loaded {annotation_key} for {genome.accession}")

    def _parse_cog_row(self, row):
        return {"name": row["COG_category"], "count": int(row["Counts"])}

    def _parse_kegg_class_row(self, row):
        return {"class_id": row["KEGG_class"], "count": int(row["Counts"])}

    def _parse_kegg_module_row(self, row):
        return {"name": row["KEGG_module"], "count": int(row["Counts"])}

    def _parse_antismash_row(self, row):
        parts = row.strip().split("\t")
        if len(parts) < 3:
            return None
        cluster = parts[-3]
        features = parts[-2]
        count_val = len(features.split(";")) if features else 0

        return {"name": cluster, "count": count_val, "features": features or ""}

    def upload_cog_results(self, genome, directory):
        self._upload_annotation_file(
            genome,
            directory,
            filename_template="{accession}_cog_summary.tsv",
            subfolder="genome",
            row_parser=self._parse_cog_row,
            annotation_key="cog_categories",
        )

    def upload_kegg_class_results(self, genome, directory):
        self._upload_annotation_file(
            genome,
            directory,
            filename_template="{accession}_kegg_classes.tsv",
            subfolder="genome",
            row_parser=self._parse_kegg_class_row,
            annotation_key="kegg_classes",
        )

    def upload_kegg_module_results(self, genome, directory):
        self._upload_annotation_file(
            genome,
            directory,
            filename_template="{accession}_kegg_modules.tsv",
            subfolder="genome",
            row_parser=self._parse_kegg_module_row,
            annotation_key="kegg_modules",
        )

    def upload_antismash_geneclusters(self, genome, directory):
        file = os.path.join(directory, "genome", "geneclusters.txt")
        if not os.path.exists(file):
            logger.warning(
                "Genome {} does not have antiSMASH geneclusters".format(
                    genome.accession
                )
            )
            return

        self._ensure_annotations(genome)
        genome.annotations[genome.ANTISMASH_GENE_CLUSTERS] = []

        with open(file, "rt") as tsv:
            for line in tsv:
                parsed = self._parse_antismash_row(line)
                if not parsed:
                    continue

                genome.annotations[genome.ANTISMASH_GENE_CLUSTERS].append(parsed)

        genome.save(using=self.database)
        logger.info(f"Loaded Genome AntiSMASH geneclusters for {genome.accession}")

    def upload_genome_files(self, genome, directory, has_pangenome):
        logger.info("Uploading genome files...")
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
            self.upload_file(
                genome,
                desc_label,
                file_format,
                filename,
                group_type=group_type,
                subdir=subdir,
                directory=directory,
                require_existent_and_non_empty=require,
            )

    def upload_catalogue_files(self):
        self.upload_file(
            self.catalogue_obj,
            "Phylogenetic tree of catalogue genomes",
            "json",
            "phylo_tree.json",
        )

    def prepare_downloadable_file(
        self, desc_label, file_format, file_name, group_type=None, subdir=None
    ):
        # TODO: need a way to check all the possible file formats and types
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

        # TODO: need a way to check all the possible file formats and types
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

        return DownloadFile(
            path=relative_path,
            alias=alias,
            download_type=download_type,
            file_type=file_type,
            long_description=desc_label,
            short_description=desc_label,
            download_group=group_type or "catalogue",
        )

    def upload_file(
        self,
        model_instance,
        desc_label,
        file_format,
        file_name,
        *,
        group_type=None,
        subdir=None,
        directory=None,
        require_existent_and_non_empty=False,
    ):
        path_parts = (
            [directory, subdir, file_name]
            if directory
            else [self.catalogue_dir, file_name]
        )
        path = os.path.join(*(p for p in path_parts if p))

        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            if require_existent_and_non_empty:
                raise FileNotFoundError(
                    f"Required file at {path} either missing or empty"
                )
            logger.warning(
                f"File not found or empty at {path}. This is allowable, but will not be uploaded."
            )
            return

        download_file = self.prepare_downloadable_file(
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
            model_instance.save(using=self.database)
