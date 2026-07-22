from pathlib import Path

import django
import pytest
from django.conf import settings

django.setup()

from analyses.models import Biome
from genomes.management.lib.genome_util import (
    read_json,
    upload_file,
    upload_genome_files,
)
from genomes.models import (
    CatalogueGenome,
    Genome,
    GenomeCatalogue,
    GenomeCatalogueSeries,
)
from genomes.models.genome import COG_CATEGORIES, KEGG_CLASSES

OCEAN_CATALOGUE_FIXTURE_ROOT = (
    Path(settings.EMG_CONFIG.genomes.results_directory_root) / "ocean-prokaryotes"
)


@pytest.fixture
def genome_catalogues(top_level_biomes):
    root_biome = Biome.objects.get(path="root")

    catalogues_list = [
        {
            "catalogue_id": "human-gut-prokaryotes",
            "version": "1.0",
            "name": "Human Gut Prokaryotes",
            "description": "Prokaryotic genomes from human gut environments",
            "catalogue_biome_label": "Human Gut",
            "catalogue_type": GenomeCatalogue.PROK,
            "biome": top_level_biomes[3],  # Human biome
        },
        {
            "catalogue_id": "ocean-prokaryotes",
            "version": "1.0",
            "name": "Ocean Prokaryotes",
            "description": "Prokaryotic genomes from ocean environments",
            "catalogue_biome_label": "Ocean",
            "catalogue_type": GenomeCatalogue.PROK,
            "biome": root_biome,
            "result_directory": "/mgnify_genomes/ocean-prokaryotes/1.0",
        },
    ]

    catalogues_objects = []
    for catalogue in catalogues_list:
        print(f"creating genome catalogue {catalogue['name']}")
        series, _ = GenomeCatalogueSeries.objects.get_or_create(
            catalogue_biome_label=catalogue["catalogue_biome_label"],
            catalogue_type=catalogue["catalogue_type"],
            defaults={
                "name": catalogue["name"],
                "biome": catalogue["biome"],
            },
        )
        catalogues_objects.append(
            GenomeCatalogue.objects.get_or_create(
                catalogue_id=catalogue["catalogue_id"],
                defaults={
                    "series": series,
                    "version": catalogue["version"],
                    "name": catalogue["name"],
                    "description": catalogue["description"],
                    "status": GenomeCatalogue.Status.PUBLISHED,
                    "result_directory": catalogue.get("result_directory"),
                },
            )[0]
        )
    return catalogues_objects


@pytest.fixture
def geographic_locations():
    return [
        "Europe",
        "North America",
        "South America",
        "Asia",
        "Africa",
        "Oceania",
        "Antarctica",
    ]


@pytest.fixture
def genomes(top_level_biomes, genome_catalogues, geographic_locations):
    genomes_list = [
        {
            "accession": "MGYG000000001",
            "ena_sample_accession": "SAMEA1",
            "ena_genome_accession": "GCA1",
            "ena_study_accession": "ERP1",
            "biome": top_level_biomes[3],  # Human biome
            "length": 1000000,
            "num_contigs": 100,
            "num_genomes_total": 1,
            "n_50": 10000,
            "gc_content": 0.5,
            "type": Genome.GenomeType.MAG,
            "completeness": 95.0,
            "contamination": 2.0,
            "trnas": 20.0,
            "nc_rnas": 10,
            "num_proteins": 1000,
            "eggnog_coverage": 80.0,
            "ipr_coverage": 75.0,
            "taxon_lineage": "Bacteria;Proteobacteria;Gammaproteobacteria",
            "catalogue": genome_catalogues[0],  # Human Gut Prokaryotes
            "geographic_origin": geographic_locations[0],  # Europe
            "annotations": {
                COG_CATEGORIES: [
                    {"name": "C", "count": 10},
                    {"name": "J", "count": 20},
                    {"name": "X", "count": 5},
                ],
                KEGG_CLASSES: [
                    {"class_id": "09102", "count": 81},
                    {"class_id": "09100", "count": 50},
                ],
            },
        },
        {
            "accession": "MGYG000000002",
            "ncbi_study_accession": "SRP1",
            "biome": top_level_biomes[3],  # Human biome
            "length": 2000000,
            "num_contigs": 200,
            "num_genomes_total": 2,
            "n_50": 20000,
            "gc_content": 0.6,
            "type": Genome.GenomeType.MAG,
            "completeness": 90.0,
            "contamination": 3.0,
            "trnas": 25.0,
            "nc_rnas": 15,
            "num_proteins": 2000,
            "eggnog_coverage": 75.0,
            "ipr_coverage": 70.0,
            "taxon_lineage": "Bacteria;Firmicutes;Bacilli",
            "catalogue": genome_catalogues[0],  # Human Gut Prokaryotes
            "geographic_origin": geographic_locations[1],  # North America
        },
        {
            "accession": "MGYG000000003",
            "ena_sample_accession": "SAMEA000000003",
            "ena_genome_accession": "GCA_000000003.1",
            "ena_study_accession": "ERP000003",
            "biome": top_level_biomes[0],  # Root biome
            "length": 3000000,
            "num_contigs": 300,
            "num_genomes_total": 30,
            "n_50": 30000,
            "gc_content": 40.1,
            "type": Genome.GenomeType.MAG,
            "completeness": 98.0,
            "contamination": 1.0,
            "trnas": 30.0,
            "nc_rnas": 20,
            "num_proteins": 3000,
            "eggnog_coverage": 85.0,
            "ipr_coverage": 80.0,
            "taxon_lineage": "d__Bacteria;p__Cyanobacteriota;c__Cyanobacteriia;o__Synechococcales;f__Cyanobiaceae;g__Synechococcus;s__Synechococcus sp000000003",
            "catalogue": genome_catalogues[1],  # Ocean Prokaryotes
            "geographic_origin": "North Atlantic Ocean",
        },
        {
            "accession": "MGYG000000004",
            "ena_sample_accession": "SAMEA000000004",
            "ena_genome_accession": "GCA_000000004.1",
            "ena_study_accession": "ERP000003",
            "biome": top_level_biomes[0],  # Root biome
            "length": 2500000,
            "num_contigs": 180,
            "num_genomes_total": 1,
            "n_50": 22000,
            "gc_content": 51.4,
            "type": Genome.GenomeType.MAG,
            "completeness": 91.2,
            "contamination": 2.1,
            "trnas": 24.0,
            "nc_rnas": 12,
            "num_proteins": 2500,
            "eggnog_coverage": 78.5,
            "ipr_coverage": 73.5,
            "taxon_lineage": "d__Bacteria;p__Pseudomonadota;c__Alphaproteobacteria;o__Pelagibacterales;f__Pelagibacteraceae;g__Pelagibacter;s__Pelagibacter sp000000004",
            "catalogue": genome_catalogues[1],  # Ocean Prokaryotes
            "geographic_origin": "South Pacific Ocean",
        },
    ]

    genomes_objects = []
    for genome_data in genomes_list:
        genome_data = dict(genome_data)
        print(f"creating genome {genome_data['accession']}")
        accession = genome_data.pop("accession")
        catalogue = genome_data.pop("catalogue")
        identity_fields = {
            "ena_genome_accession",
            "ena_sample_accession",
            "ena_study_accession",
            "ncbi_genome_accession",
            "ncbi_sample_accession",
            "ncbi_study_accession",
            "img_genome_accession",
            "patric_genome_accession",
        }
        identity_data = {
            field: genome_data.pop(field)
            for field in identity_fields
            if field in genome_data
        }
        genome, created = Genome.objects.get_or_create(
            accession=accession, defaults=identity_data
        )
        catalogue_genome, _ = CatalogueGenome.objects.get_or_create(
            genome=genome,
            catalogue=catalogue,
            defaults=genome_data,
        )

        if accession == "MGYG000000001":
            catalogue_genome.geographic_range = ["Europe", "North America"]
            catalogue_genome.save()
        elif accession == "MGYG000000002":
            catalogue_genome.geographic_range = ["North America", "South America"]
            catalogue_genome.save()
        elif accession == "MGYG000000003":
            catalogue_genome.geographic_range = ["Oceania", "Antarctica"]
            catalogue_genome.save()
        elif accession == "MGYG000000004":
            catalogue_genome.geographic_range = ["Oceania"]
            catalogue_genome.save()

        genomes_objects.append(genome)

    return genomes_objects


@pytest.fixture
def real_genome_catalogue_files(genomes, genome_catalogues):
    """Attach downloads backed by the compact on-disk v1.0 fixture."""
    catalogue = next(
        catalogue
        for catalogue in genome_catalogues
        if catalogue.catalogue_id == "ocean-prokaryotes"
    )
    website = OCEAN_CATALOGUE_FIXTURE_ROOT / "v1.0/website"
    catalogue.other_stats = read_json(website / "catalogue_summary.json")
    catalogue.save(update_fields=["other_stats"])
    upload_file(
        catalogue,
        "Phylogenetic tree of catalogue genomes",
        "json",
        "phylo_tree.json",
        directory=website,
        require_existent_and_non_empty=True,
    )

    entries = []
    for accession in ("MGYG000000003", "MGYG000000004"):
        entry = CatalogueGenome.objects.get(
            catalogue=catalogue,
            genome__accession=accession,
        )
        entry.result_directory = f"{catalogue.result_directory}/{accession}"
        entry.save(update_fields=["result_directory"])
        upload_genome_files(entry, website / accession, has_pangenome=False)
        entries.append(entry)
    return entries
