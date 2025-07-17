import django
import pytest

django.setup()

from analyses.models import Biome
from genomes.models.genome import Genome
from genomes.models.genome_catalogue import GenomeCatalogue


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
        },
    ]

    catalogues_objects = []
    for catalogue in catalogues_list:
        print(f"creating genome catalogue {catalogue['name']}")
        catalogues_objects.append(
            GenomeCatalogue.objects.get_or_create(
                catalogue_id=catalogue["catalogue_id"],
                defaults={
                    "version": catalogue["version"],
                    "name": catalogue["name"],
                    "description": catalogue["description"],
                    "catalogue_biome_label": catalogue["catalogue_biome_label"],
                    "catalogue_type": catalogue["catalogue_type"],
                    "biome": catalogue["biome"],
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
            "biome": top_level_biomes[3],  # Human biome
            "length": 1000000,
            "num_contigs": 100,
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
        },
        {
            "accession": "MGYG000000002",
            "biome": top_level_biomes[3],  # Human biome
            "length": 2000000,
            "num_contigs": 200,
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
            "biome": top_level_biomes[0],  # Root biome
            "length": 3000000,
            "num_contigs": 300,
            "n_50": 30000,
            "gc_content": 0.7,
            "completeness": 98.0,
            "contamination": 1.0,
            "trnas": 30.0,
            "nc_rnas": 20,
            "num_proteins": 3000,
            "eggnog_coverage": 85.0,
            "ipr_coverage": 80.0,
            "taxon_lineage": "Bacteria;Cyanobacteria;Cyanobacteriia",
            "catalogue": genome_catalogues[1],  # Ocean Prokaryotes
            "geographic_origin": geographic_locations[5],  # Oceania
        },
    ]

    genomes_objects = []
    for genome_data in genomes_list:
        print(f"creating genome {genome_data['accession']}")
        genome, created = Genome.objects.get_or_create(
            accession=genome_data["accession"], defaults=genome_data
        )

        if genome_data["accession"] == "MGYG000000001":
            genome.geographic_range = ["Europe", "North America"]
            genome.save()
        elif genome_data["accession"] == "MGYG000000002":
            genome.geographic_range = ["North America", "South America"]
            genome.save()
        elif genome_data["accession"] == "MGYG000000003":
            genome.geographic_range = ["Oceania", "Antarctica"]
            genome.save()

        genomes_objects.append(genome)

    return genomes_objects
