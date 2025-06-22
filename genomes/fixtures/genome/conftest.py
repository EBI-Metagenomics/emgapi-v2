import django
import pytest

django.setup()

from analyses.models import Biome
from genomes.models.genome import Genome
from genomes.models.genome_catalogue import GenomeCatalogue
from genomes.models.geographic_location import GeographicLocation


@pytest.fixture
def geographic_locations():
    locations_list = [
        {"name": "Europe"},
        {"name": "North America"},
        {"name": "South America"},
        {"name": "Asia"},
        {"name": "Africa"},
        {"name": "Oceania"},
        {"name": "Antarctica"},
    ]
    locations_objects = []
    for location in locations_list:
        print(f"creating geographic location {location['name']}")
        locations_objects.append(
            GeographicLocation.objects.get_or_create(name=location["name"])[0]
        )
    return locations_objects


@pytest.fixture
def genome_catalogues(top_level_biomes):
    # Get the root biome
    root_biome = Biome.objects.get(path="root")

    # Create a few genome catalogues
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
def genomes(top_level_biomes, genome_catalogues, geographic_locations):
    # Create a few genomes
    genomes_list = [
        {
            "accession": "MGYG000000001",
            "biome": top_level_biomes[3],  # Human biome
            "length": 1000000,
            "num_contigs": 100,
            "n_50": 10000,
            "gc_content": 0.5,
            "type": Genome.MAG,
            "completeness": 95.0,
            "contamination": 2.0,
            "trnas": 20.0,
            "nc_rnas": 10,
            "num_proteins": 1000,
            "eggnog_coverage": 80.0,
            "ipr_coverage": 75.0,
            "taxon_lineage": "Bacteria;Proteobacteria;Gammaproteobacteria",
            "catalogue": genome_catalogues[0],  # Human Gut Prokaryotes
            "geo_origin": geographic_locations[0],  # Europe
        },
        {
            "accession": "MGYG000000002",
            "biome": top_level_biomes[3],  # Human biome
            "length": 2000000,
            "num_contigs": 200,
            "n_50": 20000,
            "gc_content": 0.6,
            "type": Genome.MAG,
            "completeness": 90.0,
            "contamination": 3.0,
            "trnas": 25.0,
            "nc_rnas": 15,
            "num_proteins": 2000,
            "eggnog_coverage": 75.0,
            "ipr_coverage": 70.0,
            "taxon_lineage": "Bacteria;Firmicutes;Bacilli",
            "catalogue": genome_catalogues[0],  # Human Gut Prokaryotes
            "geo_origin": geographic_locations[1],  # North America
        },
        {
            "accession": "MGYG000000003",
            "biome": top_level_biomes[0],  # Root biome
            "length": 3000000,
            "num_contigs": 300,
            "n_50": 30000,
            "gc_content": 0.7,
            "type": Genome.ISOLATE,
            "completeness": 98.0,
            "contamination": 1.0,
            "trnas": 30.0,
            "nc_rnas": 20,
            "num_proteins": 3000,
            "eggnog_coverage": 85.0,
            "ipr_coverage": 80.0,
            "taxon_lineage": "Bacteria;Cyanobacteria;Cyanobacteriia",
            "catalogue": genome_catalogues[1],  # Ocean Prokaryotes
            "geo_origin": geographic_locations[5],  # Oceania
        },
    ]

    genomes_objects = []
    for genome_data in genomes_list:
        print(f"creating genome {genome_data['accession']}")
        genome, created = Genome.objects.get_or_create(
            accession=genome_data["accession"], defaults=genome_data
        )

        # Add geographic range
        if genome_data["accession"] == "MGYG000000001":
            genome.pangenome_geographic_range.add(geographic_locations[0])  # Europe
            genome.pangenome_geographic_range.add(
                geographic_locations[1]
            )  # North America
        elif genome_data["accession"] == "MGYG000000002":
            genome.pangenome_geographic_range.add(
                geographic_locations[1]
            )  # North America
            genome.pangenome_geographic_range.add(
                geographic_locations[2]
            )  # South America
        elif genome_data["accession"] == "MGYG000000003":
            genome.pangenome_geographic_range.add(geographic_locations[5])  # Oceania
            genome.pangenome_geographic_range.add(geographic_locations[6])  # Antarctica

        genomes_objects.append(genome)

    # Update genome counts in catalogues
    for catalogue in genome_catalogues:
        catalogue.calculate_genome_count()

    return genomes_objects
