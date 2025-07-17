import pytest

from analyses.models import Biome
from genomes.models.genome import Genome
from genomes.models.genome_catalogue import GenomeCatalogue
from genomes.models.geographic_location import GeographicLocation


@pytest.mark.django_db(transaction=True)
def test_geographic_location():
    """Test the GeographicLocation model."""
    # Test creation
    location = GeographicLocation.objects.create(name="Europe")
    assert location.name == "Europe"
    
    # Test string representation
    assert str(location) == "Europe"


@pytest.mark.django_db(transaction=True)
def test_genome_catalogue():
    """Test the GenomeCatalogue model."""
    # Create a biome for the catalogue
    biome = Biome.objects.create(biome_name="Ocean", path="root.environmental.aquatic.marine.ocean")
    
    # Test creation
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        description="Prokaryotic genomes from ocean environments",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome
    )
    
    assert catalogue.catalogue_id == "ocean-prokaryotes"
    assert catalogue.version == "1.0"
    assert catalogue.name == "Ocean Prokaryotes"
    assert catalogue.description == "Prokaryotic genomes from ocean environments"
    assert catalogue.catalogue_biome_label == "Ocean"
    assert catalogue.catalogue_type == GenomeCatalogue.PROK
    assert catalogue.biome == biome
    
    # Test string representation
    assert str(catalogue) == "Ocean Prokaryotes"
    
    # Test calculate_genome_count method
    assert catalogue.genome_count is None
    catalogue.calculate_genome_count()
    assert catalogue.genome_count == 0


@pytest.mark.django_db(transaction=True)
def test_genome():
    """Test the Genome model."""
    biome = Biome.objects.create(biome_name="Ocean", path="root.environmental.aquatic.marine.ocean")
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome
    )
    geo_origin = GeographicLocation.objects.create(name="Atlantic Ocean")
    
    # Test creation
    genome = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type=Genome.MAG,
        completeness=95.0,
        contamination=2.0,
        trnas=20.0,
        nc_rnas=10,
        num_proteins=1000,
        eggnog_coverage=80.0,
        ipr_coverage=75.0,
        taxon_lineage="Bacteria;Proteobacteria;Gammaproteobacteria",
        catalogue=catalogue,
        geo_origin=geo_origin
    )
    
    assert genome.accession == "MGYG000000001"
    assert genome.biome == biome
    assert genome.length == 1000000
    assert genome.num_contigs == 100
    assert genome.n_50 == 10000
    assert genome.gc_content == 0.5
    assert genome.type == Genome.MAG
    assert genome.completeness == 95.0
    assert genome.contamination == 2.0
    assert genome.catalogue == catalogue
    assert genome.geo_origin == geo_origin
    
    # Test string representation
    assert str(genome) == "MGYG000000001"
    
    # Test geographic_origin property
    assert genome.geographic_origin == "Atlantic Ocean"
    
    # Test geographic_range property
    assert genome.geographic_range == []
    
    # Add a geographic range
    pacific = GeographicLocation.objects.create(name="Pacific Ocean")
    genome.pangenome_geographic_range.add(pacific)
    assert genome.geographic_range == ["Pacific Ocean"]
    
    # Test last_update_iso and first_created_iso properties
    assert genome.last_update_iso is not None
    assert genome.first_created_iso is not None
    
    # Test that the genome is counted in the catalogue
    catalogue.calculate_genome_count()
    assert catalogue.genome_count == 1