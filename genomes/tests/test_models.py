import pytest

from analyses.models import Biome
from genomes.models.genome import Genome
from genomes.models.genome_catalogue import GenomeCatalogue


@pytest.mark.django_db(transaction=True)
def test_genome_catalogue():
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )

    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        description="Prokaryotic genomes from ocean environments",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )

    assert catalogue.catalogue_id == "ocean-prokaryotes"
    assert catalogue.version == "1.0"
    assert catalogue.name == "Ocean Prokaryotes"
    assert catalogue.description == "Prokaryotic genomes from ocean environments"
    assert catalogue.catalogue_biome_label == "Ocean"
    assert catalogue.catalogue_type == GenomeCatalogue.PROK
    assert catalogue.biome == biome
    assert str(catalogue) == "Ocean Prokaryotes"
    assert catalogue.calculate_genome_count == 0


@pytest.mark.django_db(transaction=True)
def test_genome():
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="ocean-prokaryotes",
        version="1.0",
        name="Ocean Prokaryotes",
        catalogue_biome_label="Ocean",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )
    genome = Genome.objects.create(
        accession="MGYG000000001",
        biome=biome,
        length=1000000,
        num_contigs=100,
        n_50=10000,
        gc_content=0.5,
        type=Genome.GenomeType.MAG,
        completeness=95.0,
        contamination=2.0,
        trnas=20.0,
        nc_rnas=10,
        num_proteins=1000,
        eggnog_coverage=80.0,
        ipr_coverage=75.0,
        taxon_lineage="Bacteria;Proteobacteria;Gammaproteobacteria",
        catalogue=catalogue,
        geographic_origin="Atlantic Ocean",
    )

    assert genome.accession == "MGYG000000001"
    assert genome.biome == biome
    assert genome.length == 1000000
    assert genome.num_contigs == 100
    assert genome.n_50 == 10000
    assert genome.gc_content == 0.5
    assert genome.type == Genome.GenomeType.MAG
    assert genome.completeness == 95.0
    assert genome.contamination == 2.0
    assert genome.catalogue == catalogue
    assert genome.geographic_origin == "Atlantic Ocean"

    assert str(genome) == "MGYG000000001"

    assert genome.geographic_origin == "Atlantic Ocean"

    genome.geographic_origin = "Mediterranean Sea"
    genome.save()
    assert genome.geographic_origin == "Mediterranean Sea"

    genome.geographic_origin = None
    genome.save()
    assert genome.geographic_origin is None

    genome.geographic_range = ["Indian Ocean", "Arctic Ocean"]
    genome.save()
    assert genome.geographic_range == ["Indian Ocean", "Arctic Ocean"]

    assert catalogue.calculate_genome_count == 1
