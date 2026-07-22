import pytest
from django.core.exceptions import ValidationError

from analyses.models import Biome, SuperStudy, SuperStudyGenomeCatalogue
from genomes.models import (
    CatalogueGenome,
    Genome,
    GenomeCatalogue,
    GenomeCatalogueSeries,
)


def make_series(name, label, biome):
    return GenomeCatalogueSeries.objects.create(
        name=name,
        catalogue_biome_label=label,
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )


def make_release(series, catalogue_id, version, status=GenomeCatalogue.Status.DRAFT):
    return GenomeCatalogue.objects.create(
        catalogue_id=catalogue_id,
        series=series,
        version=version,
        name=f"{series.name} v{version}",
        status=status,
    )


def make_catalogue_genome(catalogue, accession="MGYG000000001", biome=None):
    genome, _ = Genome.objects.get_or_create(accession=accession)
    return CatalogueGenome.objects.create(
        genome=genome,
        catalogue=catalogue,
        biome=biome or catalogue.biome,
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
        geographic_origin="Atlantic Ocean",
    )


@pytest.mark.django_db(transaction=True)
def test_genome_catalogue_release():
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )
    series = make_series("Ocean Prokaryotes", "Ocean", biome)
    catalogue = make_release(series, "ocean-prokaryotes", "1.0")
    catalogue.description = "Prokaryotic genomes from ocean environments"
    catalogue.save()

    assert catalogue.catalogue_id == "ocean-prokaryotes"
    assert catalogue.version == "1.0"
    assert catalogue.name == "Ocean Prokaryotes v1.0"
    assert catalogue.description == "Prokaryotic genomes from ocean environments"
    assert catalogue.catalogue_biome_label == "Ocean"
    assert catalogue.catalogue_type == GenomeCatalogue.PROK
    assert catalogue.biome == biome
    assert catalogue.status == GenomeCatalogue.Status.DRAFT
    assert catalogue.calculate_genome_count == 0


@pytest.mark.django_db(transaction=True)
def test_genome_identity_and_catalogue_snapshot():
    biome = Biome.objects.create(
        biome_name="Ocean", path="root.environmental.aquatic.marine.ocean"
    )
    series = make_series("Ocean Prokaryotes", "Ocean", biome)
    catalogue = make_release(series, "ocean-prokaryotes", "1.0")
    catalogue_genome = make_catalogue_genome(catalogue, biome=biome)
    genome = catalogue_genome.genome

    assert genome.accession == "MGYG000000001"
    assert str(genome) == "MGYG000000001"
    assert catalogue_genome.biome == biome
    assert catalogue_genome.length == 1000000
    assert catalogue_genome.type == Genome.GenomeType.MAG
    assert catalogue_genome.catalogue == catalogue
    assert catalogue_genome.geographic_origin == "Atlantic Ocean"

    catalogue_genome.geographic_origin = "Mediterranean Sea"
    catalogue_genome.geographic_range = ["Indian Ocean", "Arctic Ocean"]
    catalogue_genome.save()
    catalogue_genome.refresh_from_db()
    assert catalogue_genome.geographic_origin == "Mediterranean Sea"
    assert catalogue_genome.geographic_range == ["Indian Ocean", "Arctic Ocean"]
    assert catalogue.calculate_genome_count == 1


@pytest.mark.django_db(transaction=True)
def test_draft_snapshot_does_not_replace_public_snapshot():
    biome = Biome.objects.create(biome_name="root", path="root")
    series = make_series("Sheep", "Sheep", biome)
    published = make_release(series, "sheep-v1", "1", GenomeCatalogue.Status.PUBLISHED)
    draft = make_release(series, "sheep-v2", "2", GenomeCatalogue.Status.DRAFT)
    public_entry = make_catalogue_genome(published, "MGYG000000001", biome)
    make_catalogue_genome(draft, "MGYG000000001", biome)

    assert Genome.objects.count() == 1
    assert CatalogueGenome.objects.count() == 2
    assert list(GenomeCatalogue.public_objects.all()) == [published]
    assert list(CatalogueGenome.public_objects.all()) == [public_entry]


@pytest.mark.django_db(transaction=True)
def test_publish_multiple_releases_moves_genome_between_series_atomically():
    biome = Biome.objects.create(biome_name="root", path="root")
    sheep = make_series("Sheep", "Sheep", biome)
    goat = make_series("Goat", "Goat", biome)

    sheep_v1 = make_release(sheep, "sheep-v1", "1", GenomeCatalogue.Status.PUBLISHED)
    sheep_v2 = make_release(sheep, "sheep-v2", "2", GenomeCatalogue.Status.READY)
    goat_v1 = make_release(goat, "goat-v1", "1", GenomeCatalogue.Status.READY)

    make_catalogue_genome(sheep_v1, "MGYG000000001", biome)
    make_catalogue_genome(goat_v1, "MGYG000000001", biome)
    make_catalogue_genome(sheep_v2, "MGYG000000002", biome)
    super_study = SuperStudy.objects.create(slug="farm", title="Farm")
    SuperStudyGenomeCatalogue.objects.create(
        super_study=super_study, genome_catalogue=sheep_v1
    )

    result = GenomeCatalogue.publish_ready([sheep_v2.pk, goat_v1.pk])

    sheep_v1.refresh_from_db()
    sheep_v2.refresh_from_db()
    goat_v1.refresh_from_db()
    assert sheep_v1.status == GenomeCatalogue.Status.RETIRED
    assert sheep_v2.status == GenomeCatalogue.Status.PUBLISHED
    assert goat_v1.status == GenomeCatalogue.Status.PUBLISHED
    assert result["retired"] == [sheep_v1.pk]
    assert set(result["published"]) == {sheep_v2.pk, goat_v1.pk}
    assert SuperStudyGenomeCatalogue.objects.filter(
        super_study=super_study, genome_catalogue=sheep_v2
    ).exists()
    assert not SuperStudyGenomeCatalogue.objects.filter(
        super_study=super_study, genome_catalogue=sheep_v1
    ).exists()


@pytest.mark.django_db(transaction=True)
def test_publish_rejects_duplicate_genome_across_proposed_public_releases():
    biome = Biome.objects.create(biome_name="root", path="root")
    sheep = make_series("Sheep", "Sheep", biome)
    goat = make_series("Goat", "Goat", biome)
    sheep_v1 = make_release(sheep, "sheep-v1", "1", GenomeCatalogue.Status.PUBLISHED)
    goat_v1 = make_release(goat, "goat-v1", "1", GenomeCatalogue.Status.READY)
    make_catalogue_genome(sheep_v1, "MGYG000000001", biome)
    make_catalogue_genome(goat_v1, "MGYG000000001", biome)

    with pytest.raises(ValidationError, match="multiple public catalogues"):
        GenomeCatalogue.publish_ready([goat_v1.pk])

    sheep_v1.refresh_from_db()
    goat_v1.refresh_from_db()
    assert sheep_v1.status == GenomeCatalogue.Status.PUBLISHED
    assert goat_v1.status == GenomeCatalogue.Status.READY
