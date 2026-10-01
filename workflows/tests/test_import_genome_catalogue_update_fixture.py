from pathlib import Path

import pytest
from django.conf import settings

from analyses.models import Biome
from genomes.models import CatalogueGenome, Genome, GenomeCatalogue
from workflows.flows.import_genomes_flow import import_genomes_flow
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs

CATALOGUE_FIXTURE_ROOT = (
    Path(settings.EMG_CONFIG.genomes.results_directory_root) / "ocean-prokaryotes"
)
OCEAN_EUK_CATALOGUE_FIXTURE_ROOT = (
    Path(settings.EMG_CONFIG.genomes.results_directory_root) / "ocean-eukaryotes"
)


def import_fixture_release(version, catalogue_slug, pipeline_version="v3.0.0"):
    return run_flow_and_capture_logs(
        import_genomes_flow,
        results_directory=str(CATALOGUE_FIXTURE_ROOT / f"v{version}"),
        catalogue_name="Ocean Prokaryotes",
        catalogue_version=version,
        gold_biome="root",
        pipeline_version=pipeline_version,
        catalogue_type="prokaryotes",
        catalogue_biome_label="Ocean",
        destination_dir_name="ocean-prokaryotes",
        catalogue_slug=catalogue_slug,
        run_release_tasks=False,
        release_third_party_data=False,
    )


@pytest.mark.django_db(transaction=True)
def test_realistic_catalogue_update_fixture(prefect_harness):
    Biome.objects.create(biome_name="root", path="root")

    import_fixture_release("1.0", "ocean-prokaryotes")
    v1 = GenomeCatalogue.objects.get(pk="ocean-prokaryotes")
    assert v1.status == GenomeCatalogue.Status.READY
    assert set(v1.genomes.values_list("genome__accession", flat=True)) == {
        "MGYG000000003",
        "MGYG000000004",
    }
    GenomeCatalogue.publish_ready([v1.pk])

    import_fixture_release("2.0", "ocean-prokaryotes-v2-0")
    v2 = GenomeCatalogue.objects.get(pk="ocean-prokaryotes-v2-0")
    assert v2.status == GenomeCatalogue.Status.READY
    assert set(v2.genomes.values_list("genome__accession", flat=True)) == {
        "MGYG000000003",
        "MGYG000000005",
    }

    # The staged update neither duplicates stable identities nor changes public data.
    assert Genome.objects.count() == 3
    assert set(
        CatalogueGenome.public_objects.values_list("genome__accession", flat=True)
    ) == {"MGYG000000003", "MGYG000000004"}
    shared_snapshots = CatalogueGenome.objects.filter(
        genome__accession="MGYG000000003"
    ).order_by("catalogue__version")
    assert list(shared_snapshots.values_list("length", flat=True)) == [3000000, 3100000]

    new_genome = v2.genomes.get(genome__accession="MGYG000000005")
    assert len(new_genome.downloads) >= 11
    assert "genome/MGYG000000005.fna" in {
        download["path"] for download in new_genome.downloads
    }
    legacy_downloads = {download["path"]: download for download in new_genome.downloads}
    assert legacy_downloads["genome/MGYG000000005.fna"]["long_description"] == (
        "Genome assembly in FASTA format"
    )
    assert legacy_downloads["genome/MGYG000000005_eggNOG.tsv"]["short_description"] == (
        "EggNOG annotation"
    )
    assert legacy_downloads["genome/MGYG000000005_eggNOG.tsv"]["long_description"] == (
        "Result of orthology annotation at the protein level in TSV format"
    )

    GenomeCatalogue.publish_ready([v2.pk])
    v1.refresh_from_db()
    v2.refresh_from_db()
    assert v1.status == GenomeCatalogue.Status.RETIRED
    assert v2.status == GenomeCatalogue.Status.PUBLISHED
    assert set(
        CatalogueGenome.public_objects.values_list("genome__accession", flat=True)
    ) == {"MGYG000000003", "MGYG000000005"}


@pytest.mark.django_db(transaction=True)
def test_ocean_eukaryotes_fixture(prefect_harness):
    Biome.objects.create(biome_name="root", path="root")

    run_flow_and_capture_logs(
        import_genomes_flow,
        results_directory=str(OCEAN_EUK_CATALOGUE_FIXTURE_ROOT / "v1.0"),
        catalogue_name="Ocean Eukaryotes",
        catalogue_version="1.0",
        gold_biome="root",
        pipeline_version="v3.0.0",
        catalogue_type="eukaryotes",
        catalogue_biome_label="Ocean",
        destination_dir_name="ocean-eukaryotes",
        catalogue_slug="ocean-eukaryotes",
        run_release_tasks=False,
        release_third_party_data=False,
    )

    catalogue = GenomeCatalogue.objects.get(pk="ocean-eukaryotes")
    assert catalogue.status == GenomeCatalogue.Status.READY
    assert catalogue.catalogue_type == GenomeCatalogue.EUKS
    assert catalogue.other_stats["Total proteins"] == 43

    entry = catalogue.genomes.get(genome__accession="MGYG000000010")
    assert entry.num_contigs == 10
    assert entry.taxon_lineage.endswith("s__Micromonas commoda")
    assert "genome/MGYG000000010.fna" in {
        download["path"] for download in entry.downloads
    }


@pytest.mark.django_db(transaction=True)
def test_ocean_prokaryotes_v4_fixture(prefect_harness):
    Biome.objects.create(biome_name="root", path="root")
    import_fixture_release("3.0", "ocean-prokaryotes-v3-0", pipeline_version="v4.0")

    catalogue = GenomeCatalogue.objects.get(pk="ocean-prokaryotes-v3-0")
    assert catalogue.status == GenomeCatalogue.Status.READY
    assert catalogue.pipeline_version_tag == "v4.0"
    entry = catalogue.genomes.get(genome__accession="MGYG000235960")
    assert entry.annotations["cog_categories"] == [{"name": "G", "count": 171}]
    assert entry.annotations["kegg_classes"] == [{"class_id": "09101", "count": 320}]
    assert entry.annotations["kegg_modules"] == [{"name": "M00178", "count": 56}]
    assert entry.num_genomes_total == 2

    downloads = {download["path"]: download for download in entry.downloads}
    assert set(downloads) >= {
        "genome/MGYG000235960.faa.gz",
        "genome/MGYG000235960.fna.gz",
        "genome/MGYG000235960.gff.gz",
        "genome/MGYG000235960_pathofact2_combined_report.tsv.gz",
        "pan-genome/gene_presence_absence.Rtab.gz",
        "pan-genome/gene_presence_absence.csv.gz",
        "pan-genome/gene_prevalence_corrected.txt.gz",
        "pan-genome/mashtree.nwk.gz",
        "pan-genome/pan-genome.fna.gz",
    }
    assert not any(path.endswith((".fai", ".gzi", ".csi")) for path in downloads)
    assert downloads["genome/MGYG000235960.fna.gz"]["index_file"] == [
        {"index_type": "fai", "path": "genome/MGYG000235960.fna.gz.fai"},
        {"index_type": "gzi", "path": "genome/MGYG000235960.fna.gz.gzi"},
    ]
    assert downloads["genome/MGYG000235960.gff.gz"]["index_file"] == {
        "index_type": "csi",
        "path": "genome/MGYG000235960.gff.gz.csi",
    }
    assert downloads["genome/MGYG000235960.fna.gz"]["file_type"] == "fasta"
    expected_descriptions = {
        "genome/MGYG000235960.gff.gz": (
            "Genome Annotation",
            "Integrated genome annotation, including mobilome annotation, in GFF format",
        ),
        "genome/MGYG000235960_eggNOG.tsv.gz": (
            "EggNOG annotation",
            "Result of orthology annotation at the protein level in TSV format",
        ),
        "genome/MGYG000235960_pathofact2_combined_report.tsv.gz": (
            "Pathofact2-style report",
            "Pathogenicity-related annotations at protein level in TSV format",
        ),
        "pan-genome/gene_prevalence_corrected.txt.gz": (
            "Corrected gene prevalence",
            "Pan-genome gene frequencies (core/middle/rare) after completeness correction",
        ),
    }
    for path, (label, description) in expected_descriptions.items():
        assert downloads[path]["short_description"] == label
        assert downloads[path]["long_description"] == description
    assert downloads["pan-genome/pan-genome.fna.gz"]["long_description"] == (
        "Pangenome DNA sequence"
    )


def test_v4_fixture_requires_compressed_files(tmp_path):
    from shutil import copytree

    from workflows.flows.import_genomes_flow import gather_genome_dirs

    source = CATALOGUE_FIXTURE_ROOT / "v3.0" / "website"
    copytree(source, tmp_path / "website")
    genome_dir = tmp_path / "website" / "MGYG000235960" / "genome"
    (genome_dir / "MGYG000235960.fna.gz.gzi").unlink()
    with pytest.raises(ValueError, match=r"MGYG000235960\.fna\.gz\.gzi"):
        gather_genome_dirs(tmp_path / "website", "prokaryotes", "v4.0")
    with pytest.raises(ValueError, match=r"MGYG000235960\.fna"):
        gather_genome_dirs(source, "prokaryotes", "v3.0")
