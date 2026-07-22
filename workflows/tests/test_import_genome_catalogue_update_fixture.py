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


def import_fixture_release(version, catalogue_slug):
    return run_flow_and_capture_logs(
        import_genomes_flow,
        results_directory=str(CATALOGUE_FIXTURE_ROOT / f"v{version}"),
        catalogue_name="Ocean Prokaryotes",
        catalogue_version=version,
        gold_biome="root",
        pipeline_version="v3.0.0",
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

    GenomeCatalogue.publish_ready([v2.pk])
    v1.refresh_from_db()
    v2.refresh_from_db()
    assert v1.status == GenomeCatalogue.Status.RETIRED
    assert v2.status == GenomeCatalogue.Status.PUBLISHED
    assert set(
        CatalogueGenome.public_objects.values_list("genome__accession", flat=True)
    ) == {"MGYG000000003", "MGYG000000005"}
