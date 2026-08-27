from io import StringIO

import pytest
from django.core.management import call_command

from analyses.models import Biome
from genomes.models import GenomeCatalogue, GenomeCatalogueSeries, GenomeSearchIndex
from genomes.search_indexes import (
    resolve_sourmash_artifact_path,
    upsert_sourmash_search_index,
)


def make_catalogue(catalogue_id: str = "human-gut-v2-0") -> GenomeCatalogue:
    biome = Biome.objects.create(biome_name="Root", path="root")
    series = GenomeCatalogueSeries.objects.create(
        name="Human Gut",
        catalogue_biome_label="Human Gut",
        catalogue_type=GenomeCatalogue.PROK,
        biome=biome,
    )
    return GenomeCatalogue.objects.create(
        catalogue_id=catalogue_id,
        series=series,
        version="2.0",
        name=f"{catalogue_id} v2.0",
        status=GenomeCatalogue.Status.PUBLISHED,
    )


@pytest.mark.django_db
def test_resolve_sourmash_artifact_path_prefers_json(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    index_dir = tmp_path / "human-gut-v2-0"
    index_dir.mkdir(parents=True)
    json_artifact = index_dir / "genome_index.sbt.json"
    zip_artifact = index_dir / "genome_index.sbt.zip"
    json_artifact.write_text("{}", encoding="utf-8")
    zip_artifact.write_text("zip", encoding="utf-8")

    assert resolve_sourmash_artifact_path("human-gut-v2-0") == json_artifact


@pytest.mark.django_db
def test_upsert_sourmash_search_index_creates_and_retires_previous_active(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    catalogue = make_catalogue()

    old_artifact = tmp_path / catalogue.catalogue_id / "genomes_index.sbt.json"
    old_artifact.parent.mkdir(parents=True, exist_ok=True)
    old_artifact.write_text('{"old": true}', encoding="utf-8")
    old_index = GenomeSearchIndex.objects.create(
        catalogue=catalogue,
        backend=GenomeSearchIndex.Backend.SOURMASH,
        status=GenomeSearchIndex.Status.ACTIVE,
        is_active=True,
        ksize=settings.EMG_CONFIG.sourmash.default_ksize,
        moltype="DNA",
        artifact_path=str(old_artifact),
        scaled=settings.EMG_CONFIG.sourmash.default_scaled,
        genome_count=5,
    )

    new_artifact = old_artifact.parent / "genome_index.sbt.json"
    new_artifact.write_text('{"new": true}', encoding="utf-8")
    manifest_path = old_artifact.parent / "all_fasta.txt"
    manifest_path.write_text("/tmp/MGYG000000001.fna\n", encoding="utf-8")

    index, created, retired_count = upsert_sourmash_search_index(catalogue)

    assert created is True
    assert retired_count == 1
    assert index.is_active is True
    assert index.status == GenomeSearchIndex.Status.ACTIVE
    assert index.artifact_path == str(new_artifact)
    assert index.manifest_path == str(manifest_path)
    assert index.scaled == settings.EMG_CONFIG.sourmash.default_scaled
    assert index.checksum
    assert index.built_at is not None
    assert index.activated_at is not None

    old_index.refresh_from_db()
    assert old_index.is_active is False
    assert old_index.status == GenomeSearchIndex.Status.RETIRED


@pytest.mark.django_db
def test_backfill_sourmash_search_indexes_command(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    catalogue = make_catalogue("marine-v2-0")

    artifact_dir = tmp_path / catalogue.catalogue_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "genome_index.sbt.json"
    artifact_path.write_text("{}", encoding="utf-8")

    dry_run_output = StringIO()
    call_command(
        "backfill_sourmash_search_indexes",
        stdout=dry_run_output,
        dry_run=True,
    )
    assert "Would register sourmash index for marine-v2-0" in dry_run_output.getvalue()
    assert GenomeSearchIndex.objects.count() == 0

    stdout = StringIO()
    call_command("backfill_sourmash_search_indexes", stdout=stdout)
    index = GenomeSearchIndex.objects.get()
    assert index.catalogue == catalogue
    assert index.artifact_path == str(artifact_path)
    assert "marine-v2-0: created" in stdout.getvalue()
