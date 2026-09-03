from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from analyses.models import Biome
from genomes.models import GenomeCatalogue, GenomeCatalogueSeries, GenomeSearchIndex
from genomes.search_indexes import (
    resolve_sourmash_artifact_path,
    resolve_sourmash_manifest_path,
    sourmash_index_directory,
    upsert_sourmash_search_index,
)


def make_catalogue(catalogue_id: str = "human-gut-v2-0") -> GenomeCatalogue:
    biome = Biome.objects.create(
        biome_name=f"{catalogue_id} biome",
        path=f"root.{catalogue_id}",
    )
    series = GenomeCatalogueSeries.objects.create(
        name=f"{catalogue_id} series",
        catalogue_biome_label=f"{catalogue_id} biome label",
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
def test_sourmash_index_directory_uses_configured_root(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)

    assert sourmash_index_directory("human-gut-v2-0") == tmp_path / "human-gut-v2-0"


@pytest.mark.django_db
def test_resolve_sourmash_artifact_path_raises_with_expected_candidates(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)

    with pytest.raises(FileNotFoundError, match="Looked for:") as exc_info:
        resolve_sourmash_artifact_path("human-gut-v2-0")

    message = str(exc_info.value)
    assert "genome_index.sbt.json" in message
    assert "genome_index.sbt.zip" in message
    assert "genomes_index.sbt.json" in message
    assert "genomes_index.sbt.zip" in message


@pytest.mark.django_db
def test_resolve_sourmash_manifest_path_prefers_all_fasta(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    index_dir = tmp_path / "human-gut-v2-0"
    index_dir.mkdir(parents=True)
    all_fasta = index_dir / "all_fasta.txt"
    manifest = index_dir / "manifest.csv"
    all_fasta.write_text("/tmp/MGYG000000001.fna\n", encoding="utf-8")
    manifest.write_text("accession,path\n", encoding="utf-8")

    assert resolve_sourmash_manifest_path("human-gut-v2-0") == str(all_fasta)


@pytest.mark.django_db
def test_resolve_sourmash_manifest_path_falls_back_to_manifest_csv(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    index_dir = tmp_path / "human-gut-v2-0"
    index_dir.mkdir(parents=True)
    manifest = index_dir / "manifest.csv"
    manifest.write_text("accession,path\n", encoding="utf-8")

    assert resolve_sourmash_manifest_path("human-gut-v2-0") == str(manifest)


@pytest.mark.django_db
def test_resolve_sourmash_manifest_path_returns_empty_when_missing(settings, tmp_path):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)

    assert resolve_sourmash_manifest_path("human-gut-v2-0") == ""


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
def test_upsert_sourmash_search_index_updates_existing_matching_artifact(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    catalogue = make_catalogue("marine-v2-0")

    artifact_dir = tmp_path / catalogue.catalogue_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "genome_index.sbt.json"
    artifact_path.write_text('{"artifact": true}', encoding="utf-8")
    existing_index = GenomeSearchIndex.objects.create(
        catalogue=catalogue,
        backend=GenomeSearchIndex.Backend.SOURMASH,
        status=GenomeSearchIndex.Status.RETIRED,
        is_active=False,
        ksize=settings.EMG_CONFIG.sourmash.default_ksize,
        moltype="DNA",
        artifact_path=str(artifact_path),
        scaled=500,
        genome_count=1,
    )

    index, created, retired_count = upsert_sourmash_search_index(
        catalogue,
        manifest_path="manifest.csv",
        scaled=1000,
        genome_count=42,
        checksum="manual-checksum",
    )

    assert created is False
    assert retired_count == 0
    assert index.pk == existing_index.pk
    assert index.status == GenomeSearchIndex.Status.ACTIVE
    assert index.is_active is True
    assert index.manifest_path == "manifest.csv"
    assert index.scaled == 1000
    assert index.genome_count == 42
    assert index.checksum == "manual-checksum"


@pytest.mark.django_db
def test_upsert_sourmash_search_index_raises_when_explicit_artifact_missing(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    catalogue = make_catalogue("marine-v2-0")

    with pytest.raises(FileNotFoundError, match="does not exist"):
        upsert_sourmash_search_index(
            catalogue,
            artifact_path=tmp_path / "missing.sbt.json",
        )


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


@pytest.mark.django_db
def test_backfill_sourmash_search_indexes_command_includes_ready_when_requested(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    ready_catalogue = make_catalogue("soil-v1-0")
    ready_catalogue.status = GenomeCatalogue.Status.READY
    ready_catalogue.save(update_fields=["status"])

    artifact_dir = tmp_path / ready_catalogue.catalogue_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "genome_index.sbt.json"
    artifact_path.write_text("{}", encoding="utf-8")

    stdout = StringIO()
    call_command(
        "backfill_sourmash_search_indexes",
        "--include-ready",
        stdout=stdout,
    )

    index = GenomeSearchIndex.objects.get(catalogue=ready_catalogue)
    assert index.artifact_path == str(artifact_path)
    assert "soil-v1-0: created" in stdout.getvalue()


@pytest.mark.django_db
def test_backfill_sourmash_search_indexes_command_rejects_unknown_catalogue_ids():
    with pytest.raises(CommandError, match="Unknown or ineligible catalogue IDs"):
        call_command("backfill_sourmash_search_indexes", "does-not-exist")


@pytest.mark.django_db
def test_backfill_sourmash_search_indexes_command_exits_for_missing_artifacts(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    make_catalogue("human-gut-v2-0")

    stdout = StringIO()
    stderr = StringIO()
    with pytest.raises(SystemExit) as exc_info:
        call_command(
            "backfill_sourmash_search_indexes",
            stdout=stdout,
            stderr=stderr,
        )

    assert exc_info.value.code == 1
    assert "Skipping human-gut-v2-0:" in stderr.getvalue()
    assert GenomeSearchIndex.objects.count() == 0


@pytest.mark.django_db
def test_backfill_sourmash_search_indexes_command_exits_after_missing_artifact(
    settings, tmp_path
):
    settings.EMG_CONFIG.genomes.sourmash_public_signatures_dir = str(tmp_path)
    seeded = make_catalogue("human-gut-v2-0")
    make_catalogue("marine-v2-0")

    artifact_dir = tmp_path / seeded.catalogue_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "genome_index.sbt.json").write_text("{}", encoding="utf-8")

    stdout = StringIO()
    stderr = StringIO()
    with pytest.raises(SystemExit) as exc_info:
        call_command(
            "backfill_sourmash_search_indexes",
            stdout=stdout,
            stderr=stderr,
        )

    assert exc_info.value.code == 1
    assert "human-gut-v2-0: created" in stdout.getvalue()
    assert "Skipping marine-v2-0:" in stderr.getvalue()
