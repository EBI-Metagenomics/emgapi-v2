from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from genomes.models import GenomeCatalogue, GenomeSearchIndex

EMG_CONFIG = settings.EMG_CONFIG


SOURMASH_ARTIFACT_CANDIDATES = (
    "genome_index.sbt.json",
    "genome_index.sbt.zip",
    "genomes_index.sbt.json",
    "genomes_index.sbt.zip",
)


def sourmash_index_directory(catalogue_slug: str) -> Path:
    return Path(EMG_CONFIG.genomes.sourmash_public_signatures_dir) / catalogue_slug


def resolve_sourmash_artifact_path(catalogue_slug: str) -> Path:
    index_dir = sourmash_index_directory(catalogue_slug)
    for candidate in SOURMASH_ARTIFACT_CANDIDATES:
        artifact_path = index_dir / candidate
        if artifact_path.exists():
            return artifact_path

    expected = ", ".join(
        str(index_dir / candidate) for candidate in SOURMASH_ARTIFACT_CANDIDATES
    )
    raise FileNotFoundError(
        f"No sourmash search artifact found for {catalogue_slug}. "
        f"Looked for: {expected}"
    )


def resolve_sourmash_manifest_path(catalogue_slug: str) -> str:
    index_dir = sourmash_index_directory(catalogue_slug)
    for candidate in ("all_fasta.txt", "manifest.csv"):
        manifest_path = index_dir / candidate
        if manifest_path.exists():
            return str(manifest_path)
    return ""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@transaction.atomic
def upsert_sourmash_search_index(
    catalogue: GenomeCatalogue,
    *,
    artifact_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
    ksize: int | None = None,
    moltype: str = "DNA",
    scaled: int | None = None,
    genome_count: int | None = None,
    checksum: str | None = None,
) -> tuple[GenomeSearchIndex, bool, int]:
    artifact = (
        resolve_sourmash_artifact_path(catalogue.catalogue_id)
        if artifact_path is None
        else Path(artifact_path)
    )
    if not artifact.exists():
        raise FileNotFoundError(f"Sourmash search artifact does not exist: {artifact}")

    manifest = (
        resolve_sourmash_manifest_path(catalogue.catalogue_id)
        if manifest_path is None
        else str(manifest_path)
    )
    ksize = ksize or EMG_CONFIG.sourmash.default_ksize
    scaled = scaled if scaled is not None else EMG_CONFIG.sourmash.default_scaled
    genome_count = (
        genome_count if genome_count is not None else catalogue.genomes.count()
    )
    checksum = checksum or file_sha256(artifact)

    slot_qs = GenomeSearchIndex.objects.select_for_update().filter(
        catalogue=catalogue,
        backend=GenomeSearchIndex.Backend.SOURMASH,
        ksize=ksize,
        moltype=moltype,
    )
    index = slot_qs.filter(artifact_path=str(artifact)).order_by("-created_at").first()

    retired_count = (
        slot_qs.exclude(pk=getattr(index, "pk", None))
        .filter(is_active=True)
        .update(
            is_active=False,
            status=GenomeSearchIndex.Status.RETIRED,
        )
    )

    now = timezone.now()
    built_at = datetime.fromtimestamp(
        artifact.stat().st_mtime, tz=timezone.get_current_timezone()
    )

    if index is None:
        index = GenomeSearchIndex(
            catalogue=catalogue,
            backend=GenomeSearchIndex.Backend.SOURMASH,
            ksize=ksize,
            moltype=moltype,
            artifact_path=str(artifact),
        )
        created = True
    else:
        created = False

    index.status = GenomeSearchIndex.Status.ACTIVE
    index.is_active = True
    index.manifest_path = manifest
    index.scaled = scaled
    index.genome_count = genome_count
    index.checksum = checksum
    index.built_at = built_at
    index.activated_at = now
    index.save()
    return index, created, retired_count
