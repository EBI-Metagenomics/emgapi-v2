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
    """Return the published sourmash artifact directory for a catalogue.

    The artifacts here are the actual sourmash search files on disk, such as
    ``genome_index.sbt.json`` or ``genome_index.sbt.zip``.

    :param catalogue_slug: Catalogue release identifier, e.g. ``human-gut-v2-0``.
    :returns: Directory expected to contain the published sourmash index files.
    """
    return Path(EMG_CONFIG.genomes.sourmash_public_signatures_dir) / catalogue_slug


def resolve_sourmash_artifact_path(catalogue_slug: str) -> Path:
    """Resolve the preferred searchable sourmash artifact for a catalogue.

    This is used at registration/backfill time to translate the published
    filesystem layout into a single DB-backed search index record. The artifact
    is the on-disk sourmash search file itself; it is distinct from the
    ``GenomeSearchIndex`` model, which records which artifact is active for API
    use.

    :param catalogue_slug: Catalogue release identifier, e.g. ``human-gut-v2-0``.
    :raises FileNotFoundError: If no supported sourmash artifact exists.
    :returns: Path to the preferred searchable artifact.
    """
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
    """Resolve an optional manifest describing the indexed genomes.

    This manifest is companion metadata for the artifact on disk, not the DB
    search-index record itself.

    :param catalogue_slug: Catalogue release identifier, e.g. ``human-gut-v2-0``.
    :returns: Manifest path if present, otherwise an empty string.
    """
    index_dir = sourmash_index_directory(catalogue_slug)
    for candidate in ("all_fasta.txt", "manifest.csv"):
        manifest_path = index_dir / candidate
        if manifest_path.exists():
            return str(manifest_path)
    return ""


def file_sha256(path: Path) -> str:
    """Compute a SHA-256 checksum for a file.

    :param path: File to hash.
    :returns: Hex-encoded SHA-256 digest.
    """
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
    """Create or refresh the active sourmash search index for a catalogue.

    The published filesystem is treated as the source of truth here. This helper
    resolves the current artifact and persists it as the active
    ``GenomeSearchIndex`` row, retiring any previously active sourmash index for
    the same catalogue/ksize/moltype slot.

    The sourmash artifact is the searchable file on disk. The
    ``GenomeSearchIndex`` is the Django model row that points at that artifact
    and stores operational metadata such as activation state, checksum, ksize,
    moltype, genome count, and timestamps.

    :param catalogue: Catalogue release whose searchable sourmash artifact is being published.
    :param artifact_path: Optional explicit path to the searchable sourmash artifact.
    :param manifest_path: Optional explicit manifest path describing indexed genomes.
    :param ksize: Optional k-mer size for this index. Defaults to sourmash config.
    :param moltype: Molecule type stored for the index, usually ``DNA``.
    :param scaled: Optional scaled factor for the index. Defaults to sourmash config.
    :param genome_count: Optional count of indexed genomes. Defaults to catalogue membership count.
    :param checksum: Optional precomputed checksum for the artifact.
    :raises FileNotFoundError: If the resolved artifact path does not exist.
    :returns: ``(index, created, retired_count)`` for the active index row.
    """
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
