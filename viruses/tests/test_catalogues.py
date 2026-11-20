"""Verify stable identities, representative snapshots and occurrence provenance."""

import csv
import gzip
import hashlib
import json
import shutil
from pathlib import Path

import pytest
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from ninja_extra.testing import TestClient

from emgapiv2.api import api
from viruses.dev_data import seed_dev_viral_catalogue
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment, ViralFragment
from viruses.occurrences import ViralFragmentOccurrence
from workflows.flows.import_viral_catalogue_flow import (
    import_catalogue as import_catalogue_task,
)
from workflows.flows.import_viral_catalogue_flow import (
    import_viral_catalogue_flow,
)

# Exercise the task body without starting Prefect for each API test.
import_catalogue = import_catalogue_task.fn

FIXTURE = (
    Path(settings.BASE_DIR)
    / "slurm-dev-environment/fs/nfs/production/catalogs/marine-viruses/v1.0"
)
OPTIONS = {
    "catalogue_id": "marine-viruses-v1-0",
    "name": "Marine Viruses",
    "version": "1.0",
    "biome": "Marine",
    "ftp_url": "https://ftp.ebi.ac.uk/pub/databases/metagenomics/BlueRemediomics/marine_viral_catalogue_v1/",
}
REP = "MGYV0000104487"
MEMBER = "MGYV0000022715"


def edit_metadata(directory: Path, edit) -> None:
    """Mutate an uncompressed copy of the source metadata table."""
    path = directory / "viruses/viruses-all-metadata.tsv.gz"
    with gzip.open(path, "rt") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fields, rows = reader.fieldnames, list(reader)
    edit(rows)
    # Tests can add the optional source-link columns.
    fields += sorted({key for row in rows for key in row} - set(fields))
    path.unlink()
    with path.with_suffix("").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def copy_fixture(tmp_path: Path) -> Path:
    """Create an editable copy without changing the real fixture."""
    directory = tmp_path / "catalogue"
    shutil.copytree(FIXTURE, directory)
    return directory


@pytest.fixture
def catalogue(db):
    """Import one published representative catalogue."""
    return import_catalogue(str(FIXTURE), **OPTIONS, is_public=True)


def test_fixture_integrity():
    """Keep raw detections, FASTA checksums and all source exports intact."""
    rows = list(
        csv.DictReader(
            gzip.open(FIXTURE / "viruses/viruses-all-metadata.tsv.gz", "rt"),
            delimiter="\t",
        )
    )
    sequences = dict(
        record.split("\n", 1)
        for record in gzip.open(FIXTURE / "viruses/viruses.fasta.gz", "rt")
        .read()
        .split(">")[1:]
    )
    for row in rows:
        sequence = "".join(sequences[row["ID"]].split())
        assert len(sequence) == int(row["Sequence_length"])
        assert hashlib.sha256(sequence.encode()).hexdigest() == row["Sequence_sha256"]
    assert len(rows) == 10
    expected = json.loads((FIXTURE / "source_files.json").read_text())
    manifest = json.loads((FIXTURE / "fixture_manifest.json").read_text())["files"]
    assert len(expected) == 107
    assert set(expected) == set(manifest)
    for name in expected:
        content = (FIXTURE / name).read_bytes()
        assert hashlib.sha256(content).hexdigest() == manifest[name]["sha256"]
        if name.endswith(".gz"):
            gzip.decompress(content)
        if name.endswith(".tbi"):
            assert gzip.decompress(content).startswith(b"TBI\x01")


@pytest.mark.django_db
def test_representatives_only_with_member_occurrences(catalogue):
    """Store representative annotations and retain member detections separately."""
    assert catalogue.fragments.count() == 9
    assert ViralFragment.objects.count() == 9
    assert not ViralFragment.objects.filter(accession=MEMBER).exists()
    entry = catalogue.fragment_entries.select_related("fragment").get(
        fragment__accession=REP
    )
    assert entry.length == 12842
    assert entry.cluster_size == 2
    assert [item.fragment_accession for item in entry.occurrences] == [REP]
    assert [item.fragment_accession for item in entry.cluster_member_occurrences] == [
        MEMBER
    ]
    assert entry.metadata["ID"] == REP
    assert entry.metadata["Original_ID"] == entry.occurrences[0].original_id
    import_catalogue(str(FIXTURE), **OPTIONS, is_public=True)
    assert catalogue.fragment_entries.count() == 9
    assert ViralFragment.objects.count() == 9


@pytest.mark.django_db
def test_shared_identity_and_release_specific_annotations(catalogue, tmp_path):
    """Share identity while keeping different release annotations and memberships."""
    directory = copy_fixture(tmp_path)

    def change(rows):
        for row in rows:
            if row["ID"] == REP:
                row["completeness"] = "75.0"

    edit_metadata(directory, change)
    other = import_catalogue(
        str(directory),
        **{**OPTIONS, "catalogue_id": "marine-v2", "version": "2"},
        is_public=True,
    )
    first = catalogue.fragment_entries.get(fragment__accession=REP)
    second = other.fragment_entries.get(fragment__accession=REP)
    assert first.fragment_id == second.fragment_id
    assert first.completeness != second.completeness
    assert second.completeness == 75.0
    assert ViralFragment.objects.count() == 9
    assert first.fragment.catalogues.count() == 2
    other.fragment_entries.all().delete()
    assert catalogue.fragments.count() == 9
    assert ViralFragment.objects.filter(pk=first.fragment_id).exists()
    # Identical hashes with different accessions remain valid pipeline identities.
    same_hash = catalogue.fragment_entries.get(
        fragment__accession="MGYV9999999999"
    ).fragment.nt_sequence_sha256
    assert ViralFragment.objects.create(
        accession="MGYV8888888888", nt_sequence_sha256=same_hash
    )


@pytest.mark.django_db
def test_multiple_occurrences_and_duplicate_detection_rows(catalogue, tmp_path):
    """Concatenate both roles without counting duplicate rows as new sequences."""
    directory = copy_fixture(tmp_path)

    def change(rows):
        rep = dict(next(row for row in rows if row["ID"] == REP))
        rep["Source_accession"] = "SECOND_REP_SOURCE"
        rep["Source_of_prediction"] = "Other predictor"
        member = dict(next(row for row in rows if row["ID"] == MEMBER))
        member["Source_accession"] = "SECOND_MEMBER_SOURCE"
        rows.extend([rep, member, dict(rep)])

    edit_metadata(directory, change)
    import_catalogue(str(directory), **OPTIONS, is_public=True)
    entry = catalogue.fragment_entries.get(fragment__accession=REP)
    assert entry.cluster_size == 2
    assert len(entry.occurrences) == 2
    assert len(entry.cluster_member_occurrences) == 2
    assert entry.length == 12842
    assert "Other predictor" in entry.prediction_tool


@pytest.mark.django_db
def test_genome_release_reference(catalogue, genomes, tmp_path):
    """Resolve occurrences to a catalogue genome snapshot instead of its identity."""
    from genomes.models import CatalogueGenome

    genome = CatalogueGenome.public_objects.first()
    directory = copy_fixture(tmp_path)

    def change(rows):
        for row in rows:
            if row["ID"] == MEMBER:
                row["Source_genome"] = genome.genome.accession
                row["Source_catalogue_genome"] = str(genome.pk)

    edit_metadata(directory, change)
    import_catalogue(str(directory), **OPTIONS, is_public=True)
    member = catalogue.fragment_entries.get(
        fragment__accession=REP
    ).cluster_member_occurrences[0]
    assert member.catalogue_genome_id == genome.pk
    assert member.genome_catalogue_id == genome.catalogue_id


@pytest.mark.django_db
def test_public_api(catalogue):
    """Expose representative counts and both occurrence arrays with pagination."""
    client = TestClient(api)
    response = client.get("/viruses/catalogues/")
    assert response.status_code == 200
    data = response.json()["items"][0]
    assert data["fragment_count"] == data["cluster_count"] == 9
    assert data["total_sequence_count"] == 10
    assert data["virus_count"] == 8
    assert data["prophage_count"] == 1
    assert data["plasmid_count"] == 3
    base = f"/viruses/catalogues/{catalogue.pk}"
    page = client.get(f"{base}/fragments?page_size=2&page=2").json()
    assert page["count"] == 9 and len(page["items"]) == 2
    detail = client.get(f"{base}/fragments/{REP}").json()
    assert detail["accession"] == REP
    assert detail["occurrences"][0]["fragment_accession"] == REP
    assert detail["cluster_member_occurrences"][0]["fragment_accession"] == MEMBER
    assert detail["cluster_size"] == 2
    assert detail["sequence_sha256"] == detail["nt_sequence_sha256"]
    assert client.get(f"{base}/fragments/{MEMBER}").status_code == 404
    assert client.get(f"{base}/fragments?search={MEMBER}").json()["count"] == 1
    assert (
        client.get(f"{base}/fragments?cluster_representative={REP}").json()["count"]
        == 1
    )
    assert client.get(f"{base}/fragments?source_type=analysis").json()["count"] == 1
    assert client.get(f"{base}/fragments?sequence_type=prophage").json()["count"] == 1
    assert not client.get(f"{base}/fragments/MGYV0000123871").json()["is_provirus"]
    assert client.get(f"{base}/fragments?order=metadata").status_code == 422


@pytest.mark.django_db
@pytest.mark.parametrize(
    "field,value",
    [
        ("Source", "unknown"),
        ("Sequence_length", "0"),
        ("completeness", "nan"),
        ("contamination", "101"),
        ("Cluster_rep", "MGYV0000000000"),
        ("ID", "bad"),
        ("Sequence_sha256", "bad"),
        ("provirus", "maybe"),
    ],
)
def test_invalid_import_is_atomic(catalogue, tmp_path, field, value):
    """Reject invalid member rows before changing any release data."""
    directory = copy_fixture(tmp_path)
    edit_metadata(directory, lambda rows: rows[0].update({field: value}))
    with pytest.raises(ValueError):
        import_catalogue(str(directory), **{**OPTIONS, "name": "Changed"})
    catalogue.refresh_from_db()
    assert catalogue.name == OPTIONS["name"]
    assert catalogue.fragments.count() == 9


@pytest.mark.django_db
def test_checksum_conflict_does_not_change_existing_release(catalogue, tmp_path):
    """Reject an accession with a different sequence without overwriting identity."""
    directory = copy_fixture(tmp_path)
    old = ViralFragment.objects.get(accession=REP).nt_sequence_sha256

    def change(rows):
        next(row for row in rows if row["ID"] == REP)["Sequence_sha256"] = "f" * 64

    edit_metadata(directory, change)
    with pytest.raises(ValidationError):
        import_catalogue(str(directory), **{**OPTIONS, "catalogue_id": "bad-release"})
    assert not ViralCatalogue.objects.filter(pk="bad-release").exists()
    assert ViralFragment.objects.get(accession=REP).nt_sequence_sha256 == old


@pytest.mark.django_db
def test_occurrence_validation(catalogue):
    """Reject wrong occurrence roles and mutually exclusive source references."""
    entry = catalogue.fragment_entries.select_related("fragment").get(
        fragment__accession=REP
    )
    wrong = entry.occurrences[0].model_dump()
    with pytest.raises(ValueError):
        ViralFragmentOccurrence.model_validate({**wrong, "analysis_id": 1})
    entry.cluster_member_occurrences = [wrong]
    with pytest.raises(ValidationError):
        entry.full_clean()
    with pytest.raises(IntegrityError), transaction.atomic():
        ViralCatalogueViralFragment.objects.create(
            catalogue=entry.catalogue, fragment=entry.fragment, length=1
        )


@pytest.mark.django_db
@pytest.mark.parametrize("member_source", [False, True])
def test_analysis_occurrences_and_privacy(
    catalogue, assembly_analysis, tmp_path, member_source
):
    """Check privacy for representative detections and member detections."""
    directory = copy_fixture(tmp_path)
    assembly_analysis.mark_status(
        assembly_analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    )
    target = MEMBER if member_source else "MGYV9999999999"

    def change(rows):
        row = next(row for row in rows if row["ID"] == target)
        row.update(Source="analysis", Source_accession=str(assembly_analysis.accession))

    edit_metadata(directory, change)
    import_catalogue(str(directory), **OPTIONS, is_public=True)
    accession = REP if member_source else target
    entry = catalogue.fragment_entries.get(fragment__accession=accession)
    values = entry.cluster_member_occurrences if member_source else entry.occurrences
    assert values[0].analysis_id == assembly_analysis.pk
    base = f"/viruses/catalogues/{catalogue.pk}"
    assert TestClient(api).get(f"{base}/fragments/{accession}").status_code == 200
    assembly_analysis.is_private = True
    assembly_analysis.save()
    assert TestClient(api).get(f"{base}/fragments/{accession}").status_code == 404
    assert TestClient(api).get(base).json()["fragment_count"] == 8
    with pytest.raises(ValidationError):
        import_catalogue(str(directory), **OPTIONS, is_public=True)


@pytest.mark.django_db
def test_catalogue_visibility_and_downloads(catalogue):
    """Hide unpublished releases and retain the standard file manifest."""
    client = TestClient(api)
    base = f"/viruses/catalogues/{catalogue.pk}"
    data = client.get(base).json()
    downloads = data["downloads"]
    assert len(downloads) == 33
    aliases = {file["alias"] for file in downloads}
    assert "viruses/cluster_representatives/viruses_final.gff.gz" in aliases
    assert (
        "viruses/cluster_representatives/protein_clusters_phammseqs/viruses_phams.tsv"
        in aliases
    )
    assert not any(
        "itol_" in alias or alias.endswith((".html", ".tbl.gz", ".tbi"))
        for alias in aliases
    )
    assert (
        "viruses/cluster_representatives/functional_annotation/hmmer/GVDB_summary.tsv.gz"
        not in aliases
    )
    assert all(
        file["short_description"] != Path(file["alias"]).name for file in downloads
    )
    assert all(file["long_description"] for file in downloads)
    assert all("path" not in file for file in data["downloads"])
    catalogue.is_public = False
    catalogue.save()
    assert client.get(base).status_code == 404
    assert client.get(f"{base}/fragments/{REP}").status_code == 404


@pytest.mark.django_db
def test_dev_seed_links_analysis(assembly_analysis):
    """Keep the synthetic ASA provenance in its representative occurrence."""
    assembly_analysis.mark_status(
        assembly_analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    )
    catalogue = seed_dev_viral_catalogue()
    entry = catalogue.fragment_entries.get(fragment__accession="MGYV9999999999")
    assert entry.occurrences[0].analysis_id == assembly_analysis.pk


@pytest.mark.django_db(transaction=True)
def test_prefect_flow(prefect_harness):
    """Import nine representative identities from ten raw sequence rows."""
    assert (
        import_viral_catalogue_flow(results_directory=str(FIXTURE), **OPTIONS)
        == OPTIONS["catalogue_id"]
    )
    assert ViralFragment.objects.count() == 9
    assert ViralCatalogueViralFragment.objects.count() == 9
    assert not ViralCatalogue.objects.get().is_public


@pytest.mark.django_db
def test_occurrence_snapshots_export_as_json(catalogue):
    """Serialize loaded Pydantic lists through Django's fixture exporter."""
    from django.core import serializers

    entry = catalogue.fragment_entries.get(fragment__accession=REP)
    payload = json.loads(serializers.serialize("json", [entry]))[0]["fields"]
    assert payload["occurrences"][0]["fragment_accession"] == REP
    assert payload["cluster_member_occurrences"][0]["fragment_accession"] == MEMBER
    restored = next(
        serializers.deserialize(
            "json",
            json.dumps(
                [
                    {
                        "model": "viruses.viralcatalogueviralfragment",
                        "pk": entry.pk,
                        "fields": payload,
                    }
                ]
            ),
        )
    ).object
    restored.full_clean(validate_unique=False, validate_constraints=False)


@pytest.mark.django_db
def test_virus_admin_change_forms(admin_client, catalogue):
    """Render imported catalogue JSON and occurrence snapshots in Unfold forms."""
    from django.urls import reverse

    entry = catalogue.fragment_entries.select_related("fragment").first()
    for obj in [catalogue, entry.fragment, entry]:
        url = reverse(f"admin:viruses_{obj._meta.model_name}_change", args=[obj.pk])
        response = admin_client.get(url)
        assert response.status_code == 200
        assert "unfold/" in response.content.decode()
