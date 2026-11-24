"""Verify raw cluster rows, plasmids, release visibility and numeric filters."""

import csv
import gzip
import hashlib
import json
import shutil
from pathlib import Path

import pytest
from django.conf import settings
from django.core.exceptions import ValidationError
from ninja_extra.testing import TestClient
from ninja_jwt.tokens import SlidingToken

from emgapiv2.api import api
from viruses.dev_data import seed_dev_viral_catalogue
from viruses.models import ViralCatalogue, ViralCatalogueViralFragment, ViralFragment
from workflows.flows.import_viral_catalogue_flow import (
    import_catalogue,
    import_viral_catalogue_flow,
)

FIXTURE = (
    Path(settings.BASE_DIR)
    / "slurm-dev-environment/fs/nfs/production/catalogs/marine-viruses/v1.0"
)
OPTIONS = dict(
    catalogue_id="v1.0",
    name="Marine Viruses",
    version="1.0",
    biome="Marine",
    ftp_url="https://example.org/mgnify-viruses/v1.0/",
)
REP, MEMBER = "MGYV0000104487", "MGYV0000022715"


@pytest.fixture
def catalogue(db):
    return import_catalogue.fn(str(FIXTURE), **OPTIONS, is_public=True)


def metadata(kind="viruses"):
    with gzip.open(FIXTURE / f"{kind}/{kind}-all-metadata.tsv.gz", "rt") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


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
def test_rows_are_exact_pipeline_subset(catalogue):
    entry = catalogue.fragment_entries.get(fragment__accession=REP)
    expected = [row for row in metadata() if row["Cluster_rep"] == REP]
    assert entry.metadata_rows == expected
    assert entry.cluster_size == 2
    assert ViralFragment.objects.count() == 12
    assert not ViralFragment.objects.filter(accession=MEMBER).exists()
    data = TestClient(api).get(f"/viruses/catalogues/v1.0/fragments/{REP}").json()
    assert data["metadata_rows"] == expected
    assert data["metadata_url"].endswith("/viruses/viruses-all-metadata.tsv.gz")
    assert "occurrences" not in data
    assert "cluster_member_occurrences" not in data


@pytest.mark.django_db
def test_plasmid_representatives(catalogue):
    client = TestClient(api)
    data = client.get("/viruses/catalogues/v1.0/fragments?kind=plasmids").json()
    assert data["count"] == 3
    first = data["items"][0]
    assert first["sequence_type"] == "plasmid"
    assert first["completeness"] is None
    detail = client.get(
        f"/viruses/catalogues/v1.0/fragments/{first['accession']}"
    ).json()
    assert detail["metadata_rows"] == [
        row for row in metadata("plasmids") if row["Cluster_rep"] == first["accession"]
    ]
    assert detail["metadata_url"].endswith("/plasmids/plasmids-all-metadata.tsv.gz")


@pytest.mark.django_db
def test_latest_and_admin_previews(catalogue):
    client = TestClient(api)
    assert client.get("/viruses/catalogues/latest").json()["catalogue_id"] == "v1.0"
    assert "/viruses/catalogues/" not in api.get_openapi_schema()["paths"]
    preview = import_catalogue.fn(
        str(FIXTURE), **{**OPTIONS, "catalogue_id": "v1.1", "version": "1.1"}
    )
    assert client.get("/viruses/catalogues/v1.1").status_code == 404
    assert client.get(f"/viruses/catalogues/v1.1/fragments/{REP}").status_code == 404
    token = SlidingToken()
    token["username"] = "Webin-admin"
    token["is_admin"] = True
    headers = {"Authorization": f"Bearer {token}"}
    assert client.get("/viruses/catalogues/v1.1", headers=headers).status_code == 200
    assert (
        client.get(
            f"/viruses/catalogues/v1.1/fragments/{REP}", headers=headers
        ).status_code
        == 200
    )
    assert (
        client.get("/viruses/catalogues/latest", headers=headers).json()["catalogue_id"]
        == "v1.0"
    )
    import_catalogue.fn(
        str(FIXTURE),
        **{**OPTIONS, "catalogue_id": preview.pk, "version": "1.1"},
        is_public=True,
    )
    assert ViralCatalogue.objects.filter(is_public=True).count() == 1
    assert client.get("/viruses/catalogues/latest").json()["catalogue_id"] == "v1.1"
    assert client.get("/viruses/catalogues/v1.0").status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize(
    "query",
    [
        "completeness_min=20&completeness_max=90",
        "contamination_max=5",
        "length_min=5000&length_max=20000",
    ],
)
def test_numeric_filters(catalogue, query):
    data = TestClient(api).get(f"/viruses/catalogues/v1.0/fragments?{query}").json()
    from urllib.parse import parse_qsl

    for item in data["items"]:
        for key, value in parse_qsl(query):
            field, comparison = key.rsplit("_", 1)
            assert item[field] is not None
            assert (
                item[field] >= float(value)
                if comparison == "min"
                else item[field] <= float(value)
            )


@pytest.mark.django_db
def test_source_and_member_search(catalogue):
    client = TestClient(api)
    assert (
        client.get(f"/viruses/catalogues/v1.0/fragments?search={MEMBER}").json()[
            "items"
        ][0]["accession"]
        == REP
    )
    assert (
        client.get("/viruses/catalogues/v1.0/fragments?source_type=analysis").json()[
            "count"
        ]
        == 1
    )
    assert (
        client.get(
            "/viruses/catalogues/v1.0/fragments?completeness_min=101"
        ).status_code
        == 422
    )


@pytest.mark.django_db
def test_release_annotations_are_independent(catalogue):
    second = import_catalogue.fn(
        str(FIXTURE), **{**OPTIONS, "catalogue_id": "v1.1", "version": "1.1"}
    )
    assert ViralFragment.objects.count() == 12
    entry = second.fragment_entries.get(fragment__accession=REP)
    entry.completeness = 99
    entry.save()
    assert catalogue.fragment_entries.get(fragment__accession=REP).completeness != 99


@pytest.mark.django_db
def test_private_sources_are_hidden(catalogue, assembly_analysis):
    assembly_analysis.mark_status(
        assembly_analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    )
    entry = catalogue.fragment_entries.get(fragment__accession=REP)
    entry.source_analyses.add(assembly_analysis)
    client = TestClient(api)
    url = f"/viruses/catalogues/v1.0/fragments/{REP}"
    assert client.get(url).status_code == 200
    assembly_analysis.is_private = True
    assembly_analysis.save()
    assert client.get(url).status_code == 404


@pytest.mark.django_db
def test_downloads_include_krona(catalogue):
    data = TestClient(api).get("/viruses/catalogues/v1.0").json()
    assert data["fragment_count"] == 9
    assert data["plasmid_count"] == 3
    krona = [item for item in data["downloads"] if "_krona" in item["alias"]]
    assert len(krona) == 3
    assert all(item["file_type"] == "html" for item in krona)
    for item in krona:
        assert "<krona" in (FIXTURE / item["alias"]).read_text()


@pytest.mark.django_db
def test_invalid_import_rolls_back(catalogue, tmp_path):
    directory = tmp_path / "catalogue"
    shutil.copytree(FIXTURE, directory)
    rows = metadata()
    rows[0]["Sequence_length"] = "-1"
    with gzip.open(directory / "viruses/viruses-all-metadata.tsv.gz", "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(ValueError):
        import_catalogue.fn(str(directory), **OPTIONS, is_public=True)
    assert catalogue.fragment_entries.count() == 12


@pytest.mark.django_db
def test_dev_seed_and_json_export(assembly_analysis):
    from django.core import serializers

    assembly_analysis.mark_status(
        assembly_analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    )
    catalogue = seed_dev_viral_catalogue()
    assert catalogue.pk == "v1.0"
    entry = catalogue.fragment_entries.get(fragment__accession="MGYV9999999999")
    assert entry.source_analyses.filter(pk=assembly_analysis.pk).exists()
    payload = json.loads(serializers.serialize("json", [entry]))[0]["fields"]
    assert payload["metadata_rows"] == entry.metadata_rows


@pytest.mark.django_db(transaction=True)
def test_prefect_flow(prefect_harness):
    assert (
        import_viral_catalogue_flow(results_directory=str(FIXTURE), **OPTIONS) == "v1.0"
    )
    assert ViralCatalogueViralFragment.objects.count() == 12


@pytest.mark.django_db
def test_duplicate_pipeline_rows_are_preserved(catalogue, tmp_path):
    """Retain source rows exactly while counting distinct sequence identities."""
    directory = tmp_path / "catalogue"
    shutil.copytree(FIXTURE, directory)
    rows = metadata()
    rows.append(next(row for row in rows if row["ID"] == REP).copy())
    with gzip.open(directory / "viruses/viruses-all-metadata.tsv.gz", "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    result = import_catalogue.fn(str(directory), **OPTIONS, is_public=True)
    entry = result.fragment_entries.get(fragment__accession=REP)
    assert len(entry.metadata_rows) == 3
    assert entry.cluster_size == 2


@pytest.mark.django_db
def test_catalogue_identifier_matches_version():
    with pytest.raises(ValidationError, match="version ID"):
        import_catalogue.fn(str(FIXTURE), **{**OPTIONS, "catalogue_id": "marine"})
