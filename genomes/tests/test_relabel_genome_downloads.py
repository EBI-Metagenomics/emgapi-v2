from copy import deepcopy
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from analyses.models import Biome
from genomes.tests.test_models import make_catalogue_genome, make_release, make_series


@pytest.mark.django_db
def test_relabel_only_changes_download_labels():
    biome = Biome.objects.create(biome_name="root", path="root")
    series = make_series("Ocean", "Ocean", biome)
    release = make_release(series, "ocean-v1", "1")
    other_release = make_release(series, "ocean-v2", "2")
    entry = make_catalogue_genome(release)
    other_entry = make_catalogue_genome(other_release)
    entry.downloads = [
        {
            "path": "genome/MGYG000000001.fna.gz",
            "alias": "MGYG000000001.fna.gz",
            "download_group": "Genome analysis",
            "download_type": "Genome analysis",
            "index_file": [
                {"index_type": "fai", "path": "genome/MGYG000000001.fna.gz.fai"},
                {"index_type": "gzi", "path": "genome/MGYG000000001.fna.gz.gzi"},
            ],
            "file_size_bytes": 123,
            "short_description": "Nucleic Acid Sequence",
            "long_description": "Genome assembly in FASTA format",
            "extra_metadata": {"keep": True},
        },
        {
            "path": "genome/MGYG000000001_crisprcasfinder.tsv",
            "download_group": "Genome analysis",
            "download_type": "Genome analysis",
        },
    ]
    entry.save()
    other_entry.downloads = deepcopy(entry.downloads)
    other_entry.save()
    before = deepcopy(entry.__dict__)

    output = StringIO()
    call_command("relabel_genome_downloads", release.pk, dry_run=True, stdout=output)
    entry.refresh_from_db()
    assert entry.downloads == before["downloads"]
    assert "Would update downloads for 1" in output.getvalue()

    call_command("relabel_genome_downloads", release.pk, stdout=StringIO())
    entry.refresh_from_db()
    expected = deepcopy(before["downloads"])
    expected[0].update(
        download_group="genome_sequence.assembly", download_type="Genome sequence"
    )
    expected[1].update(
        download_group="genome_analysis.crispr", download_type="Genome analysis"
    )
    assert entry.downloads == expected
    for field, value in before.items():
        if not field.startswith("_") and field != "downloads":
            assert getattr(entry, field) == value
    other_entry.refresh_from_db()
    assert other_entry.downloads == before["downloads"]

    output = StringIO()
    call_command("relabel_genome_downloads", release.pk, stdout=output)
    assert "Updated downloads for 0" in output.getvalue()

    call_command("relabel_genome_downloads", stdout=StringIO())
    other_entry.refresh_from_db()
    assert other_entry.downloads == expected


@pytest.mark.django_db
def test_relabel_rejects_unknown_catalogue():
    with pytest.raises(CommandError, match="Unknown catalogue IDs: missing"):
        call_command("relabel_genome_downloads", "missing")
