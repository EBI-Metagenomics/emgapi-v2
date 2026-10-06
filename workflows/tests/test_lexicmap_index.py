import shlex
from pathlib import Path
from unittest.mock import patch

import pytest

from genomes.models import CatalogueGenome, GenomeSearchIndex
from workflows.flows.lexicmap_index import EMG_CONFIG, make_lexicmap_index


@pytest.mark.django_db
@pytest.mark.parametrize("publish_fails", [False, True])
def test_index_replacement(genomes, tmp_path, monkeypatch, publish_fails):
    catalogue = CatalogueGenome.objects.first().catalogue
    previous = GenomeSearchIndex.objects.create(
        catalogue=catalogue,
        backend="lexicmap",
        status="ACTIVE",
        is_active=True,
        artifact_path="old/index.lmi",
    )
    monkeypatch.setattr(EMG_CONFIG.lexicmap, "build_root", str(tmp_path / "build"))
    for accession in catalogue.genomes.values_list("genome__accession", flat=True):
        genome = tmp_path / "website" / accession / "genome"
        genome.mkdir(parents=True)
        (genome / f"{accession}.fna").write_text(">contig\nACGT\n")

    def job(**kwargs):
        if kwargs["name"].startswith("Build"):
            command = shlex.split(kwargs["command"])
            output = Path(command[command.index("-O") + 1])
            output.mkdir()
            (output / "info.toml").touch()
        elif publish_fails:
            raise RuntimeError("Copy failed")

    with patch("workflows.flows.lexicmap_index.run_cluster_job", side_effect=job):
        if publish_fails:
            with pytest.raises(RuntimeError, match="Copy failed"):
                make_lexicmap_index.fn(
                    catalogue_slug=catalogue.pk, results_directory=str(tmp_path)
                )
        else:
            make_lexicmap_index.fn(
                catalogue_slug=catalogue.pk, results_directory=str(tmp_path)
            )
    previous.refresh_from_db()
    index = GenomeSearchIndex.objects.exclude(pk=previous.pk).get()
    assert previous.is_active == publish_fails
    assert index.is_active == (not publish_fails)
    assert index.status == ("FAILED" if publish_fails else "ACTIVE")
