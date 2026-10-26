from types import SimpleNamespace

from workflows.management.commands.backfill_v6_assembly_sanntis_downloads import (
    add_missing_sanntis_downloads,
)


class FakeAnalysis:
    def __init__(self, identifier):
        self.accession = "MGYA00000001"
        self.assembly = SimpleNamespace(first_accession=identifier)
        self.downloads = []

    def add_download(self, download):
        self.downloads.append(download.model_dump(exclude={"parent_identifier"}))


def test_add_missing_sanntis_downloads_honours_dry_run(tmp_path):
    identifier = "ERZ123456"
    sanntis_dir = tmp_path / "pathways-and-systems" / "sanntis"
    sanntis_dir.mkdir(parents=True)
    (sanntis_dir / f"{identifier}_sanntis.gff.gz").write_bytes(b"gff")
    summary = sanntis_dir / f"{identifier}_sanntis_concatenated_summary.tsv.gz"
    summary.write_bytes(b"summary")
    summary.with_suffix(f"{summary.suffix}.gzi").write_bytes(b"index")
    analysis = FakeAnalysis(identifier)

    assert add_missing_sanntis_downloads(analysis, tmp_path, dry_run=True) == 2
    assert analysis.downloads == []

    assert add_missing_sanntis_downloads(analysis, tmp_path, dry_run=False) == 2
    assert {download["alias"] for download in analysis.downloads} == {
        f"{identifier}_sanntis.gff.gz",
        f"{identifier}_sanntis_concatenated_summary.tsv.gz",
    }
