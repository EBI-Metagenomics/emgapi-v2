"""Tests for the standalone datamover export."""

import gzip
from pathlib import Path
from unittest.mock import patch

import pytest

from analyses.utils.export_rhea_index import (
    RHEA_HEADER,
    export_index,
    read_chebi_ids,
    relative_rhea_path,
)

ROW = "contig\tprotein\thash\tRHEA:123\tCHEBI:9 + CHEBI:1 = CHEBI:9\treaction\tTrue\n"


@pytest.mark.parametrize("compressed", [False, True])
def test_parser_reads_unique_participants(tmp_path, compressed):
    path = tmp_path / ("rhea.tsv.gz" if compressed else "rhea.tsv")
    opener = gzip.open if compressed else open
    with opener(path, "wt") as stream:
        stream.write("# comment\n" + "\t".join(RHEA_HEADER) + "\n" + ROW * 2)
    assert read_chebi_ids(path) == [1, 9]


def test_parser_empty_and_invalid(tmp_path):
    path = tmp_path / "rhea.tsv"
    path.write_text("")
    assert read_chebi_ids(path) == []
    path.write_text("not\ta\trhea\ttable\n")
    with pytest.raises(ValueError, match="row 1"):
        read_chebi_ids(path)


@pytest.mark.parametrize(
    "value", ["../x.tsv.gz", "/x.tsv.gz", "x\ny.tsv.gz", "x.txt", ""]
)
def test_rejects_unsafe_paths(value):
    with pytest.raises(ValueError):
        relative_rhea_path(value)


def test_export_copies_only_selected_files_and_preserves_previous_output(tmp_path):
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(
        "analysis_accession\tpipeline_version\trhea_path\n"
        "MGYA00000001\tV6\tstudy/a.tsv.gz\n"
        "MGYA00000002\tV6.1\tstudy/b.tsv.gz\n"
    )
    workdir = tmp_path / "cache"
    (workdir / "study").mkdir(parents=True)
    path = workdir / "study/a.tsv.gz"
    with gzip.open(path, "wt") as stream:
        stream.write(ROW)
    (tmp_path / "ftp/study").mkdir(parents=True)
    (tmp_path / "ftp/study/a.tsv.gz").write_bytes(path.read_bytes())
    output = tmp_path / "index.tsv"

    def rsync(command, check):
        assert check
        assert command[:2] == ["rsync", "-rt"]
        files = Path(command[2].split("=", 1)[1])
        assert files.read_text() == "study/a.tsv.gz\n"
        assert command[-2:] == [str(tmp_path / "ftp") + "/", str(workdir) + "/"]

    with patch("analyses.utils.export_rhea_index.subprocess.run", side_effect=rsync):
        assert export_index(manifest, tmp_path / "ftp", workdir, output, limit=1) == 1
        expected = "analysis_accession\tchebi_ids\nMGYA00000001\t{1,9}\n"
        assert output.read_text() == expected
        with gzip.open(path, "wt") as stream:
            stream.write("broken\n")
        with pytest.raises(ValueError):
            export_index(manifest, tmp_path / "ftp", workdir, output, limit=1)
        assert output.read_text() == expected


@pytest.mark.parametrize(
    "rows",
    [
        "MGYA00000001\tV5\ta.tsv\n",
        "bad\tV6\ta.tsv\n",
        "MGYA00000001\tV6\ta.tsv\nMGYA00000001\tV6\tb.tsv\n",
        "",
    ],
)
def test_export_rejects_invalid_manifest_before_rsync(tmp_path, rows):
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text("analysis_accession\tpipeline_version\trhea_path\n" + rows)
    with patch("analyses.utils.export_rhea_index.subprocess.run") as rsync:
        with pytest.raises(ValueError):
            export_index(manifest, tmp_path, tmp_path / "cache", tmp_path / "index.tsv")
        rsync.assert_not_called()


def test_export_skips_missing_sources_even_with_cached_files(tmp_path):
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(
        "analysis_accession\tpipeline_version\trhea_path\n"
        "MGYA00000001\tV6\ta.tsv\n"
        "MGYA00000002\tV6\tmissing.tsv\n"
    )
    ftp = tmp_path / "ftp"
    ftp.mkdir()
    (ftp / "a.tsv").write_text(ROW)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "a.tsv").write_text(ROW)
    (cache / "missing.tsv").write_text(ROW)
    output = tmp_path / "index.tsv"

    def rsync(command, check):
        files = Path(command[2].split("=", 1)[1])
        assert files.read_text() == "a.tsv\n"

    with patch("analyses.utils.export_rhea_index.subprocess.run", side_effect=rsync):
        assert export_index(manifest, ftp, cache, output) == 1
    assert output.read_text() == "analysis_accession\tchebi_ids\nMGYA00000001\t{1,9}\n"
    assert output.with_suffix(".missing.tsv").read_text() == (
        "analysis_accession\trhea_path\treason\n"
        "MGYA00000002\tmissing.tsv\tsource_missing\n"
    )


def test_export_all_sources_missing_preserves_output(tmp_path):
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(
        "analysis_accession\tpipeline_version\trhea_path\n"
        "MGYA00000001\tV6\tmissing.tsv\n"
    )
    output = tmp_path / "index.tsv"
    output.write_text("previous export\n")
    with patch("analyses.utils.export_rhea_index.subprocess.run") as rsync:
        with pytest.raises(ValueError, match="No source files available"):
            export_index(manifest, tmp_path, tmp_path / "cache", output)
        rsync.assert_not_called()
    assert output.read_text() == "previous export\n"
    assert "source_missing" in output.with_suffix(".missing.tsv").read_text()
