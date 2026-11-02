import gzip
import shutil
from collections import Counter
from pathlib import Path

import pytest

from proteins.accession.contract import validate_sequence
from proteins.accession.fasta import FastaRecord, read_fasta
from proteins.accession.gff import Gene, read_gff

# The pipeline's tests/assembly_erz101.fasta.gz with its contigs renamed as the pipeline
# does, run through its COMBINED_GENE_CALLER subworkflow with the caller versions the
# pipeline uses (Pyrodigal 3.6.3, FragGeneScanRS 1.1.0, mgnify-pipelines-toolkit 1.4.17).
FIXTURES = Path(__file__).parent.parent / "fixtures"
FAA = FIXTURES / "ERZ101.faa.gz"
GFF = FIXTURES / "ERZ101.gff.gz"


def write(tmp_path, text, name="input"):
    path = tmp_path / name
    path.write_text(text)
    return path


# FASTA


def test_reads_the_gene_callers_protein_fasta():
    records = {r.id: r for r in read_fasta(FAA)}
    assert len(records) == 105
    record = records["ERZ101_1_1"]
    assert record.description.startswith("# 1399 # 1947 # 1 # ID=1_1;partial=00;")
    # Lines of 60, 60, 60 and 2 residues: (1947 - 1399 + 1) / 3 codons, less the stop.
    assert len(record.sequence) == 182
    assert record.sequence.startswith(
        "MKKFLSSAFALFMLSMSTAFAQLPSVTLRTIEGKTIDTAKLSNDGKPFIISFFASWCKPCN"
    )
    assert record.sequence.endswith("SLPTVESKATENNK")


def test_gene_caller_output_passes_the_hash_contract_unchanged():
    for record in read_fasta(FAA):
        assert validate_sequence(record.sequence) == record.sequence.encode()


def test_plain_and_gzipped_files_read_the_same(tmp_path):
    plain = tmp_path / "ERZ101.faa"
    with gzip.open(FAA, "rb") as src, open(plain, "wb") as dst:
        shutil.copyfileobj(src, dst)
    assert list(read_fasta(plain)) == list(read_fasta(FAA))


def test_only_line_breaks_are_removed(tmp_path):
    # Nothing else is changed: validation is the contract's job.
    path = write(tmp_path, ">a  desc\twith tabs\nmK\nV*\r\n>b\n>c\tdesc\nX\n")
    assert list(read_fasta(path)) == [
        FastaRecord("a", "desc\twith tabs", "mKV*"),
        FastaRecord("b", "", ""),
        FastaRecord("c", "desc", "X"),
    ]


def test_empty_file_has_no_records(tmp_path):
    assert list(read_fasta(write(tmp_path, ""))) == []


@pytest.mark.parametrize(
    "text, error",
    [
        ("MKV\n>a\nMKV\n", r":1: sequence before the first header"),
        (">a\nMKV\n> \nMKV\n", r":3: empty FASTA header"),
    ],
)
def test_malformed_fasta_is_rejected(tmp_path, text, error):
    with pytest.raises(ValueError, match=error):
        list(read_fasta(write(tmp_path, text)))


def test_invalid_utf8_is_rejected_as_invalid_input(tmp_path):
    path = tmp_path / "input"
    path.write_bytes(b">a\nMK\xffV\n")
    with pytest.raises(ValueError):
        list(read_fasta(path))


# GFF


def test_reads_the_gene_callers_merged_gff():
    genes = {g.id: g for g in read_gff(GFF)}
    assert len(genes) == 105
    assert genes["ERZ101_1_1"] == Gene(
        "ERZ101_1_1", "ERZ101_1", 1399, 1947, 1, "Pyrodigal", "3.6.3"
    )
    assert genes["ERZ101_1_1_160_+"] == Gene(
        "ERZ101_1_1_160_+", "ERZ101_1", 1, 160, 1, "FragGeneScanRS", "1.1.0"
    )
    assert Counter((g.caller_name, g.caller_version) for g in genes.values()) == {
        ("Pyrodigal", "3.6.3"): 94,
        ("FragGeneScanRS", "1.1.0"): 11,
    }
    assert Counter(g.strand for g in genes.values()) == {1: 53, -1: 52}


def test_fasta_and_gff_fixtures_have_the_same_genes():
    assert {r.id for r in read_fasta(FAA)} == {g.id for g in read_gff(GFF)}


def gff_line(source="Pyrodigal_v3.6.3", feature="CDS", strand="-", attributes="ID=g1"):
    return f"c1\t{source}\t{feature}\t10\t99\t.\t{strand}\t0\t{attributes}\n"


def test_comments_blank_lines_and_other_features_are_skipped(tmp_path):
    text = "##gff-version 3\n\n" + gff_line(feature="gene") + gff_line()
    assert list(read_gff(write(tmp_path, text))) == [
        Gene("g1", "c1", 10, 99, -1, "Pyrodigal", "3.6.3")
    ]


def test_source_is_split_at_the_last_v(tmp_path):
    (gene,) = read_gff(write(tmp_path, gff_line(source="My_vcaller_v2.1_beta")))
    assert (gene.caller_name, gene.caller_version) == ("My_vcaller", "2.1_beta")


@pytest.mark.parametrize(
    "line, error",
    [
        # As written by mgnify-pipelines-toolkit when the pipeline gives no versions.
        (gff_line(source="Pyrodigal"), "'Pyrodigal' has no gene caller version"),
        (gff_line(source="Pyrodigal_v"), "has no gene caller version"),
        (gff_line(source="_v3.6.3"), "has no gene caller version"),
        (gff_line(strand="."), "strand '.' is neither"),
        (gff_line(attributes="Name=g1"), "expected one ID= attribute"),
        (gff_line(attributes="ID=g1;ID=g2"), "expected one ID= attribute"),
        (gff_line(attributes="ID="), "expected one ID= attribute"),
        ("c1\tPyrodigal_v3.6.3\tCDS\t10\t99\n", "expected 9 tab-separated columns"),
        (gff_line().replace("\t10\t", "\tten\t"), "invalid literal for int"),
    ],
)
def test_malformed_gff_is_rejected_with_its_line_number(tmp_path, line, error):
    path = write(tmp_path, "##gff-version 3\n" + line)
    with pytest.raises(ValueError, match=":2: ") as excinfo:
        list(read_gff(path))
    assert error in str(excinfo.value)
