import gzip
import hashlib
from pathlib import Path

import pytest

from proteins.accession.contract import protein_hash
from proteins.accession.fasta import read_fasta
from proteins.accession.inputs import InvalidInput, major_minor, read_input

FIXTURES = Path(__file__).parent.parent / "fixtures"
FAA = FIXTURES / "ERZ101.faa.gz"
GFF = FIXTURES / "ERZ101.gff.gz"
CONTIGS = FIXTURES / "ERZ101.fasta.gz"
CONTIG_MAP = FIXTURES / "ERZ101_mapping.csv"


def test_reads_the_pipeline_output():
    result = read_input(FAA, GFF, CONTIGS, CONTIG_MAP)

    records = list(read_fasta(FAA))
    assert [o.gene_id for o in result.occurrences] == [r.id for r in records]
    assert [o.description for o in result.occurrences] == [
        r.description for r in records
    ]
    assert all(o.hash == protein_hash(o.sequence) for o in result.occurrences)
    assert result.proteins == sorted({(o.hash, o.sequence) for o in result.occurrences})

    first = result.occurrences[0]
    assert (first.contig, first.start, first.end, first.strand) == (
        "ERZ101_2",
        2985,
        6581,
        1,
    )
    assert (first.caller_name, first.caller_version) == ("Pyrodigal", "3.6.3")

    contigs = {r.id: r.sequence for r in read_fasta(CONTIGS)}
    assert {c.name for c in result.contigs} == {o.contig for o in result.occurrences}
    for contig in result.contigs:
        assert contig.length == len(contigs[contig.name])
        assert contig.hash == hashlib.sha256(contigs[contig.name].encode()).digest()
        assert contig.original_name.startswith("ERZ12345-fake-contig-")
        assert contig.kmer_coverage is None


def test_without_a_contig_map_original_names_are_unknown():
    assert all(c.original_name is None for c in read_input(FAA, GFF, CONTIGS).contigs)


def write(path: Path, text: str) -> Path:
    path.write_bytes(
        gzip.compress(text.encode()) if path.suffix == ".gz" else text.encode()
    )
    return path


def gff_row(gene_id, contig="c1", source="Pyrodigal_v3.6.3"):
    return f"{contig}\t{source}\tCDS\t1\t9\t.\t-\t0\tID={gene_id}\n"


@pytest.fixture
def files(tmp_path):
    """Writes an input of genes, given as {gene_id: sequence}, on contigs c1 and c2."""

    def make(genes, gff=None, contigs=">c1\nACGTACGTA\n>c2\nacgt\n", contig_map=None):
        faa = "".join(
            f">{id} # 1 # 9 # -1 # ID=1\n{seq}\n" for id, seq in genes.items()
        )
        return (
            write(tmp_path / "in.faa.gz", faa),
            write(tmp_path / "in.gff", gff or "".join(gff_row(id) for id in genes)),
            write(tmp_path / "contigs.fasta", contigs),
            contig_map and write(tmp_path / "map.csv", contig_map),
        )

    return make


def problems(*paths):
    with pytest.raises(InvalidInput) as excinfo:
        read_input(*paths)
    return excinfo.value.problems


def test_lower_case_sequences_are_upper_cased(files):
    (occurrence,) = read_input(*files({"g1": "mkv"})).occurrences
    assert occurrence.sequence == "MKV"
    assert occurrence.hash == protein_hash("MKV")


def test_proteins_are_distinct_and_sorted_by_hash(files):
    result = read_input(*files({"g1": "MKV", "g2": "mkv", "g3": "MAG", "g4": "MKV"}))
    assert result.proteins == sorted(
        [(protein_hash("MKV"), "MKV"), (protein_hash("MAG"), "MAG")]
    )
    assert len(result.occurrences) == 4


def test_names_every_invalid_sequence(files):
    assert problems(*files({"g1": "MKß", "g2": "MKV", "g3": "M*", "g4": ""})) == [
        "g1: invalid character 'ß' at position 3",
        "g3: invalid character '*' at position 2",
        "g4: empty sequence",
    ]


def test_fasta_and_gff_must_agree(files):
    assert problems(
        *files({"g1": "MKV", "g2": "MKV"}, gff=gff_row("g1") + gff_row("g3"))
    ) == ["g2: not in the GFF", "g3: not in the FASTA"]


def test_gene_ids_must_be_unique(files):
    paths = files({"g1": "MKV"}, gff=gff_row("g1") + gff_row("g1"))
    write(paths[0], ">g1\nMKV\n>g1\nMAG\n")
    assert problems(*paths) == [
        "g1: more than one GFF row",
        "g1: more than one FASTA record",
    ]


def test_every_gene_contig_must_be_in_the_contigs_file(files):
    assert problems(
        *files(
            {"g1": "MKV", "g2": "MKV", "g3": "MAG"},
            gff=gff_row("g1") + gff_row("g2", "c3") + gff_row("g3", "c3"),
        )
    ) == [
        "g2: contig c3 is not in the contigs file",
        "g3: contig c3 is not in the contigs file",
    ]


def test_contig_names_must_be_unique(files):
    assert problems(*files({"g1": "MKV"}, contigs=">c1\nAC\n>c1\nAC\n")) == [
        "contig c1: more than one record"
    ]


def test_only_contigs_carrying_a_gene_are_kept(files):
    (contig,) = read_input(*files({"g1": "MKV"})).contigs
    assert contig.name == "c1"
    assert contig.length == 9


def test_contig_hash_is_of_the_upper_cased_sequence(files):
    (contig,) = read_input(*files({"g1": "MKV"}, gff=gff_row("g1", "c2"))).contigs
    assert contig.hash == hashlib.sha256(b"ACGT").digest()


@pytest.mark.parametrize(
    "original_name, kmer_coverage",
    [
        ("NODE_1_length_9_cov_12.5", 12.5),
        ("NODE_1_length_9_cov_3", 3.0),
        ("NODE_1_length_9_cov_3_ID_7", 3.0),
        ("k141_0 flag=1 multi=2.0000 len=9", None),
        ("contig_1", None),
    ],
)
def test_kmer_coverage_comes_from_spades_names(files, original_name, kmer_coverage):
    (contig,) = read_input(
        *files({"g1": "MKV"}, contig_map=f"original\trenamed\n{original_name}\tc1\n")
    ).contigs
    assert contig.original_name == original_name
    assert contig.kmer_coverage == kmer_coverage


def test_every_contig_carrying_a_gene_must_be_in_the_contig_map(files):
    assert problems(
        *files(
            {"g1": "MKV", "g2": "MKV"},
            gff=gff_row("g1") + gff_row("g2", "c2"),
            contig_map="original\trenamed\nx\tc1\n",
        )
    ) == ["contig c2: not in the contig map"]


def test_contig_map_must_have_the_pipeline_header(files):
    with pytest.raises(ValueError, match="header"):
        read_input(*files({"g1": "MKV"}, contig_map="x\tc1\n"))


@pytest.mark.parametrize(
    "version, expected", [("6.0", "6.0"), ("6.0.5", "6.0"), ("6.1.2", "6.1")]
)
def test_major_minor(version, expected):
    assert major_minor(version) == expected


@pytest.mark.parametrize("version", ["6.10", "10.0", "6", "6.1x", "v6.0", ""])
def test_major_minor_rejects(version):
    with pytest.raises(InvalidInput, match="one digit each"):
        major_minor(version)
