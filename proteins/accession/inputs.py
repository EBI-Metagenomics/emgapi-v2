"""Step 1 of accessioning: reading the input into memory and checking it."""

import csv
import hashlib
import re
from typing import NamedTuple

from .contract import InvalidSequence, protein_hash, validate_sequence
from .fasta import open_text, read_fasta
from .gff import read_gff

PIPELINE_VERSION = re.compile(r"([0-9]\.[0-9])(\..*)?")
SPADES_COVERAGE = re.compile(r"_cov_([0-9]+(?:\.[0-9]+)?)")


class InvalidInput(ValueError):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("\n".join(problems))


class Occurrence(NamedTuple):
    gene_id: str
    description: str  # the rest of the FASTA header
    sequence: str  # validated and upper-cased
    hash: bytes
    contig: str
    start: int
    end: int
    strand: int
    caller_name: str
    caller_version: str


class Contig(NamedTuple):
    name: str
    original_name: str | None
    length: int
    hash: bytes
    kmer_coverage: float | None


class Input(NamedTuple):
    occurrences: list[Occurrence]  # in the FASTA's order
    proteins: list[tuple[bytes, str]]  # distinct (hash, sequence), sorted by hash
    contigs: list[Contig]  # only those carrying a gene


def major_minor(pipeline_version: str) -> str:
    """6.0.5 -> 6.0. The registry's version is decimal(2,1) in the release, so one digit each."""
    match = PIPELINE_VERSION.fullmatch(pipeline_version)
    if not match:
        raise InvalidInput(
            [
                f"pipeline version {pipeline_version!r} is not major.minor, one digit each"
            ]
        )
    return match[1]


def read_input(faa, gff, contigs, contig_map=None) -> Input:
    """Raises InvalidInput naming every problem found, or ValueError for a malformed file."""
    problems = []

    genes = {}
    for gene in read_gff(gff):
        if gene.id in genes:
            problems.append(f"{gene.id}: more than one GFF row")
        genes[gene.id] = gene

    occurrences, proteins, seen = [], {}, set()
    for record in read_fasta(faa):
        if record.id in seen:
            problems.append(f"{record.id}: more than one FASTA record")
            continue
        seen.add(record.id)
        gene = genes.get(record.id)
        if gene is None:
            problems.append(f"{record.id}: not in the GFF")
            continue
        try:
            sequence = validate_sequence(record.sequence).decode("ascii")
        except InvalidSequence as e:
            problems.append(f"{record.id}: {e}")
            continue
        hash = protein_hash(sequence)
        proteins[hash] = sequence
        occurrences.append(
            Occurrence(
                record.id,
                record.description,
                sequence,
                hash,
                gene.contig,
                gene.start,
                gene.end,
                gene.strand,
                gene.caller_name,
                gene.caller_version,
            )
        )
    problems += [f"{id}: not in the FASTA" for id in sorted(genes.keys() - seen)]

    original_names = read_contig_map(contig_map) if contig_map else None
    carrying = {o.contig for o in occurrences}
    found, in_file = {}, set()
    for record in read_fasta(contigs):
        if record.id not in carrying:
            continue
        if record.id in in_file:
            problems.append(f"contig {record.id}: more than one record")
        in_file.add(record.id)
        original_name = None
        if original_names is not None:
            original_name = original_names.get(record.id)
            if original_name is None:
                problems.append(f"contig {record.id}: not in the contig map")
                continue
        coverage = original_name and SPADES_COVERAGE.search(original_name)
        found[record.id] = Contig(
            record.id,
            original_name,
            len(record.sequence),
            hashlib.sha256(record.sequence.upper().encode("ascii")).digest(),
            float(coverage[1]) if coverage else None,
        )
    problems += [
        f"{o.gene_id}: contig {o.contig} is not in the contigs file"
        for o in occurrences
        if o.contig not in in_file
    ]

    if problems:
        raise InvalidInput(problems)
    return Input(occurrences, sorted(proteins.items()), list(found.values()))


def read_contig_map(path) -> dict[str, str]:
    """Each renamed contig's original name, from the pipeline's RENAME_CONTIGS mapping."""
    with open_text(path) as f:
        rows = csv.reader(f, delimiter="\t")
        if next(rows, None) != ["original", "renamed"]:
            raise ValueError(f"{path}: the header is not 'original<TAB>renamed'")
        return {renamed: original for original, renamed in rows}
