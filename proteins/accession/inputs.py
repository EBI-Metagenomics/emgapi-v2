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
PARTIAL = re.compile(r"(?<![^;\s])partial=([^;\s]*)")
TRUNCATIONS = {"00", "01", "10", "11"}


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
    truncation: str | None  # Pyrodigal's partial=XY; None for FragGeneScanRS


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
    """6.0.5 -> 6.0. The registry's version is decimal(2,1) in the release, so one digit each.

    :param pipeline_version: The pipeline's version, e.g. 6.0.5.
    """
    match = PIPELINE_VERSION.fullmatch(pipeline_version)
    if not match:
        raise InvalidInput(
            [
                f"pipeline version {pipeline_version!r} is not major.minor, one digit each"
            ]
        )
    return match[1]


def read_genes(faa) -> list[tuple[str, bytes]]:
    """Each gene's id and protein hash, in the FASTA's order. Raises InvalidInput naming every problem found.

    :param faa: A protein FASTA.
    """
    genes, seen, problems = [], set(), []
    for record in read_fasta(faa):
        if record.id in seen:
            problems.append(f"{record.id}: more than one FASTA record")
            continue
        seen.add(record.id)
        try:
            genes.append((record.id, protein_hash(record.sequence)))
        except InvalidSequence as e:
            problems.append(f"{record.id}: {e}")
    if problems:
        raise InvalidInput(problems)
    return genes


def read_input(faa, gff, contigs, contig_map=None) -> Input:
    """Raises InvalidInput naming every problem found, or ValueError for a malformed file.

    :param faa: The combined gene caller's protein FASTA.
    :param gff: Its merged GFF.
    :param contigs: The contigs the genes were called on.
    :param contig_map: The RENAME_CONTIGS mapping, if any.
    :return: The analysis's proteins, contigs and occurrences.
    """
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
        truncation = None
        if gene.caller_name == "Pyrodigal":
            flags = PARTIAL.findall(record.description)
            if len(flags) != 1 or flags[0] not in TRUNCATIONS:
                problems.append(f"{record.id}: no single valid partial= flag")
                continue
            truncation = flags[0]
        elif gene.caller_name != "FragGeneScanRS":
            problems.append(
                f"{record.id}: gene caller {gene.caller_name!r} is neither Pyrodigal nor FragGeneScanRS"
            )
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
                truncation,
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
    """Each renamed contig's original name, from the pipeline's RENAME_CONTIGS mapping.

    :param path: A TSV with the header 'original<TAB>renamed'.
    """
    with open_text(path) as f:
        rows = csv.reader(f, delimiter="\t")
        if next(rows, None) != ["original", "renamed"]:
            raise ValueError(f"{path}: the header is not 'original<TAB>renamed'")
        return {renamed: original for original, renamed in rows}
