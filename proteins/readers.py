"""Streaming readers for the gene prediction files: FASTA and the merged GFF, plain or gzipped."""

import gzip
from typing import IO, Iterator, NamedTuple

STRANDS = {"+": 1, "-": -1}


class FastaRecord(NamedTuple):
    id: str  # the first word of the header
    description: str  # the rest of the header, "" if there is none
    sequence: str


def open_text(path) -> IO[str]:
    """Open a file for reading as text, decompressing it if it is gzipped.

    :param path: A plain or gzipped file.
    """
    with open(path, "rb") as f:
        gzipped = f.read(2) == b"\x1f\x8b"
    # Decoding errors raise UnicodeDecodeError, a ValueError, like any other invalid input.
    return (gzip.open if gzipped else open)(path, "rt", encoding="utf-8")


def read_fasta(path) -> Iterator[FastaRecord]:
    """Yield the records of a FASTA file.

    Only the line breaks between a record's lines are removed. The sequence is
    otherwise returned as it is: validating it is the hash contract's job.

    :param path: A plain or gzipped FASTA file.
    """
    with open_text(path) as f:
        header, lines = None, []
        for lineno, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if line.startswith(">"):
                if header is not None:
                    yield _record(header, lines)
                if not line[1:].strip():
                    raise ValueError(f"{path}:{lineno}: empty FASTA header")
                header, lines = line[1:], []
            elif header is None:
                raise ValueError(f"{path}:{lineno}: sequence before the first header")
            else:
                lines.append(line)
        if header is not None:
            yield _record(header, lines)


def _record(header: str, lines: list[str]) -> FastaRecord:
    id, *description = header.split(maxsplit=1)
    return FastaRecord(id, "".join(description), "".join(lines))


class Gene(NamedTuple):
    id: str
    contig: str
    start: int  # 1-based, inclusive
    end: int  # inclusive
    strand: int  # 1 or -1
    caller_name: str
    caller_version: str


def read_gff(path) -> Iterator[Gene]:
    """Yield a Gene for each CDS line. Other features and comments are skipped.

    :param path: A plain or gzipped GFF3 file.
    """
    with open_text(path) as f:
        for lineno, line in enumerate(f, 1):
            if line.startswith("#") or not line.strip():
                continue
            try:
                gene = _gene(line.rstrip("\n").split("\t"))
            except ValueError as e:
                raise ValueError(f"{path}:{lineno}: {e}") from None
            if gene:
                yield gene


def _gene(columns: list[str]) -> Gene | None:
    if len(columns) != 9:
        raise ValueError(f"expected 9 tab-separated columns, found {len(columns)}")
    contig, source, feature, start, end, _, strand, _, attributes = columns
    if feature != "CDS":
        return None
    # The merge writes the source as "{name}_v{version}", e.g. "Pyrodigal_v3.6.3".
    name, _, version = source.rpartition("_v")
    if not name or not version:
        raise ValueError(f"source {source!r} has no gene caller version")
    if strand not in STRANDS:
        raise ValueError(f"strand {strand!r} is neither '+' nor '-'")
    ids = [a[3:] for a in attributes.split(";") if a.startswith("ID=")]
    if len(ids) != 1 or not ids[0]:
        raise ValueError(f"expected one ID= attribute in {attributes!r}")
    return Gene(ids[0], contig, int(start), int(end), STRANDS[strand], name, version)
