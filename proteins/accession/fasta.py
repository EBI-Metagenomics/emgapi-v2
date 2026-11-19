"""Streaming FASTA reading, for plain or gzipped files."""

import gzip
from typing import IO, Iterator, NamedTuple


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
