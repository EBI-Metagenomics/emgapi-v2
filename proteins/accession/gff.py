"""Reading the merged GFF of the pipeline's combined gene caller."""

from typing import Iterator, NamedTuple

from .fasta import open_text

STRANDS = {"+": 1, "-": -1}


class Gene(NamedTuple):
    id: str
    contig: str
    start: int  # 1-based, inclusive
    end: int  # inclusive
    strand: int  # 1 or -1
    caller_name: str
    caller_version: str


def read_gff(path) -> Iterator[Gene]:
    """Yield a Gene for each CDS line. Other features and comments are skipped."""
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
