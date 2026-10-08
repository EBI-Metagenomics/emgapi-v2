"""Run LexicMap in the Django Tasks worker."""

import csv
import subprocess
import tempfile
import time
from pathlib import Path

from django.conf import settings

from genomes.lexicmap_schema import LexicMapMatch, SearchQuery


def parse_matches(path, catalogue):
    """Keep the best HSP for each genome; coverage remains genome-wide."""
    best = {}
    with Path(path).open() as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            if row["sgenome"] in ("", "-"):
                continue
            match = LexicMapMatch(
                genome=row["sgenome"],
                catalogue=catalogue,
                query_coverage=row["qcovGnm"],
                identity=row["pident"],
                bitscore=row["bitscore"],
                evalue=row["evalue"],
                sequence_id=row["sseqid"],
                query_start=row["qstart"],
                query_end=row["qend"],
                subject_start=row["sstart"],
                subject_end=row["send"],
                strand=row["sstr"],
            )
            previous = best.get(match.genome)
            if (
                previous is None
                or match.bitscore * match.identity
                > previous.bitscore * previous.identity
            ):
                best[match.genome] = match
    return list(best.values())


def search(query: SearchQuery, indexes: list[dict]) -> list[dict]:
    config = settings.EMG_CONFIG.lexicmap
    root = Path(config.index_root).resolve()
    deadline = time.monotonic() + config.search_timeout
    matches = []
    with tempfile.TemporaryDirectory(prefix="lexicmap-") as directory:
        fasta = Path(directory) / "query.fna"
        fasta.write_text(f">query\n{query.sequence}\n")
        output = Path(directory) / "matches.tsv"
        for index in indexes:
            target = (root / index["path"]).resolve()
            if not target.is_relative_to(root) or target == root:
                raise ValueError("Invalid LexicMap index path")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("LexicMap search timed out")
            subprocess.run(
                [
                    config.binary_path,
                    "search",
                    "-d",
                    str(target),
                    str(fasta),
                    "-o",
                    str(output),
                    "-j",
                    str(config.search_threads),
                    "--top-n-genomes",
                    str(query.max_results),
                    "--align-min-match-pident",
                    str(query.min_identity),
                    "--align-min-match-len",
                    "50",
                    "--min-qcov-per-genome",
                    str(query.min_query_coverage),
                    "--min-qcov-per-hsp",
                    "0",
                ],
                check=True,
                timeout=remaining,
                stdout=subprocess.DEVNULL,
            )
            matches.extend(parse_matches(output, index["catalogue"]))
    matches.sort(key=lambda m: m.bitscore * m.identity, reverse=True)
    return [match.model_dump() for match in matches[: query.max_results]]
