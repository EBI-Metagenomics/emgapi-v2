"""The mgyp-accession command."""

import argparse
import logging
import os
from importlib.metadata import PackageNotFoundError, version

from .accession import (
    IncompleteRerun,
    accession,
    connector,
    lookup,
    retry_on_connect_failure,
    write_lookup,
    write_output,
)
from .inputs import major_minor, read_input

logger = logging.getLogger("mgyp-accession")

EXIT_FAILURE = 1  # retried by Nextflow
EXIT_INVALID_INPUT = 2
EXIT_INCOMPLETE_RERUN = 3


def package_version() -> str:
    try:
        return version("mgyp-accession")
    except PackageNotFoundError:  # not installed, as when run from emgapi-v2's checkout
        return "unknown"


def positive_int(value: str) -> int:
    if not value.isdigit() or int(value) < 1:
        raise argparse.ArgumentTypeError(f"{value!r} is not a positive integer")
    return int(value)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mgyp-accession",
        description="Give every protein predicted for an assembly its MGYP, and stage"
        " the new proteins, the contigs and the occurrences for the daily load."
        " The Protein DB is reached through the libpq connection string in PROTEINDB_DSN.",
    )
    parser.add_argument("--version", action="version", version=package_version())
    parser.add_argument(
        "--assembly", required=True, help="ENA assembly accession, e.g. ERZ101"
    )
    parser.add_argument(
        "--pipeline-version",
        required=True,
        help="only major.minor is used (6.0.5 -> 6.0), one digit each",
    )
    parser.add_argument(
        "--faa", required=True, help="protein FASTA of the combined gene caller"
    )
    parser.add_argument(
        "--gff", required=True, help="merged GFF of the combined gene caller"
    )
    parser.add_argument(
        "--contigs", required=True, help="the contigs the genes were called on"
    )
    parser.add_argument(
        "--contig-map",
        help="RENAME_CONTIGS mapping, TSV with header 'original<TAB>renamed'",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="the protein FASTA with MGYPs (gzipped if it ends in .gz)",
    )
    parser.add_argument(
        "--connections",
        type=positive_int,
        default=16,
        help="connections used for lookups (default: %(default)s)",
    )
    parser.add_argument(
        "--lookup-only",
        action="store_true",
        help="look up only, never writing to the database: --out is a TSV of"
        " gene ID and MGYP, empty when not found",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        format="%(name)s: %(levelname)s: %(message)s", level=logging.INFO
    )
    args = parse_args(argv)

    try:
        pipeline_version = major_minor(args.pipeline_version)
        input = read_input(args.faa, args.gff, args.contigs, args.contig_map)
    except ValueError as e:
        logger.error("invalid input:\n%s", e)
        return EXIT_INVALID_INPUT

    dsn = os.environ.get("PROTEINDB_DSN")
    if not dsn:
        logger.error("PROTEINDB_DSN is not set")
        return EXIT_FAILURE
    connect = connector(dsn, f"mgyp-accession {args.assembly}")

    if args.lookup_only:
        hashes = [hash for hash, _ in input.proteins]
        ids = retry_on_connect_failure(
            lambda: lookup(connect, hashes, args.connections)
        )
        write_lookup(args.out, input.occurrences, ids)
        found = sum(o.hash in ids for o in input.occurrences)
        print(f"found: {found}\tnot found: {len(input.occurrences) - found}")
        return 0

    try:
        ids = retry_on_connect_failure(
            lambda: accession(
                connect, args.assembly, pipeline_version, input, args.connections
            )
        )
    except IncompleteRerun as e:
        logger.error("%s", e)
        return EXIT_INCOMPLETE_RERUN
    write_output(args.out, input.occurrences, ids)
    return 0
