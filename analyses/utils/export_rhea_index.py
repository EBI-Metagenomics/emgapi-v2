#!/usr/bin/env python3
"""Export compact molecule sets on a datamover, using only the standard library."""

import argparse
import csv
import gzip
import logging
import re
import stat
import subprocess
import tempfile
from pathlib import Path, PurePosixPath

RHEA_HEADER = [
    "contig_id",
    "protein_id",
    "protein_hash",
    "rhea_id",
    "chebi_reaction",
    "reaction",
    "top_hit",
]


def read_chebi_ids(path: Path) -> list[int]:
    """Read the unique participants from a plain or compressed Rhea table.

    :param path: The ASAv6 proteins2rhea table.
    :return: Sorted ChEBI numbers.
    :raises ValueError: If a row does not match the pipeline format.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    compounds = set()
    with opener(path, "rt", newline="") as stream:
        for number, row in enumerate(csv.reader(stream, delimiter="\t"), 1):
            if not row or row[0].startswith("#") or row == RHEA_HEADER:
                continue
            if len(row) != 7 or not re.fullmatch(r"RHEA:\d+", row[3]):
                raise ValueError(f"{path}: invalid proteins2rhea row {number}")
            participants = {
                int(identifier) for identifier in re.findall(r"\bCHEBI:(\d+)\b", row[4])
            }
            if not row[1] or not participants:
                raise ValueError(
                    f"{path}: missing protein or compounds in row {number}"
                )
            compounds.update(participants)
    return sorted(compounds)


def relative_rhea_path(value: str) -> str:
    """Validate a relative FTP path before rsync.

    :param value: A path relative to the configured FTP root.
    :return: The normalized path.
    :raises ValueError: If the path escapes the root or contains control characters.
    """
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or any(ord(char) < 32 for char in value)
        or path.suffix not in {".gz", ".tsv"}
    ):
        raise ValueError(f"Invalid relative Rhea path: {value!r}")
    return str(path)


def export_index(
    manifest: Path,
    ftp_root: Path,
    workdir: Path,
    output: Path,
    limit: int | None = None,
) -> int:
    """Copy manifest files and export one PostgreSQL array per MGYA accession.

    :param manifest: A TSV with analysis_accession, pipeline_version and rhea_path.
    :param ftp_root: The production results filesystem root on the datamover.
    :param workdir: A writable directory for the retained Rhea files.
    :param output: The compact TSV destination.
    :param limit: Optional number of analyses for a smaller test export.
    Missing source files appear in ``<output stem>.missing.tsv``.
    Existing cache files cannot replace missing production files.

    :return: The number of exported analyses, excluding missing sources.
    :raises ValueError: If manifest rows are invalid or duplicated.
    """
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    entries = []
    seen = set()
    with manifest.open(newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        required = {"analysis_accession", "pipeline_version", "rhea_path"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"Manifest requires columns: {sorted(required)}")
        for row in reader:
            accession = row["analysis_accession"]
            if not re.fullmatch(r"MGYA\d{8}", accession or ""):
                raise ValueError(f"Invalid analysis accession: {accession!r}")
            if not re.fullmatch(r"V6(?:\.\d+)*", row["pipeline_version"] or ""):
                raise ValueError(f"Not an ASAv6 analysis: {accession}")
            if accession in seen:
                raise ValueError(f"Duplicate analysis: {accession}")
            seen.add(accession)
            entries.append((accession, relative_rhea_path(row["rhea_path"])))
            if limit and len(entries) >= limit:
                break
    if not entries:
        raise ValueError("Manifest contains no analyses")
    if not ftp_root.is_dir():
        raise FileNotFoundError(f"FTP root is not an accessible directory: {ftp_root}")
    output.parent.mkdir(parents=True, exist_ok=True)
    available = []
    missing_report = output.with_suffix(".missing.tsv")
    with missing_report.open("w", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(["analysis_accession", "rhea_path", "reason"])
        for accession, relative in entries:
            try:
                source_stat = (ftp_root / relative).stat()
            except FileNotFoundError:
                writer.writerow([accession, relative, "source_missing"])
                logging.warning("Skipping %s: source missing: %s", accession, relative)
                continue
            if not stat.S_ISREG(source_stat.st_mode):
                raise ValueError(f"Source is not a regular file: {ftp_root / relative}")
            available.append((accession, relative))
    logging.info("Missing source report: %s", missing_report)
    if not available:
        raise ValueError(
            f"No source files available; check FTP root and {missing_report}"
        )
    entries = available
    workdir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=workdir, suffix=".files") as files:
        files.write("\n".join(sorted({path for _, path in entries})) + "\n")
        files.flush()
        subprocess.run(
            [
                "rsync",
                "-rt",
                f"--files-from={files.name}",
                "--",
                str(ftp_root.resolve()) + "/",
                str(workdir.resolve()) + "/",
            ],
            check=True,
        )
    # Publish only complete exports. Failed reruns preserve the previous output.
    with tempfile.NamedTemporaryFile(
        mode="w", dir=output.parent, newline="", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
            writer.writerow(["analysis_accession", "chebi_ids"])
            for accession, relative in entries:
                identifiers = read_chebi_ids(workdir / relative)
                writer.writerow(
                    [accession, "{" + ",".join(map(str, identifiers)) + "}"]
                )
                logging.info("%s: %s unique ChEBIs", accession, len(identifiers))
            stream.close()
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
    return len(entries)


def main() -> None:
    """Run the standalone datamover export command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--ftp-root", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    count = export_index(**vars(args))
    logging.info("Exported %s analyses to %s", count, args.output)


if __name__ == "__main__":
    main()
