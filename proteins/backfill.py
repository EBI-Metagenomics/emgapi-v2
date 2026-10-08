"""Accessioning the v6 assembly analyses that ran before the pipeline accessioned its proteins."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from django.db import connections

from analyses.models import Analysis

TASK = Path(__file__).with_name("backfill_task.sh")
V6 = {
    version.value: version.label.removeprefix("v")
    for version in Analysis.PipelineVersions
    if version.label.startswith("v6")
}


class Pending(NamedTuple):
    accession: str
    pipeline_version: str
    faa: Path
    gff: Path
    contigs: Path


class Submitted(NamedTuple):
    run: Path
    ready: int
    missing: list[Pending]
    jobs: list[int]


def pending(cds: str, qc: str, study: str | None = None) -> list[Pending]:
    """Completed v6 assembly analyses whose accession and major.minor Tier 1 has not registered.

    Of several analyses of one assembly under one version, the newest is taken.

    :param cds: The directory of the CDS outputs, within an analysis's results directory.
    :param qc: The directory of the QC outputs, within an analysis's results directory.
    :param study: A study accession, to list only its analyses.
    """
    analyses = (
        Analysis.objects.filter_by_statuses(
            [Analysis.AnalysisStates.ANALYSIS_COMPLETED]
        )
        .filter(assembly__isnull=False, pipeline_version__in=V6, results_dir__gt="")
        .select_related("assembly")
        .order_by("id")
    )
    if study:
        analyses = analyses.filter(study__accession=study)
    with connections["proteindb"].cursor() as cursor:
        cursor.execute("SELECT accession, pipeline_version FROM proteindb.assembly")
        registered = set(cursor.fetchall())
    newest = {}
    for analysis in analyses:
        accession = analysis.assembly.first_accession
        version = V6[analysis.pipeline_version]
        if (accession, version) in registered:
            continue
        results = Path(analysis.results_dir)
        newest[accession, version] = Pending(
            accession,
            version,
            results / cds / f"{accession}_predicted_cds.faa.gz",
            results / cds / f"{accession}_predicted_cds.gff.gz",
            results / qc / f"{accession}_filtered_contigs.fasta.gz",
        )
    return list(newest.values())


def output(root: Path, analysis: Pending) -> Path:
    """Where the backfill writes an analysis's FASTA with MGYPs.

    :param root: The backfill's directory.
    :param analysis: The analysis.
    """
    return (
        root
        / analysis.accession
        / analysis.pipeline_version
        / f"{analysis.accession}_predicted_cds.faa.gz"
    )


def image(root: Path, uri: str) -> Path:
    """The image as a SIF under root, pulled once, so that the tasks do not each pull it.

    :param root: The backfill's directory.
    :param uri: The image's URI, docker://….
    """
    sif = root / "images" / (uri.rsplit("/", 1)[-1].replace(":", "_") + ".sif")
    if not sif.exists():
        sif.parent.mkdir(parents=True, exist_ok=True)
        partial = sif.with_suffix(".partial")
        partial.unlink(missing_ok=True)
        subprocess.run(
            ["singularity", "pull", str(partial), uri],
            check=True,
            capture_output=True,
            text=True,
        )
        partial.rename(sif)
    return sif


def submit(
    root: Path,
    analyses: list[Pending],
    uri: str,
    parallelism: int,
    array_size: int,
) -> Submitted:
    """Lists the analyses under a new run directory of root, and submits arrays for those with every input.

    Each array waits for the previous one, so that at most `parallelism` tasks run at once.

    :param root: The backfill's directory.
    :param analyses: The analyses to accession.
    :param uri: The mgyp-accession image's URI.
    :param parallelism: How many tasks run at once.
    :param array_size: The most tasks in one array.
    :return: The run directory, how many analyses were submitted, those left out, and the arrays' job ids.
    """
    run = root / "runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    (run / "logs").mkdir(parents=True)
    ready, missing = [], []
    for analysis in analyses:
        inputs = (analysis.faa, analysis.gff, analysis.contigs)
        (ready if all(path.is_file() for path in inputs) else missing).append(analysis)
    write(run / "missing.tsv", [list(analysis) for analysis in missing])
    write(run / "tasks.tsv", [[*a, output(root, a)] for a in ready])
    jobs = []
    if ready:
        sif = image(root, uri)
        for offset in range(0, len(ready), array_size):
            last = min(array_size, len(ready) - offset)
            after = [f"--dependency=afterany:{jobs[-1]}"] if jobs else []
            argv = [
                "sbatch",
                "--parsable",
                f"--array=1-{last}%{parallelism}",
                "--job-name=proteindb-backfill",
                "--cpus-per-task=4",
                "--mem=16G",
                "--time=4:00:00",
                f"--output={run / 'logs' / '%A_%a.out'}",
                *after,
                str(TASK),
                str(run / "tasks.tsv"),
                str(offset),
                str(sif),
            ]
            out = subprocess.run(argv, check=True, capture_output=True, text=True)
            jobs.append(int(out.stdout.split(";")[0]))
    return Submitted(run, len(ready), missing, jobs)


def write(path: Path, rows: list[list]) -> None:
    """Writes rows as TSV.

    :param path: The file to write.
    :param rows: The rows, each a list of values.
    """
    path.write_text("".join("\t".join(map(str, row)) + "\n" for row in rows))
