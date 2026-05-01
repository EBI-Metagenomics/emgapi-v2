from __future__ import annotations

import csv
import gzip
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypedDict

from prefect import get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.deployments import run_deployment

from activate_django_first import EMG_CONFIG  # noqa: F401

from analyses.models import Analysis
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
    django_db_task as task,
)
from workflows.prefect_utils.build_cli_command import cli_command

EGGNOG_FOLDER = "eggnog"
EGGNOG_ANNOTATIONS_SUFFIX = "_emapper_annotations.tsv.gz"
EGGNOG_DOWNLOAD_GROUP = f"{Analysis.FUNCTIONAL_ANNOTATION}.eggnog"


class EggnogHeaderFixCandidate(TypedDict, total=False):
    """Shape of a candidate EggNOG file that needs its duplicate header removed."""

    analysis_accession: str
    download_alias: str
    nfs_path: str
    external_path: str
    has_duplicate_header: bool
    backup_path: str


def _analysis_eggnog_annotation_downloads(analysis: Analysis):
    """Return EggNOG annotation downloads for one analysis.

    :param analysis: Analysis to inspect.
    """
    return [
        download
        for download in analysis.downloads_as_objects
        if str(download.path).endswith(EGGNOG_ANNOTATIONS_SUFFIX)
    ]


def _eggnog_analyses(analysis_accessions: list[str] | None = None):
    """Return assembly analyses with EggNOG downloads.

    :param analysis_accessions: Optional accession filter.
    """
    analyses = (
        Analysis.objects.filter(
            assembly__isnull=False,
            downloads__contains=[{"download_group": EGGNOG_DOWNLOAD_GROUP}],
        )
        .select_related("assembly")
        .order_by("accession")
    )
    if analysis_accessions is not None:
        analyses = analyses.filter(accession__in=analysis_accessions)
    return analyses


def _build_candidate_rows(analysis: Analysis) -> list[EggnogHeaderFixCandidate]:
    """Build review rows for EggNOG annotation files with repeated headers.

    :param analysis: Analysis to inspect.
    """
    if not analysis.results_dir:
        return []

    results_root = Path(analysis.results_dir)

    downloads = _analysis_eggnog_annotation_downloads(analysis)
    if not downloads:
        return []

    if analysis.external_results_dir:
        external_root = (
            EMG_CONFIG.slurm.private_results_dir
            if analysis.is_private
            else EMG_CONFIG.slurm.ftp_results_dir
        )
        external_dir = Path(external_root) / analysis.external_results_dir
    else:
        external_dir = None

    rows: list[EggnogHeaderFixCandidate] = []
    for download in downloads:
        nfs_path = results_root / str(download.path)
        if nfs_path.is_file() and has_duplicated_header_lines(nfs_path):
            if external_dir is not None:
                external_path = str(external_dir / str(download.path))
            else:
                external_path = ""

            rows.append(
                {
                    "analysis_accession": analysis.accession,
                    "download_alias": download.alias,
                    "nfs_path": str(nfs_path),
                    "external_path": external_path,
                    "has_duplicate_header": True,
                }
            )
    return rows


def has_duplicated_header_lines(path: Path) -> bool:
    """Return True when a gzipped EggNOG TSV contains a repeated header row.

    :param path: Gzipped EggNOG TSV path.
    """
    with gzip.open(path, "rt", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        header = next(reader, None)
        if header is None:
            return False

        for row in reader:
            if row == header:
                return True

    return False


def remove_duplicate_header_lines(path: Path) -> int:
    """Rewrite a gzipped TSV in place, preserving only the first header line.

    :param path: Gzipped EggNOG TSV path.
    """
    with TemporaryDirectory(dir=path.parent) as tmpdir:
        temp_path = Path(tmpdir) / path.name

        with (
            gzip.open(path, "rt", newline="") as source_handle,
            gzip.open(
                temp_path,
                "wt",
                newline="",
            ) as temp_handle,
        ):
            reader = csv.reader(source_handle, delimiter="\t")
            writer = csv.writer(temp_handle, delimiter="\t", lineterminator="\n")
            header = next(reader, None)
            if header is None:
                return 0

            duplicate_headers = 0
            writer.writerow(header)
            for row in reader:
                if row == header:
                    duplicate_headers += 1
                    continue
                writer.writerow(row)

        if duplicate_headers == 0:
            return 0

        shutil.copystat(path, temp_path)
        temp_path.replace(path)
        return duplicate_headers


@task(name="Collect EggNOG header fix candidates")
def collect_eggnog_header_fix_candidates(
    analysis_accessions: list[str] | None = None,
) -> list[EggnogHeaderFixCandidate]:
    """Collect files with duplicated EggNOG headers and publish a review artifact.

    :param analysis_accessions: Optional accession filter.
    """
    logger = get_run_logger()
    candidates: list[EggnogHeaderFixCandidate] = []
    for analysis in _eggnog_analyses(analysis_accessions):
        candidates.extend(_build_candidate_rows(analysis))

    create_table_artifact(
        key="eggnog-header-fix-candidates",
        table=candidates,
        description=(
            f"EggNOG annotation TSV files with duplicated headers "
            f"({len(candidates)} file(s) found)"
        ),
    )

    logger.info(f"Found {len(candidates)} EggNOG annotation files with bad headers")
    return candidates


@task(name="Repair EggNOG header files")
def repair_eggnog_header_files(
    candidates: list[EggnogHeaderFixCandidate],
) -> list[EggnogHeaderFixCandidate]:
    """Remove duplicate headers from each candidate file on NFS and back it up.

    :param candidates: Candidate files to repair.
    """
    logger = get_run_logger()
    repaired: list[EggnogHeaderFixCandidate] = []

    for candidate in candidates:
        path = Path(candidate["nfs_path"])
        backup_path = path.with_name(f"{path.name}.bak")
        logger.info(f"Repairing {path}")
        if not backup_path.exists():
            shutil.copy2(path, backup_path)
            logger.info(f"Backed up original file to {backup_path}")

        removed_headers = remove_duplicate_header_lines(path)
        if removed_headers > 0:
            repaired_candidate = dict(candidate)
            repaired_candidate["backup_path"] = str(backup_path)
            repaired.append(repaired_candidate)
            logger.info(f"Removed {removed_headers} duplicate header(s) from {path}")
        else:
            logger.info(f"No duplicate headers found in {path}; skipping rewrite")

    return repaired


@task(name="Resync EggNOG results to FTP")
def resync_eggnog_results_to_ftp(
    candidates: list[EggnogHeaderFixCandidate],
) -> list[str]:
    """Re-sync repaired EggNOG TSVs from NFS to the external tree.

    :param candidates: Repaired candidate files to sync.
    """
    logger = get_run_logger()

    synced_analyses: list[str] = []
    analyses = _eggnog_analyses(
        sorted({candidate["analysis_accession"] for candidate in candidates})
    )
    candidate_paths = {Path(candidate["nfs_path"]) for candidate in candidates}

    for analysis in analyses:
        if not analysis.results_dir or not analysis.external_results_dir:
            continue

        if analysis.is_private:
            mirror_root = EMG_CONFIG.slurm.private_results_dir
        else:
            mirror_root = EMG_CONFIG.slurm.ftp_results_dir

        source_root = Path(analysis.results_dir)
        target_root = Path(mirror_root) / analysis.external_results_dir
        source_files = [
            path for path in candidate_paths if path.is_relative_to(source_root)
        ]
        if not source_files:
            continue

        logger.info(f"Re-syncing EggNOG results for {analysis.accession}")

        run_deployment(
            name="move-data/move_data_deployment",
            parameters={
                "move_command": cli_command(["rsync", "-av"]),
                "source": [str(path) for path in sorted(source_files)],
                "target": str(target_root),
            },
            job_variables={
                "partition": EMG_CONFIG.slurm.datamover_partition,
            },
        )
        synced_analyses.append(analysis.accession)

    logger.info(
        f"Re-synced EggNOG results for {len(synced_analyses)} analyfisis/analyses"
    )
    return synced_analyses


@flow(flow_run_name="Repair EggNOG TSV headers")
def repair_eggnog_tsv_headers(
    analysis_accessions: list[str] | None = None,
    dry_run: bool = False,
    sync_to_ftp: bool = False,
) -> list[EggnogHeaderFixCandidate]:
    """List, repair, and optionally resync EggNOG TSV files with repeated headers.

    :param analysis_accessions: Optional accession filter.
    :param dry_run: When True, only list candidates.
    :param sync_to_ftp: When True, sync repaired files to the mirror.
    """
    logger = get_run_logger()

    candidates = collect_eggnog_header_fix_candidates(analysis_accessions)

    if not candidates:
        logger.info("No EggNOG files need repairing.")
        return []

    if dry_run:
        logger.info(
            f"dry_run=True - listing {len(candidates)} EggNOG file(s) without changes."
        )
        return candidates

    logger.info(f"Repairing {len(candidates)} EggNOG file(s) on NFS.")
    repaired = repair_eggnog_header_files(candidates)

    if not sync_to_ftp:
        logger.info("sync_to_ftp=False - Flow complete after NFS repair.")
        return repaired

    if not repaired:
        logger.info("No repaired analyses to sync to FTP.")
        return []

    resync_eggnog_results_to_ftp(repaired)
    return repaired
