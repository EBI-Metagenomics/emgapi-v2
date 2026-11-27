"""Populate molecule search from public ASAv6 results on the FTP filesystem."""

from pathlib import Path
from uuid import uuid4

from prefect import get_run_logger
from prefect.deployments import run_deployment

from activate_django_first import EMG_CONFIG  # noqa

from analyses.models import Analysis
from analyses.utils.export_rhea_index import relative_rhea_path
from workflows.flows.analysis.assembly.tasks.import_rhea import import_rhea_annotations
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.flows_utils import django_db_flow as flow


@flow
def initialise_molecule_index(
    workdir: Path,
    study_accession: str | None = None,
    overwrite: bool = False,
    timeout: int = 14400,
) -> None:
    """Copy Rhea files in study batches, then import completed public analyses.

    :param workdir: Worker-visible directory under a configured shared filesystem root.
    :param study_accession: Optional MGYS restriction for a trial import.
    :param overwrite: Replace existing molecule sets when true.
    :param timeout: Maximum seconds for each study's datamover deployment.
    :raises RuntimeError: If any copy or analysis import fails.
    :raises ValueError: If the workdir is outside the configured shared roots.
    """
    logger = get_run_logger()
    workdir = Path(workdir)
    server_root = Path(EMG_CONFIG.slurm.shared_filesystem_root_on_server)
    cluster_root = Path(EMG_CONFIG.slurm.shared_filesystem_root_on_slurm)
    if ".." in workdir.parts or not workdir.is_absolute():
        raise ValueError("workdir must be an absolute shared filesystem path")
    if workdir.is_relative_to(server_root):
        cluster_workdir = cluster_root / workdir.relative_to(server_root)
    elif workdir.is_relative_to(cluster_root):
        cluster_workdir = workdir
    else:
        raise ValueError("workdir must be under a configured shared filesystem root")
    workdir.mkdir(parents=True, exist_ok=True)
    analyses = Analysis.public_objects.filter(
        assembly__isnull=False,
        pipeline_version__startswith="V6",
        study__is_private=False,
        study__is_suppressed=False,
    )
    if study_accession:
        analyses = analyses.filter(study__accession=study_accession)
    if not overwrite:
        analyses = analyses.filter(molecule_index__isnull=True)
    studies = list(
        analyses.order_by("study_id").values_list("study_id", flat=True).distinct()
    )
    failures = []
    for study_id in studies:
        entries = []
        for analysis in analyses.filter(study_id=study_id).order_by("id").iterator():
            try:
                downloads = [
                    d
                    for d in analysis.downloads_as_objects
                    if d.download_group == "functional_annotation.rhea_reactions"
                ]
                if len(downloads) != 1 or not analysis.external_results_dir:
                    raise ValueError("Expected one published Rhea download")
                relative = relative_rhea_path(
                    str(Path(analysis.external_results_dir) / downloads[0].path)
                )
                entries.append((analysis.id, analysis.accession, relative))
            except Exception:
                logger.exception("Invalid Rhea download for %s", analysis.accession)
                failures.append(analysis.accession)
        if not entries:
            continue
        target = workdir / str(study_id)
        cluster_target = cluster_workdir / str(study_id)
        target.mkdir(exist_ok=True)
        try:
            manifest = workdir / f"rhea-{uuid4().hex}.files"
            manifest.write_text(
                "\n".join(sorted({path for _, _, path in entries})) + "\n"
            )
            cluster_manifest = cluster_workdir / manifest.name
            result = run_deployment(
                name="move-data/move_data_deployment",
                parameters={
                    "move_command": cli_command(
                        [
                            "rsync",
                            "-rt",
                            f"--files-from={cluster_manifest}",
                            "--",
                        ]
                    ),
                    "source": str(Path(EMG_CONFIG.slurm.ftp_results_dir)) + "/",
                    "target": str(cluster_target) + "/",
                    "make_target": False,
                },
                job_variables={"partition": EMG_CONFIG.slurm.datamover_partition},
                timeout=timeout,
            )
            if not result.state or not result.state.is_completed():
                raise RuntimeError(f"Datamover did not complete: {result.id}")
            # A timed-out deployment can still need its files list.
            manifest.unlink()
        except Exception:
            logger.exception("Rhea copy failed for study %s", study_id)
            failures.extend(accession for _, accession, _ in entries)
            continue
        for analysis_id, accession, relative in entries:
            try:
                count = import_rhea_annotations(analysis_id, target / relative)
                logger.info("Indexed %s ChEBIs for %s", count, accession)
            except Exception:
                logger.exception("Rhea import failed for %s", accession)
                failures.append(accession)
    if failures:
        raise RuntimeError(f"Rhea imports failed: {', '.join(failures)}")
