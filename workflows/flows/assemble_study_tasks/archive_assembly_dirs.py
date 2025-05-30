from pathlib import Path
from shutil import rmtree

from django.db.models import QuerySet
from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG
from analyses.models import Study, Assembly


@flow(
    name="Archive assembly directories",
    flow_run_name="Archive assembly directories of {reads_study_mgys}",
)
def archive_assembly_dirs(reads_study_mgys: str, dry_run: bool = True):
    """
    Currently this deletes miassembler workdirs provided the study is in a suitable state of uploaded/inactive assemblies.
    In future, it might move assembly dirs to LTS or a results archive if needed.
    """
    logger = get_run_logger()

    study = Study.objects.get(accession=reads_study_mgys)
    logger.info(f"Study is {study}")

    assemblies_started = study.assemblies_reads.filter_by_statuses(
        [Assembly.AssemblyStates.ASSEMBLY_STARTED]
    )
    logger.info(
        f"Study has {assemblies_started.count()} reads-assemblies in STARTED state"
    )

    started_not_finished: QuerySet = assemblies_started.exclude_by_statuses(
        [
            Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
            Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
            Assembly.AssemblyStates.ASSEMBLY_FAILED,
            Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
            Assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED,
        ]
    )

    if started_not_finished.exists():
        logger.warning(
            f"{started_not_finished.count()} of the assemblies are not in any terminal state."
        )
        logger.warning(f"**Not** cleaning {study}")
        return

    # Assembly workdir roots could be in any ena-accession prefixed dir, with or without samplesheet hash
    # e.g. one of
    # /nfs/.../slurm_workdir/ERP123_miassembler/
    # /nfs/.../slurm_workdir/PRJ123_miassembler/
    # /nfs/.../slurm_workdir/ERP123_miassembler/abcdef123456
    # /nfs/.../slurm_workdir/PRJ123_miassembler/abcdef123456
    # Look directly, since we may be cleaning folders that have not appeared in an assembly .dir field (if all FAILED)

    potential_workdirs_roots = {
        Path(EMG_CONFIG.slurm.default_workdir)
        / f"{study.ena_study.accession}_miassembler",
    }.union(
        {
            Path(EMG_CONFIG.slurm.default_workdir) / f"{acc}_miassembler"
            for acc in study.ena_accessions
        }
    )

    for potential_workdirs_root in potential_workdirs_roots:
        if not potential_workdirs_root.is_dir():
            logger.info(
                f"No directory found at {potential_workdirs_root}. Nothing to be done."
            )
            continue
        logger.info(f"Looking for workdirs under {potential_workdirs_root}")

        if (workdir := potential_workdirs_root / "work").is_dir():
            logger.warning(f"Found workdir {workdir}. Deleting it.")
            if dry_run:
                logger.info("No action since in dry_run mode.")
            else:
                rmtree(workdir)

        samplesheet_workdirs = potential_workdirs_root.glob("**/work")
        for workdir in samplesheet_workdirs:
            if workdir.is_dir():
                logger.warning(f"Found workdir {workdir}. Deleting it.")
                if dry_run:
                    logger.info("No action since in dry_run mode.")
                else:
                    rmtree(workdir)
