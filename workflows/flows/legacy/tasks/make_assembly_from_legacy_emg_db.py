from prefect import task, get_run_logger
from sqlalchemy import select

from analyses.models import Study, Sample, Assembly
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyAssembly,
    LegacySample,
    LegacyAssemblySample,
    LegacyRun,
    LegacyAssemblyRun,
)
from workflows.ena_utils.ena_api_requests import sync_sample_metadata_from_ena
from workflows.flows.legacy.tasks.make_run_from_legacy_emg_db import (
    make_run_from_legacy_emg_db,
)
from workflows.flows.legacy.tasks.make_sample_from_legacy_emg_db import (
    make_sample_from_legacy_emg_db,
)


@task
def make_assembly_from_legacy_emg_db(
    legacy_assembly: LegacyAssembly,
    study: Study,
    sample: Sample,
) -> Assembly | None:
    from workflows.data_io_utils.legacy_emg_dbs import legacy_emg_db_session

    logger = get_run_logger()

    with legacy_emg_db_session() as session:
        # In this new schema, Assembly has a sample field and runs ManyToMany.
        # A co-assembly might have multiple runs and samples.
        # For now we use the analysis job's primary sample for the assembly.
        assembly, created = Assembly.objects.get_or_create(
            ena_study=study.ena_study,
            assembly_study=study,  # could also be reads study, but unclear from information present for legacy cases
            sample=sample,
            defaults={
                "ena_accessions": [legacy_assembly.accession],
            },
        )
        if created:
            logger.info(f"Created new Assembly object {assembly}")

        # Ensure all samples linked to this assembly exist in the new DB
        # before creating the runs.
        legacy_samples_stmt = (
            select(LegacySample)
            .join(
                LegacyAssemblySample,
                LegacySample.sample_id == LegacyAssemblySample.sample_id,
            )
            .where(LegacyAssemblySample.assembly_id == legacy_assembly.assembly_id)
        )
        legacy_samples = session.scalars(legacy_samples_stmt).unique().all()
        for leg_sample in legacy_samples:
            s = make_sample_from_legacy_emg_db(leg_sample, study)
            sync_sample_metadata_from_ena(s.ena_sample)

        # Link all runs associated with this legacy assembly
        legacy_runs_stmt = (
            select(LegacyRun)
            .join(
                LegacyAssemblyRun,
                LegacyRun.run_id == LegacyAssemblyRun.run_id,
            )
            .where(LegacyAssemblyRun.assembly_id == legacy_assembly.assembly_id)
        )
        legacy_runs = session.scalars(legacy_runs_stmt).unique().all()
        for leg_run in legacy_runs:
            r = make_run_from_legacy_emg_db(leg_run, study)
            assembly.runs.add(r)

    return assembly
