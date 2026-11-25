from django.db.models import QuerySet
from prefect import flow

import analyses.models
from workflows.flows.upload_assembly import (
    release_tpa_study_if_complete,
    upload_assembly,
)


@flow(
    log_prints=True,
    flow_run_name="Upload assemblies of: {study}",
)
def upload_assemblies(study: analyses.models.Study, dry_run: bool = False):
    """
    Uploads all completed, not-previously-uploaded assemblies to ENA.
    The first assembly upload will usually trigger a TPA study to be created,
    which is publicly released once all assemblies are uploaded.
    """
    assemblies_to_upload: QuerySet = study.assemblies_reads.filter_by_statuses(
        [analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED]
    ).exclude_by_statuses(
        [
            analyses.models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
            analyses.models.Assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED,
        ]
    )
    print(f"Will upload assemblies: {assemblies_to_upload.count()}")
    for assembly in assemblies_to_upload:
        upload_assembly(assembly.id, dry_run=dry_run)

    release_tpa_study_if_complete(study, dry_run=dry_run)
