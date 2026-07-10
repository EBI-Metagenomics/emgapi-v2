from django.db.models import QuerySet
from prefect import get_run_logger
from prefect.artifacts import create_table_artifact
from sqlalchemy import select

import activate_django_first  # noqa

from analyses.models import Analysis, Assembly
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacyAnalysisJob,
    legacy_emg_db_session,
)
from workflows.flows.legacy.tasks.make_assembly_from_legacy_emg_db import (
    make_assembly_from_legacy_emg_db,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow


def _assembly_analyses_queryset(
    mgys: str | None = None,
    mgys_gte: str | None = None,
    mgys_lte: str | None = None,
) -> QuerySet[Analysis]:
    analyses = (
        Analysis.objects.filter(assembly__isnull=False)
        .select_related("study", "sample", "assembly", "study__ena_study")
        .order_by("study__accession", "accession")
    )

    if mgys:
        analyses = analyses.filter(study__accession=mgys)
    if mgys_gte:
        analyses = analyses.filter(study__accession__gte=mgys_gte)
    if mgys_lte:
        analyses = analyses.filter(study__accession__lte=mgys_lte)

    return analyses


def _get_unique_assembly_by_ena_accession(accession: str) -> Assembly | None:
    assemblies = Assembly.objects.filter(ena_accessions__contains=[accession])
    if assemblies.count() > 1:
        raise Assembly.MultipleObjectsReturned(
            f"Multiple assemblies found with accession {accession}"
        )
    return assemblies.first()


@flow(
    name="Relink legacy assembly analyses",
    flow_run_name="Relink legacy assembly analyses",
)
def relink_legacy_assembly_analyses(
    mgys: str | None = None,
    mgys_gte: str | None = None,
    mgys_lte: str | None = None,
    dry_run: bool = True,
) -> list[dict]:
    """
    Repoint imported legacy assembly analyses to the Assembly matching their legacy ERZ.

    The legacy importer stores MGYA numeric IDs from legacy ANALYSIS_JOB.JOB_ID, so this
    flow uses each analysis id to fetch the legacy job and its ASSEMBLY.ACCESSION.
    """
    logger = get_run_logger()
    analyses = list(_assembly_analyses_queryset(mgys, mgys_gte, mgys_lte))
    changes = []

    logger.info(f"Checking {len(analyses)} assembly analyses")

    with legacy_emg_db_session() as session:
        for analysis in analyses:
            legacy_analysis = session.scalar(
                select(LegacyAnalysisJob).where(LegacyAnalysisJob.job_id == analysis.id)
            )
            if not legacy_analysis:
                logger.warning(f"No legacy analysis found for {analysis.accession}")
                continue
            if not legacy_analysis.assembly:
                logger.warning(
                    f"Legacy analysis {analysis.accession} has no linked assembly"
                )
                continue

            legacy_assembly = legacy_analysis.assembly
            legacy_accession = legacy_assembly.accession
            current_assembly = analysis.assembly
            current_accessions = current_assembly.ena_accessions or []

            if legacy_accession in current_accessions:
                logger.info(
                    f"{analysis.accession} already points to assembly with {legacy_accession}"
                )
                continue

            target_assembly = _get_unique_assembly_by_ena_accession(legacy_accession)
            change = {
                "analysis": analysis.accession,
                "study": analysis.study.accession,
                "legacy_assembly_accession": legacy_accession,
                "current_assembly_id": current_assembly.id,
                "current_assembly_accessions": ", ".join(current_accessions),
                "target_assembly_id": target_assembly.id if target_assembly else None,
                "action": "would_update" if dry_run else "updated",
            }

            if dry_run:
                change["action"] = (
                    "would_update_to_existing_assembly"
                    if target_assembly
                    else "would_create_assembly_and_update"
                )
                logger.info(change)
                changes.append(change)
                continue

            target_assembly = make_assembly_from_legacy_emg_db(
                legacy_assembly,
                analysis.study,
                analysis.sample,
            )
            analysis.assembly = target_assembly
            analysis.save(update_fields=["assembly"])
            change["target_assembly_id"] = target_assembly.id
            logger.info(change)
            changes.append(change)

    if changes:
        create_table_artifact(
            key="legacy-assembly-analysis-relinking",
            table=changes,
            description=(
                "Legacy assembly analysis links that would change"
                if dry_run
                else "Legacy assembly analysis links changed"
            ),
        )

    logger.info(
        f"{len(changes)} analyses need relinking"
        if dry_run
        else f"Relinked {len(changes)} analyses"
    )
    return changes
