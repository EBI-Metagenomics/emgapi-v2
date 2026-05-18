from __future__ import annotations

from typing import Any

from django.db import connection, transaction
from prefect import get_run_logger
from prefect.artifacts import create_table_artifact
from pydantic import BaseModel, Field

from activate_django_first import EMG_CONFIG  # noqa: F401

from analyses.models import Analysis, Assembly, Run
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


def find_duplicated_runs(
    accessions: list[str] | None = None,
) -> list[list[Run]]:
    """
    Load duplicate Run groups by exact ``ena_accessions`` set.

    :param accessions: Optional ENA accessions used to limit which groups are returned.
    :return: Duplicate groups whose sorted accession arrays are identical.
    """
    groups: list[list[Run]] = []
    requested = set(accessions or [])

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE
            -- 1. Expand each run row into one row per accession alias.
            run_accessions AS (
                SELECT
                    r.id AS run_id,
                    unnest(r.ena_accessions) AS accession
                FROM analyses_run r
            ),
            -- 2. Build graph edges between runs that share any accession.
            run_links AS (
                SELECT DISTINCT
                    a.run_id AS source_id,
                    b.run_id AS target_id
                FROM run_accessions a
                JOIN run_accessions b
                    ON a.accession = b.accession
            ),
            -- 3. Recursively walk the overlap graph from each run.
            components AS (
                SELECT
                    source_id AS root_id,
                    source_id AS run_id
                FROM run_links
                UNION
                SELECT
                    c.root_id,
                    l.target_id AS run_id
                FROM components c
                JOIN run_links l
                    ON l.source_id = c.run_id
            ),
            -- 4. Collapse each connected component to a stable component id.
            grouped AS (
                SELECT
                    MIN(root_id) AS component_id,
                    run_id
                FROM components
                GROUP BY run_id
            )
            -- 5. Return one row per duplicate group.
            SELECT
                array_agg(run_id ORDER BY run_id) AS run_ids
            FROM grouped
            GROUP BY component_id
            HAVING COUNT(*) > 1
            """)
        duplicate_groups = [row[0] for row in cursor.fetchall()]

    for run_ids in duplicate_groups:
        group = list(
            Run.objects.select_related("study", "sample", "ena_study")
            .filter(id__in=run_ids)
            .order_by("id")
        )

        if requested and not any(
            bool(requested.intersection(set(run.ena_accessions))) for run in group
        ):
            continue

        groups.append(group)
    return groups


class GroupMergeResult(BaseModel):
    """Summary of one duplicate-run merge attempt."""

    canonical_run: str
    merged_runs: list[str]
    deleted_runs: list[str] = Field(default_factory=list)
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_artifact_row(self) -> dict[str, str]:
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_run": self.canonical_run,
            "merged_runs": ", ".join(self.merged_runs),
            "deleted_runs": ", ".join(self.deleted_runs),
            "status": self.status,
            "error": self.error or "",
        }


def update_run_accessions(canonical: Run, duplicates: list[Run]) -> None:
    """Union ENA accession aliases onto the canonical Run."""
    all_accessions = set(canonical.ena_accessions or [])
    for duplicate in duplicates:
        all_accessions.update(duplicate.ena_accessions or [])

    merged_accessions = sorted(all_accessions)
    if sorted(canonical.ena_accessions or []) != merged_accessions:
        canonical.ena_accessions = merged_accessions
        canonical.save(update_fields=["ena_accessions"])


def rewire_run_relations(canonical_run: Run, duplicates: list[Run]) -> None:
    """Reassign direct Run relations from duplicate Runs to the canonical Run."""
    assembly_run_through = Assembly.runs.through

    for duplicate in duplicates:
        # -- Analyses -- #
        if Analysis.objects.filter(run=duplicate).exists():
            Analysis.objects.filter(run=duplicate).update(run=canonical_run)

        # -- Assemblies -- #
        duplicate_assembly_ids = list(
            assembly_run_through.objects.filter(run_id=duplicate.id).values_list(
                "assembly_id", flat=True
            )
        )
        assembly_run_through.objects.bulk_create(
            [
                assembly_run_through(
                    assembly_id=assembly_id,
                    run_id=canonical_run.id,
                )
                for assembly_id in duplicate_assembly_ids
            ],
            ignore_conflicts=True,
        )
        assembly_run_through.objects.filter(run_id=duplicate.id).delete()


def run_is_fully_detached(run: Run) -> bool:
    """Check whether a Run has any remaining direct relations that block deletion."""
    return not any(
        [
            run.analyses.exists(),
            run.assemblies.exists(),
        ]
    )


@task
def merge_duplicate_group(runs: list[Run], dry_run: bool = True) -> GroupMergeResult:
    """Merge one duplicate Run group onto a single canonical Run."""
    logger = get_run_logger()

    canonical_run = sorted(runs, key=lambda run: run.id)[0]
    duplicates = [run for run in runs if run.pk != canonical_run.pk]

    grouped_accessions = sorted(
        {accession for run in runs for accession in run.ena_accessions}
    )

    result = GroupMergeResult(
        canonical_run=canonical_run.first_accession or str(canonical_run.id),
        merged_runs=[run.first_accession or str(run.id) for run in runs],
        deleted_runs=[run.first_accession or str(run.id) for run in duplicates],
        grouped_accessions=grouped_accessions,
        status="dry_run" if dry_run else "applied",
    )

    if dry_run:
        logger.info(f"Dry run: Would merge {len(duplicates)} runs into {canonical_run}")
        return result

    with transaction.atomic():
        update_run_accessions(canonical_run, duplicates)
        rewire_run_relations(canonical_run, duplicates)

        deleted_runs: list[str] = []
        for duplicate in duplicates:
            duplicate.refresh_from_db()
            if run_is_fully_detached(duplicate):
                deleted_runs.append(duplicate.first_accession or str(duplicate.id))
                duplicate.delete()
            else:
                raise ValueError(
                    f"Run {duplicate} is still connected to other objects and cannot be deleted."
                )
        result.deleted_runs = deleted_runs

    return result


@flow(flow_run_name="Deduplicate runs")
def deduplicate_runs(
    accessions: list[str] | None = None,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """
    Deduplicate MGnify Run rows and emit a Prefect artifact summary.

    :param accessions: Optional ENA accessions used to limit processing to matching groups.
    :param dry_run: If True, report intended changes without persisting them.
    :return: Flat artifact rows describing every processed duplicate group.
    """
    logger = get_run_logger()
    duplicated_runs = find_duplicated_runs(accessions=accessions)
    logger.info(
        f"Found {len(duplicated_runs)} duplicate runs to process"
        + (" in dry-run mode" if dry_run else "")
    )

    results: list[GroupMergeResult] = []
    for index, runs in enumerate(duplicated_runs, start=1):
        logger.info(
            f"Processing run group {index}/{len(duplicated_runs)}: "
            f"{', '.join(run.first_accession or str(run.id) for run in runs)}"
        )
        try:
            result = merge_duplicate_group(runs, dry_run=dry_run)
            results.append(result)
            logger.info(
                f"Finished group {index}/{len(duplicated_runs)} "
                f"with canonical run {result.canonical_run}"
            )
        except Exception as exc:
            logger.exception(
                f"Failed to deduplicate run group {[run.id for run in runs]}",
                exc_info=exc,
            )

    artifact_rows = [result.as_artifact_row() for result in results]
    create_table_artifact(
        key="run-deduplication-summary",
        table=artifact_rows,
        description="Summary of duplicate run housekeeping actions.",
    )
    return artifact_rows
