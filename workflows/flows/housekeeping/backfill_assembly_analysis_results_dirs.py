from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from prefect import get_run_logger
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG  # noqa: F401

from analyses.models import Analysis
from workflows.models import (
    AssemblyAnalysisBatchAnalysis,
    StudyAnalysisBatch,
)
from workflows.prefect_utils.flows_utils import (
    django_db_flow as flow,
)


class ResultsDirRow(TypedDict, total=False):
    """Row describing one assembly analysis results_dir state."""

    analysis_id: int
    analysis_accession: str
    assembly_accession: str
    batch_count: int
    batch_ids: str
    previous_results_dir: str
    expected_results_dir: str
    modified: bool


def expected_results_dir(
    batch_analysis_relation: AssemblyAnalysisBatchAnalysis,
) -> str:
    """Return the canonical assembly-analysis results_dir for batch-managed runs.

    For the current assembly batching model this is the base batch workspace,
    not a pipeline-specific subdirectory such as ``asa/<assembly_accession>``.
    """
    return str(Path(batch_analysis_relation.batch.workspace_dir))


def collect_results_dir_backfill_report(
    analysis_accessions: list[str] | None = None,
    dry_mode: bool = False,
) -> list[ResultsDirRow]:
    """Backfill blank assembly analysis results_dir values from their batch."""

    logger = get_run_logger()

    batch_analysis_relations = (
        AssemblyAnalysisBatchAnalysis.all_objects.filter(
            analysis__assembly__isnull=False,
            analysis__pipeline_version=Analysis.PipelineVersions.v6,
            batch__batch_type=StudyAnalysisBatch.BatchType.ASSEMBLY_ANALYSIS,
        )
        .select_related("batch", "analysis__assembly")
        .order_by("analysis__accession", "created_at", "id")
    )

    if analysis_accessions is not None:
        batch_analysis_relations = batch_analysis_relations.filter(
            analysis__accession__in=analysis_accessions
        )

    logger.info("Collecting assembly analysis batch relations")

    relations_by_analysis_id: dict[int, list[AssemblyAnalysisBatchAnalysis]] = {}
    for batch_analysis_relation in batch_analysis_relations:
        relations_by_analysis_id.setdefault(
            batch_analysis_relation.analysis_id, []
        ).append(batch_analysis_relation)

    relation_count = sum(
        len(analysis_batch_relations)
        for analysis_batch_relations in relations_by_analysis_id.values()
    )
    logger.info(
        "Collected %s batch relation(s) for %s analysis/analyses",
        relation_count,
        len(relations_by_analysis_id),
    )

    analyses_to_update: list[Analysis] = []
    updated_rows: list[ResultsDirRow] = []
    results_dir_to_backfill_count = 0
    already_set_count = 0
    multiple_batch_count = 0

    for analysis_batch_relations in relations_by_analysis_id.values():
        analysis = analysis_batch_relations[0].analysis
        row: ResultsDirRow = {
            "analysis_id": analysis.id,
            "analysis_accession": analysis.accession,
            "assembly_accession": analysis.assembly.first_accession or "",
            "batch_count": len(analysis_batch_relations),
            "batch_ids": ",".join(
                str(relation.batch_id) for relation in analysis_batch_relations
            ),
            "previous_results_dir": analysis.results_dir or "",
        }

        if len(analysis_batch_relations) > 1:
            # This really shouldn't happen, but I need to know if it does.
            multiple_batch_count += 1
            logger.warning(
                "Analysis %s is linked to %s assembly analysis batches; skipping update",
                analysis.accession,
                len(analysis_batch_relations),
            )
            row["expected_results_dir"] = ""
            row["modified"] = False
            updated_rows.append(row)
            continue

        batch_analysis_relation = analysis_batch_relations[0]
        new_results_dir = expected_results_dir(batch_analysis_relation)
        if analysis.results_dir:
            already_set_count += 1
            row["expected_results_dir"] = new_results_dir
            row["modified"] = False
            updated_rows.append(row)
            continue

        row["expected_results_dir"] = new_results_dir
        row["modified"] = True
        updated_rows.append(row)
        results_dir_to_backfill_count += 1

        if not dry_mode:
            analysis.results_dir = new_results_dir
            analyses_to_update.append(analysis)

    if dry_mode:
        logger.info(
            "Dry run complete: %s would be updated, %s already set, %s multiple-batch",
            results_dir_to_backfill_count,
            already_set_count,
            multiple_batch_count,
        )
        return updated_rows

    if analyses_to_update:
        logger.info(
            "Backfilling results_dir for %s analysis/analyses",
            len(analyses_to_update),
        )
        Analysis.objects.bulk_update(analyses_to_update, ["results_dir"])

    logger.info(
        "Results_dir backfill complete: %s updated, %s already set, %s multiple-batch",
        len(analyses_to_update),
        already_set_count,
        multiple_batch_count,
    )
    return updated_rows


@flow(name="Backfill assembly analysis results_dir")
def backfill_assembly_analysis_results_dirs(
    analysis_accessions: list[str] | None = None,
    dry_mode: bool = False,
) -> list[ResultsDirRow]:
    """One-off housekeeping flow to backfill assembly analysis results_dir values."""
    logger = get_run_logger()

    logger.info("Starting assembly analysis results_dir backfill")
    updated_rows = collect_results_dir_backfill_report(
        analysis_accessions, dry_mode=dry_mode
    )

    create_table_artifact(
        key="assembly-analysis-results-dir-backfill-updated",
        table=updated_rows,
        description=(
            "Assembly analyses reviewed for results_dir backfill from their "
            f"assembly batch ({len(updated_rows)} analysis/analyses)"
        ),
    )

    logger.info(f"Backfilled results_dir for {len(updated_rows)} analyses")

    return updated_rows
