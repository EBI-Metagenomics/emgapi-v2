from __future__ import annotations

from typing import Any

from django.db import connection, transaction
from prefect import get_run_logger
from prefect.artifacts import create_table_artifact
from pydantic import BaseModel, Field

from activate_django_first import EMG_CONFIG  # noqa: F401

from analyses.models import (
    Analysis,
    Assembly,
    AssemblyAnalysisRequest,
    Run,
    Sample,
    Study,
    StudyPublication,
    SuperStudyStudy,
)
from workflows.models import AssemblyAnalysisBatch
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task


def find_duplicated_studies(
    accessions: list[str] | None = None,
) -> list[list[Study]]:
    """
    Load duplicate Study groups by exact `ena_accessions` set.

    :param accessions: Optional MGYS or ENA accessions used to limit which groups are returned.
    :return: Duplicate groups whose sorted accession arrays are identical.
    """
    groups: list[list[Study]] = []
    requested = set(accessions or [])

    with connection.cursor() as cursor:
        cursor.execute("""
            WITH RECURSIVE
            -- 1. Expand each study row into one row per accession alias.
            study_accessions AS (
                SELECT
                    s.id AS study_id,
                    unnest(s.ena_accessions) AS accession
                FROM analyses_study s
            ),
            -- 2. Build graph edges between studies that share any accession.
            study_links AS (
                SELECT DISTINCT
                    a.study_id AS source_id,
                    b.study_id AS target_id
                FROM study_accessions a
                JOIN study_accessions b
                    ON a.accession = b.accession
            ),
            -- 3. Recursively walk the overlap graph from each study.
            components AS (
                SELECT
                    source_id AS root_id,
                    source_id AS study_id
                FROM study_links
                UNION
                SELECT
                    c.root_id,
                    l.target_id AS study_id
                FROM components c
                JOIN study_links l
                    ON l.source_id = c.study_id
            ),
            -- 4. Collapse each connected component to a stable component id.
            grouped AS (
                SELECT
                    MIN(root_id) AS component_id,
                    study_id
                FROM components
                GROUP BY study_id
            )
            -- 5. Return one row per duplicate group.
            SELECT
                array_agg(study_id ORDER BY study_id) AS study_ids
            FROM grouped
            GROUP BY component_id
            HAVING COUNT(*) > 1
            """)
        duplicate_groups = [row[0] for row in cursor.fetchall()]

    for study_ids in duplicate_groups:
        group = list(
            Study.objects.select_related("ena_study")
            .filter(id__in=study_ids)
            .order_by("accession")
        )

        if requested and not any(
            item.accession in requested
            or bool(requested.intersection(set(item.ena_accessions)))
            for item in group
        ):
            continue

        groups.append(group)
    return groups


class GroupMergeResult(BaseModel):
    """
    Summary of one duplicate-group merge attempt.

    :param canonical_study: MGYS accession chosen as canonical for the group.
    :param merged_studies: All MGYS accessions are seen in the group.
    """

    canonical_study: str
    merged_studies: list[str]
    deleted_studies: list[str] = Field(default_factory=list)
    canonical_ena_study: str | None = None
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_artifact_row(self) -> dict[str, str]:
        """
        Convert the merge result into one flat row for Prefect artifact output.

        :return: Dictionary suitable for `create_table_artifact`.
        """
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_study": self.canonical_study,
            "merged_studies": ", ".join(self.merged_studies),
            "deleted_studies": ", ".join(self.deleted_studies),
            "canonical_ena_study": self.canonical_ena_study or "",
            "status": self.status,
            "error": self.error or "",
        }


def update_study_accessions(canonical: Study, duplicates: list[Study]) -> None:
    """
    Union ENA accession aliases onto the canonical Study.

    :param canonical: The Study row that will be kept.
    :param duplicates: Study rows that will be merged into the canonical one.
    """
    all_accessions = set(canonical.ena_accessions or [])
    for duplicate in duplicates:
        all_accessions.update(duplicate.ena_accessions or [])

    merged_accessions = sorted(all_accessions)
    if sorted(canonical.ena_accessions or []) != merged_accessions:
        canonical.ena_accessions = merged_accessions
        canonical.save(update_fields=["ena_accessions"])


def merge_study_fields(canonical: Study, duplicates: list[Study]) -> None:
    """
    Preserve study-owned fields from duplicates before deleting them.

    Rules are intentionally simple:
    - keep the canonical value when already set
    - fill empty scalar fields from duplicates
    - merge metadata keys, keeping canonical values on conflict
    - OR boolean feature flags so duplicate-only enabled flags are preserved
    """
    update_fields: set[str] = set()

    for duplicate in duplicates:
        if not canonical.title and duplicate.title:
            canonical.title = duplicate.title
            update_fields.add("title")

        merged_metadata = {
            **(duplicate.metadata or {}),
            **(canonical.metadata or {}),
        }
        if merged_metadata != (canonical.metadata or {}):
            canonical.metadata = merged_metadata
            update_fields.add("metadata")

        if not canonical.results_dir and duplicate.results_dir:
            canonical.results_dir = duplicate.results_dir
            update_fields.add("results_dir")

        if not canonical.external_results_dir and duplicate.external_results_dir:
            canonical.external_results_dir = duplicate.external_results_dir
            update_fields.add("external_results_dir")

        if canonical.biome_id is None and duplicate.biome_id is not None:
            canonical.biome_id = duplicate.biome_id
            update_fields.add("biome")

        canonical_features = canonical.features.model_dump()
        duplicate_features = duplicate.features.model_dump()
        merged_features = {}
        for key in set(canonical_features) | set(duplicate_features):
            canonical_value = canonical_features.get(key)
            duplicate_value = duplicate_features.get(key)
            if isinstance(canonical_value, bool) or isinstance(duplicate_value, bool):
                merged_features[key] = bool(canonical_value) or bool(duplicate_value)
            else:
                merged_features[key] = (
                    canonical_value if canonical_value is not None else duplicate_value
                )
        if merged_features != canonical_features:
            canonical.features = merged_features
            update_fields.add("features")

    if update_fields:
        canonical.save(update_fields=sorted(update_fields))


def rewire_study_relations(canonical_study: Study, duplicates: list[Study]) -> None:
    """
    Reassign direct Study relations from duplicate Studies to the canonical Study.

    :param canonical_study: The Study row that will be kept.
    :param duplicates: Study rows that will be merged into the canonical one.
    """

    # We expect that the repeated studies to be 2 (hence the name), but just-in-case we have a loop
    # in case there are more than 2 duplicates.

    for duplicate in duplicates:
        # -- Runs -- #
        if Run.objects.filter(study=duplicate).exists():
            Run.objects.filter(study=duplicate).update(study=canonical_study)

        # -- Analyses -- #
        if Analysis.objects.filter(study=duplicate).exists():
            Analysis.objects.filter(study=duplicate).update(study=canonical_study)

        # -- Assemblies: reads_study -- #
        if Assembly.objects.filter(reads_study=duplicate).exists():
            Assembly.objects.filter(reads_study=duplicate).update(
                reads_study=canonical_study
            )

        # -- Assemblies: assembly_study -- #
        if Assembly.objects.filter(assembly_study=duplicate).exists():
            Assembly.objects.filter(assembly_study=duplicate).update(
                assembly_study=canonical_study
            )

        # -- Assembly Analysis Requests -- #
        if AssemblyAnalysisRequest.objects.filter(study=duplicate).exists():
            AssemblyAnalysisRequest.objects.filter(study=duplicate).update(
                study=canonical_study
            )

        # -- Assembly Analysis Batches -- #
        if AssemblyAnalysisBatch.objects.filter(study=duplicate).exists():
            AssemblyAnalysisBatch.objects.filter(study=duplicate).update(
                study=canonical_study
            )

        # -- Samples -- #
        sample_study_through = Sample.studies.through
        duplicate_study_samples_ids = list(
            sample_study_through.objects.filter(study_id=duplicate.id).values_list(
                "sample_id", flat=True
            )
        )
        sample_study_through.objects.bulk_create(
            [
                sample_study_through(
                    sample_id=sample_id,
                    study_id=canonical_study.id,
                )
                for sample_id in duplicate_study_samples_ids
            ],
            ignore_conflicts=True,  # Skip in case there is a conflict (samples already linked to the study)
        )
        sample_study_through.objects.filter(study_id=duplicate.id).delete()

        # -- Super Studies -- #
        duplicate_super_study_links = list(
            SuperStudyStudy.objects.filter(study=duplicate)
        )
        canonical_super_study_ids = set(
            SuperStudyStudy.objects.filter(study=canonical_study).values_list(
                "super_study_id", flat=True
            )
        )

        deleted_super_study_links = 0
        for link in duplicate_super_study_links:
            if link.super_study_id in canonical_super_study_ids:
                deleted_super_study_links += 1
                link.delete()
            else:
                link.study = canonical_study
                link.save(update_fields=["study"])

        # -- Publications -- #
        duplicate_publication_links = list(
            StudyPublication.objects.filter(study=duplicate)
        )
        canonical_publication_ids = set(
            StudyPublication.objects.filter(study=canonical_study).values_list(
                "publication_id", flat=True
            )
        )

        deleted_publication_links = 0
        for link in duplicate_publication_links:
            if link.publication_id in canonical_publication_ids:
                deleted_publication_links += 1
                link.delete()
            else:
                link.study = canonical_study
                link.save(update_fields=["study"])


def study_is_fully_detached(study: Study) -> bool:
    """
    Check whether a Study has any remaining direct relations that block deletion.

    :param study: Study row being considered for deletion after rewiring.
    :return: True if no direct tracked relations remain attached.
    """
    return not any(
        [
            study.runs.exists(),
            study.analyses.exists(),
            study.assemblies_reads.exists(),
            study.assemblies_assembly.exists(),
            study.samples.exists(),
            AssemblyAnalysisRequest.objects.filter(study=study).exists(),
            AssemblyAnalysisBatch.objects.filter(study=study).exists(),
            SuperStudyStudy.objects.filter(study=study).exists(),
            StudyPublication.objects.filter(study=study).exists(),
        ]
    )


@task
def merge_duplicate_group(
    studies: list[Study], dry_run: bool = True
) -> GroupMergeResult:
    """
    Merge one duplicate Study group onto a single canonical Study.

    :param studies: Duplicate Study rows in one connected component.
    :param dry_run: If True, calculate merge actions without persisting them.
    :return: Summary of the merge outcome for reporting.
    """
    logger = get_run_logger()

    # Pick the study with the lowest MGYS, as those are likely to have been
    # pulled from the API V1 database and are the ones we need to keep.
    canonical_study = sorted(studies, key=lambda study: study.accession)[0]

    duplicates = [study for study in studies if study.pk != canonical_study.pk]

    grouped_accessions = sorted(
        {accession for study in studies for accession in study.ena_accessions}
    )

    result = GroupMergeResult(
        canonical_study=canonical_study.accession,
        merged_studies=[study.accession for study in studies],
        deleted_studies=[study.accession for study in duplicates],
        grouped_accessions=grouped_accessions,
        status="dry_run" if dry_run else "applied",
    )

    ena_study_accessions = set(study.ena_study.accession for study in studies)
    if len(ena_study_accessions) > 1:
        raise ValueError(
            f"Duplicated study {canonical_study} is connected to different ENA studies: {ena_study_accessions}"
        )

    canonical_ena_study = canonical_study.ena_study
    result.canonical_ena_study = canonical_ena_study.accession

    if dry_run:
        logger.info(
            f"Dry run: Would merge {len(duplicates)} studies into {canonical_study}"
        )
        return result

    with transaction.atomic():

        update_study_accessions(canonical_study, duplicates)
        merge_study_fields(canonical_study, duplicates)

        rewire_study_relations(canonical_study, duplicates)

        deleted_studies: list[str] = []
        for duplicate in duplicates:
            duplicate.refresh_from_db()
            if study_is_fully_detached(duplicate):
                deleted_studies.append(duplicate.accession)
                duplicate.delete()
            else:
                raise ValueError(
                    f"Study {duplicate} is still connected to other objects and cannot be deleted."
                )
        result.deleted_studies = deleted_studies

    return result


@flow(flow_run_name="Deduplicate studies")
def deduplicate_studies(
    accessions: list[str] | None = None,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """
    Deduplicate MGnify Study rows and emit a Prefect artifact summary.

    :param accessions: Optional MGYS or ENA accessions are used to limit processing to those matching this list.
    :param dry_run: If True, report intended changes and roll back each group transaction.
    :return: Flat artifact rows describing every processed duplicate group.
    """
    logger = get_run_logger()
    duplicated_studies = find_duplicated_studies(accessions=accessions)
    logger.info(
        f"Found {len(duplicated_studies)} duplicate studies to process"
        + (" in dry-run mode" if dry_run else "")
    )

    results: list[GroupMergeResult] = []
    for index, studies in enumerate(duplicated_studies, start=1):
        logger.info(
            f"Processing study group {index}/{len(duplicated_studies)}: "
            f"{', '.join(study.accession for study in studies)}"
        )
        try:
            result = merge_duplicate_group(studies, dry_run=dry_run)
            results.append(result)
            logger.info(
                f"Finished group {index}/{len(duplicated_studies)} "
                f"with canonical study {result.canonical_study}"
            )
        except Exception as exc:
            logger.exception(
                f"Failed to deduplicate study group "
                f"{[study.accession for study in studies]}",
                exc_info=exc,
            )

    artifact_rows = [result.as_artifact_row() for result in results]
    create_table_artifact(
        key="study-deduplication-summary",
        table=artifact_rows,
        description="Summary of duplicate study housekeeping actions.",
    )
    return artifact_rows
