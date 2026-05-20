from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from pydantic import BaseModel, Field

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


class GroupMergeResult(BaseModel):
    """
    Summary of one duplicate-group merge attempt.

    :param canonical_study: MGYS accession chosen as canonical for the group.
    :param merged_studies: All MGYS accessions seen in the group.
    """

    canonical_study: str
    merged_studies: list[str]
    deleted_studies: list[str] = Field(default_factory=list)
    canonical_ena_study: str | None = None
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_csv_row(self) -> dict[str, str]:
        """Convert the merge result into one flat CSV row."""
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_study": self.canonical_study,
            "merged_studies": ", ".join(self.merged_studies),
            "deleted_studies": ", ".join(self.deleted_studies),
            "canonical_ena_study": self.canonical_ena_study or "",
            "status": self.status,
            "error": self.error or "",
        }


class Command(BaseCommand):
    help = (
        "Deduplicate MGnify studies that share ENA accessions and write a CSV summary."
    )

    @staticmethod
    def known_ena_accessions(study: Study) -> set[str]:
        """Collect known ENA aliases from the MGnify Study and linked ENA Study."""
        accessions = set(study.ena_accessions or [])
        if study.ena_study:
            accessions.add(study.ena_study.accession)
            accessions.update(study.ena_study.additional_accessions or [])
        return {accession for accession in accessions if accession}

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--accession",
            action="append",
            dest="accessions",
            help="Limit processing to groups matching this MGYS or ENA accession. Repeatable.",
        )
        parser.add_argument(
            "--output-csv",
            default="study_deduplication_summary.csv",
            help="Path to the CSV report to write.",
        )

    def find_duplicated_studies(
        self,
        accessions: list[str] | None = None,
    ) -> list[list[Study]]:
        """
        Load duplicate Study groups by overlapping ``ena_accessions`` values.

        :param accessions: Optional MGYS or ENA accessions used to limit which groups are returned.
        :return: Duplicate groups whose accession aliases place them in the same connected component.
        """
        groups: list[list[Study]] = []
        requested = set(accessions or [])

        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE
                study_accessions AS (
                    SELECT
                        s.id AS study_id,
                        unnest(s.ena_accessions) AS accession
                    FROM analyses_study s
                    WHERE s.ena_accessions IS NOT NULL
                        AND cardinality(s.ena_accessions) > 0
                ),
                study_links AS (
                    SELECT DISTINCT
                        a.study_id AS source_id,
                        b.study_id AS target_id
                    FROM study_accessions a
                    JOIN study_accessions b
                        ON a.accession = b.accession
                ),
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
                grouped AS (
                    SELECT
                        MIN(root_id) AS component_id,
                        study_id
                    FROM components
                    GROUP BY study_id
                )
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

    def update_study_accessions(
        self, canonical: Study, duplicates: list[Study]
    ) -> None:
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

    def merge_study_fields(self, canonical: Study, duplicates: list[Study]) -> None:
        """
        Preserve study-owned fields from duplicates before deleting them.

        Rules are intentionally simple:
        - keep the canonical value when already set
        - fill empty scalar fields from duplicates
        - merge metadata keys, keeping canonical values on conflict
        - OR boolean feature flags so duplicate-only enabled flags are preserved

        :param canonical: The Study row that will be kept.
        :param duplicates: Study rows that will be merged into the canonical one.
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
                if isinstance(canonical_value, bool) or isinstance(
                    duplicate_value, bool
                ):
                    merged_features[key] = bool(canonical_value) or bool(
                        duplicate_value
                    )
                else:
                    merged_features[key] = (
                        canonical_value
                        if canonical_value is not None
                        else duplicate_value
                    )
            if merged_features != canonical_features:
                canonical.features = merged_features
                update_fields.add("features")

        if update_fields:
            canonical.save(update_fields=sorted(update_fields))

    def rewire_study_relations(
        self, canonical_study: Study, duplicates: list[Study]
    ) -> None:
        """
        Reassign direct Study relations from duplicate Studies to the canonical Study.

        :param canonical_study: The Study row that will be kept.
        :param duplicates: Study rows that will be merged into the canonical one.
        """
        # We expect that the repeated studies to be 2 (hence the name), but just-in-case we have a loop
        # in case there are more than 2 duplicates.
        for duplicate in duplicates:
            # -- Runs -- #
            canonical_runs = canonical_study.runs.all()
            duplicate_runs = duplicate.runs.all()

            canonical_run_accessions = set(
                accession for run in canonical_runs for accession in run.ena_accessions
            )
            duplicate_run_accessions = set(
                accession for run in duplicate_runs for accession in run.ena_accessions
            )
            overlapping_accessions = canonical_run_accessions & duplicate_run_accessions

            if overlapping_accessions:
                canonical_study.runs.filter(
                    ena_accessions__overlap=list(overlapping_accessions)
                ).delete()

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
                ignore_conflicts=True,
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

    def study_is_fully_detached(self, study: Study) -> bool:
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

    def merge_duplicate_group(
        self,
        studies: list[Study],
        dry_run: bool = True,
    ) -> GroupMergeResult:
        """
        Merge one duplicate Study group onto a single canonical Study.

        :param studies: Duplicate Study rows in one connected component.
        :param dry_run: If True, calculate merge actions without persisting them.
        :return: Summary of the merge outcome for reporting.
        """
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

        canonical_ena_accessions = self.known_ena_accessions(canonical_study)
        different_ena_studies = {}
        for study in studies:
            study_ena_accessions = self.known_ena_accessions(study)
            if not canonical_ena_accessions.intersection(study_ena_accessions):
                different_ena_studies[study.accession] = sorted(study_ena_accessions)

        if different_ena_studies:
            raise ValueError(
                f"Duplicated study {canonical_study} is connected to different ENA studies: "
                f"canonical={sorted(canonical_ena_accessions)}, duplicates={different_ena_studies}"
            )

        canonical_ena_study = canonical_study.ena_study
        result.canonical_ena_study = (
            canonical_ena_study.accession if canonical_ena_study else None
        )

        if dry_run:
            self.stdout.write(
                f"Dry run: would merge {len(duplicates)} studies into {canonical_study}"
            )
            return result

        with transaction.atomic():
            self.update_study_accessions(canonical_study, duplicates)
            self.merge_study_fields(canonical_study, duplicates)
            self.rewire_study_relations(canonical_study, duplicates)

            deleted_studies: list[str] = []
            for duplicate in duplicates:
                duplicate.refresh_from_db()
                if self.study_is_fully_detached(duplicate):
                    deleted_studies.append(duplicate.accession)
                    duplicate.delete()
                else:
                    raise ValueError(
                        f"Study {duplicate} is still connected to other objects and cannot be deleted."
                    )
            result.deleted_studies = deleted_studies

        return result

    def write_results_csv(self, rows: list[dict[str, str]], output_path: Path) -> Path:
        """
        Persist deduplication results as a CSV report.

        :param rows: Flat rows describing each processed duplicate group.
        :param output_path: Destination CSV path.
        :return: The written output path.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "grouped_accessions",
            "canonical_study",
            "merged_studies",
            "deleted_studies",
            "canonical_ena_study",
            "status",
            "error",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return output_path

    def deduplicate_studies(
        self,
        accessions: list[str] | None = None,
        dry_run: bool = True,
        output_csv: str | Path | None = None,
    ) -> list[dict[str, str]]:
        """
        Deduplicate MGnify Study rows and emit a summary.

        :param accessions: Optional MGYS or ENA accessions used to limit processing to matching groups.
        :param dry_run: If True, report intended changes and roll back each group transaction.
        :param output_csv: Destination for the CSV summary report.
        :return: Flat rows describing every processed duplicate group.
        """
        duplicated_studies = self.find_duplicated_studies(accessions=accessions)
        self.stdout.write(
            f"Found {len(duplicated_studies)} duplicate studies to process"
            + (" in dry-run mode" if dry_run else "")
        )

        results: list[GroupMergeResult] = []
        for index, studies in enumerate(duplicated_studies, start=1):
            self.stdout.write(
                f"Processing study group {index}/{len(duplicated_studies)}: "
                f"{', '.join(study.accession for study in studies)}"
            )
            try:
                result = self.merge_duplicate_group(studies, dry_run=dry_run)
            except Exception as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to deduplicate study group {[study.accession for study in studies]}: {exc}"
                    )
                )
                results.append(
                    GroupMergeResult(
                        canonical_study=studies[0].accession,
                        merged_studies=[study.accession for study in studies],
                        grouped_accessions=sorted(
                            {
                                accession
                                for study in studies
                                for accession in study.ena_accessions
                            }
                        ),
                        canonical_ena_study=studies[0].ena_study.accession,
                        status="failed",
                        error=str(exc),
                    )
                )
                continue

            results.append(result)
            self.stdout.write(
                f"Finished group {index}/{len(duplicated_studies)} "
                f"with canonical study {result.canonical_study}"
            )

        csv_rows = [result.as_csv_row() for result in results]
        output_path = Path(output_csv)
        self.write_results_csv(csv_rows, output_path)
        self.stdout.write(f"Wrote study deduplication summary to {output_path}")
        return csv_rows

    def handle(self, *args, **options):
        self.deduplicate_studies(
            accessions=options["accessions"],
            dry_run=options["dry_run"],
            output_csv=options["output_csv"],
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Study deduplication summary written to {options['output_csv']}"
            )
        )
