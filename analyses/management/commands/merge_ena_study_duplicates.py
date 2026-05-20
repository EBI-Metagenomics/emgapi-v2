from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from pydantic import BaseModel, Field

import ena.models as ena_models
from analyses.models import Analysis, Assembly, Run, Sample, Study
from workflows.ena_utils.ena_accession_matching import INSDC_STUDY_ACCESSION_REGEX


class GroupMergeResult(BaseModel):
    """Summary of one duplicate ENA Study merge attempt."""

    canonical_ena_study: str
    merged_ena_studies: list[str]
    deleted_ena_studies: list[str] = Field(default_factory=list)
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_csv_row(self) -> dict[str, str]:
        """Convert the merge result into one flat CSV row."""
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_ena_study": self.canonical_ena_study,
            "merged_ena_studies": ", ".join(self.merged_ena_studies),
            "deleted_ena_studies": ", ".join(self.deleted_ena_studies),
            "status": self.status,
            "error": self.error or "",
        }


class Command(BaseCommand):
    help = "Deduplicate ENA Study rows that share accessions and write a CSV summary."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--accession",
            action="append",
            dest="accessions",
            help="Limit processing to groups matching this ENA accession. Repeatable.",
        )
        parser.add_argument(
            "--output-csv",
            default="ena_study_deduplication_summary.csv",
            help="Path to the CSV report to write.",
        )

    def find_duplicated_ena_studies(
        self,
        accessions: list[str] | None = None,
    ) -> list[list[ena_models.Study]]:
        """
        Load duplicate ENA Study groups by overlapping accessions.

        :param accessions: Optional ENA accessions used to limit which groups are returned.
        :return: Duplicate groups whose accession aliases place them in the same connected component.
        """
        groups: list[list[ena_models.Study]] = []
        requested = set(accessions or [])

        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE
                -- 1. Expand each ENA Study row into one row per accession alias.
                study_accessions AS (
                    SELECT
                        s.accession AS study_id,
                        s.accession AS accession
                    FROM ena_study s
                    WHERE s.accession IS NOT NULL
                    UNION
                    SELECT
                        s.accession AS study_id,
                        jsonb_array_elements_text(s.additional_accessions) AS accession
                    FROM ena_study s
                    WHERE s.additional_accessions IS NOT NULL
                ),
                -- 2. Build graph edges between ENA studies that share any accession.
                study_links AS (
                    SELECT DISTINCT
                        a.study_id AS source_id,
                        b.study_id AS target_id
                    FROM study_accessions a
                    JOIN study_accessions b
                        ON a.accession = b.accession
                ),
                -- 3. Recursively walk the overlap graph from each ENA study.
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
                -- 5. Return one row per duplicate ENA Study group.
                SELECT
                    array_agg(study_id ORDER BY study_id) AS study_ids
                FROM grouped
                GROUP BY component_id
                HAVING COUNT(*) > 1
                """)
            duplicate_groups = [row[0] for row in cursor.fetchall()]

        for study_ids in duplicate_groups:
            group = list(
                ena_models.Study.objects.filter(accession__in=study_ids).order_by(
                    "accession"
                )
            )

            if requested and not any(
                item.accession in requested
                or bool(requested.intersection(set(item.additional_accessions or [])))
                for item in group
            ):
                continue

            groups.append(group)
        return groups

    def choose_canonical_ena_study(
        self, studies: list[ena_models.Study]
    ) -> ena_models.Study:
        """
        Pick a stable canonical ENA Study.

        :param studies: Duplicate ENA Study rows in one connected component.
        :return: The ENA Study row to keep.
        """
        return sorted(
            studies,
            key=lambda study: (
                not bool(INSDC_STUDY_ACCESSION_REGEX.fullmatch(study.accession)),
                study.accession,
            ),
        )[0]

    def update_ena_study_accessions(
        self, canonical: ena_models.Study, duplicates: list[ena_models.Study]
    ) -> None:
        """
        Union ENA accession aliases onto the canonical ENA Study.

        :param canonical: ENA Study row that will be kept.
        :param duplicates: ENA Study rows that will be rewired and deleted.
        """
        all_accessions = {
            canonical.accession,
            *(canonical.additional_accessions or []),
        }
        for duplicate in duplicates:
            all_accessions.add(duplicate.accession)
            all_accessions.update(duplicate.additional_accessions or [])

        merged_additional_accessions = sorted(
            accession
            for accession in all_accessions
            if accession != canonical.accession
        )
        if (
            sorted(canonical.additional_accessions or [])
            != merged_additional_accessions
        ):
            canonical.additional_accessions = merged_additional_accessions
            canonical.save(update_fields=["additional_accessions"])

    def rewire_ena_study_relations(
        self, canonical_ena_study: ena_models.Study, duplicates: list[ena_models.Study]
    ) -> None:
        """
        Reassign direct ENA Study relations to the canonical ENA Study.

        :param canonical_ena_study: ENA Study row that will be kept.
        :param duplicates: ENA Study rows that will be deleted.
        """
        canonical_study_accessions = [
            canonical_ena_study.accession,
            *(canonical_ena_study.additional_accessions or []),
        ]
        for duplicate in duplicates:
            # -- MGnify Studies -- #
            mgnify_studies = list(Study.objects.filter(ena_study=duplicate))
            for study in mgnify_studies:
                study.ena_study = canonical_ena_study
                study.ena_accessions = sorted(
                    set((study.ena_accessions or []) + canonical_study_accessions)
                )
            if mgnify_studies:
                Study.objects.bulk_update(
                    mgnify_studies, ["ena_study", "ena_accessions"]
                )

            # -- MGnify Samples -- #
            Sample.objects.filter(ena_study=duplicate).update(
                ena_study=canonical_ena_study
            )

            # -- Runs -- #
            Run.objects.filter(ena_study=duplicate).update(
                ena_study=canonical_ena_study
            )

            # -- Assemblies -- #
            Assembly.objects.filter(ena_study=duplicate).update(
                ena_study=canonical_ena_study
            )

            # -- Analyses -- #
            Analysis.objects.filter(ena_study=duplicate).update(
                ena_study=canonical_ena_study
            )

            # -- ENA Samples -- #
            ena_models.Sample.objects.filter(study=duplicate).update(
                study=canonical_ena_study
            )

    def ena_study_is_fully_detached(self, ena_study: ena_models.Study) -> bool:
        """
        Check whether an ENA Study has remaining relations that block deletion.

        :param ena_study: ENA Study row being considered for deletion after rewiring.
        :return: True if no tracked relations remain attached.
        """
        return not any(
            [
                Study.objects.filter(ena_study=ena_study).exists(),
                Sample.objects.filter(ena_study=ena_study).exists(),
                Run.objects.filter(ena_study=ena_study).exists(),
                Assembly.objects.filter(ena_study=ena_study).exists(),
                Analysis.objects.filter(ena_study=ena_study).exists(),
                ena_models.Sample.objects.filter(study=ena_study).exists(),
            ]
        )

    def merge_duplicate_group(
        self,
        studies: list[ena_models.Study],
        dry_run: bool = True,
    ) -> GroupMergeResult:
        """
        Merge one duplicate ENA Study group onto a single canonical ENA Study.

        :param studies: Duplicate ENA Study rows in one connected component.
        :param dry_run: If True, calculate merge actions without persisting them.
        :return: Summary of the merge outcome for reporting.
        """
        canonical_ena_study = self.choose_canonical_ena_study(studies)
        duplicates = [study for study in studies if study.pk != canonical_ena_study.pk]
        grouped_accessions = sorted(
            {
                accession
                for study in studies
                for accession in [study.accession, *(study.additional_accessions or [])]
            }
        )

        result = GroupMergeResult(
            canonical_ena_study=canonical_ena_study.accession,
            merged_ena_studies=[study.accession for study in studies],
            deleted_ena_studies=[study.accession for study in duplicates],
            grouped_accessions=grouped_accessions,
            status="dry_run" if dry_run else "applied",
        )

        if dry_run:
            self.stdout.write(
                f"Dry run: would merge {len(duplicates)} ENA studies into {canonical_ena_study}"
            )
            return result

        with transaction.atomic():
            self.update_ena_study_accessions(canonical_ena_study, duplicates)
            self.rewire_ena_study_relations(canonical_ena_study, duplicates)

            deleted_ena_studies: list[str] = []
            for duplicate in duplicates:
                duplicate.refresh_from_db()
                if self.ena_study_is_fully_detached(duplicate):
                    deleted_ena_studies.append(duplicate.accession)
                    duplicate.delete()
                else:
                    raise ValueError(
                        f"ENA Study {duplicate} is still connected to other objects and cannot be deleted."
                    )
            result.deleted_ena_studies = deleted_ena_studies

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
            "canonical_ena_study",
            "merged_ena_studies",
            "deleted_ena_studies",
            "status",
            "error",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return output_path

    def merge_ena_study_duplicates(
        self,
        accessions: list[str] | None = None,
        dry_run: bool = True,
        output_csv: str | Path | None = None,
    ) -> list[dict[str, str]]:
        """
        Deduplicate ENA Study rows and emit a summary.

        :param accessions: Optional ENA accessions used to limit processing to matching groups.
        :param dry_run: If True, report intended changes without persisting them.
        :param output_csv: Destination for the CSV summary report.
        :return: Flat rows describing every processed duplicate group.
        """
        duplicated_studies = self.find_duplicated_ena_studies(accessions=accessions)
        self.stdout.write(
            f"Found {len(duplicated_studies)} duplicate ENA studies to process"
            + (" in dry-run mode" if dry_run else "")
        )

        results: list[GroupMergeResult] = []
        for index, studies in enumerate(duplicated_studies, start=1):
            self.stdout.write(
                f"Processing ENA study group {index}/{len(duplicated_studies)}: "
                f"{', '.join(study.accession for study in studies)}"
            )
            try:
                result = self.merge_duplicate_group(studies, dry_run=dry_run)
            except Exception as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to deduplicate ENA study group {[study.accession for study in studies]}: {exc}"
                    )
                )
                canonical_study = self.choose_canonical_ena_study(studies)
                results.append(
                    GroupMergeResult(
                        canonical_ena_study=canonical_study.accession,
                        merged_ena_studies=[study.accession for study in studies],
                        grouped_accessions=sorted(
                            {
                                accession
                                for study in studies
                                for accession in [
                                    study.accession,
                                    *(study.additional_accessions or []),
                                ]
                            }
                        ),
                        status="failed",
                        error=str(exc),
                    )
                )
                continue

            results.append(result)
            self.stdout.write(
                f"Finished group {index}/{len(duplicated_studies)} "
                f"with canonical ENA study {result.canonical_ena_study}"
            )

        csv_rows = [result.as_csv_row() for result in results]
        output_path = (
            Path(output_csv)
            if output_csv
            else Path.cwd() / "ena_study_deduplication_summary.csv"
        )
        self.write_results_csv(csv_rows, output_path)
        self.stdout.write(f"Wrote ENA study deduplication summary to {output_path}")
        return csv_rows

    def handle(self, *args, **options):
        self.merge_ena_study_duplicates(
            accessions=options["accessions"],
            dry_run=options["dry_run"],
            output_csv=options["output_csv"],
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"ENA study deduplication summary written to {options['output_csv']}"
            )
        )
