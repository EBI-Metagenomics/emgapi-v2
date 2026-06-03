from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from pydantic import BaseModel, Field

from analyses.models import Analysis, Assembly, Run


class GroupMergeResult(BaseModel):
    """Summary of one duplicate-run merge attempt."""

    canonical_run: str
    merged_runs: list[str]
    deleted_runs: list[str] = Field(default_factory=list)
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_csv_row(self) -> dict[str, str]:
        """Convert the merge result into one flat CSV row."""
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_run": self.canonical_run,
            "merged_runs": ", ".join(self.merged_runs),
            "deleted_runs": ", ".join(self.deleted_runs),
            "status": self.status,
            "error": self.error or "",
        }


class Command(BaseCommand):
    help = "Deduplicate MGnify runs that share ENA accessions and write a CSV summary."

    def add_arguments(self, parser):
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            default=True,
            help="Report intended changes without modifying the database (default).",
        )
        mode_group.add_argument(
            "--apply",
            action="store_false",
            dest="dry_run",
            help="Apply deduplication changes to the database.",
        )
        parser.add_argument(
            "--accession",
            action="append",
            dest="accessions",
            help="Limit processing to groups matching this ENA accession. Repeatable.",
        )
        parser.add_argument(
            "--output-csv",
            default="run_deduplication_summary.csv",
            help="Path to the CSV report to write.",
        )

    def find_duplicated_runs(
        self,
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

    def update_run_accessions(self, canonical: Run, duplicates: list[Run]) -> None:
        """
        Union ENA accession aliases onto the canonical Run.

        :param canonical: The Run row that will be kept.
        :param duplicates: Run rows that will be merged into the canonical one.
        """
        all_accessions = set(canonical.ena_accessions or [])
        for duplicate in duplicates:
            all_accessions.update(duplicate.ena_accessions or [])

        merged_accessions = sorted(all_accessions)
        if sorted(canonical.ena_accessions or []) != merged_accessions:
            canonical.ena_accessions = merged_accessions
            canonical.save(update_fields=["ena_accessions"])

    def rewire_run_relations(self, canonical_run: Run, duplicates: list[Run]) -> None:
        """
        Reassign direct Run relations from duplicate Runs to the canonical Run.

        :param canonical_run: The Run row that will be kept.
        :param duplicates: Run rows that will be merged into the canonical one.
        """
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

    def run_is_fully_detached(self, run: Run) -> bool:
        """
        Check whether a Run has any remaining direct relations that block deletion.

        :param run: Run row being considered for deletion after rewiring.
        :return: True if no direct tracked relations remain attached.
        """
        return not any(
            [
                run.analyses.exists(),
                run.assemblies.exists(),
            ]
        )

    def merge_duplicate_group(
        self,
        runs: list[Run],
        dry_run: bool = True,
    ) -> GroupMergeResult:
        """
        Merge one duplicate Run group onto a single canonical Run.

        :param runs: Duplicate Run rows in one connected component.
        :param dry_run: If True, calculate merge actions without persisting them.
        :return: Summary of the merge outcome for reporting.
        """
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
            self.stdout.write(
                f"Dry run: Would merge {len(duplicates)} runs into {canonical_run}"
            )
            return result

        with transaction.atomic():
            self.update_run_accessions(canonical_run, duplicates)
            self.rewire_run_relations(canonical_run, duplicates)

            deleted_runs: list[str] = []
            for duplicate in duplicates:
                duplicate.refresh_from_db()
                if self.run_is_fully_detached(duplicate):
                    deleted_runs.append(duplicate.first_accession or str(duplicate.id))
                    duplicate.delete()
                else:
                    raise ValueError(
                        f"Run {duplicate} is still connected to other objects and cannot be deleted."
                    )
            result.deleted_runs = deleted_runs

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
            "canonical_run",
            "merged_runs",
            "deleted_runs",
            "status",
            "error",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return output_path

    def deduplicate_runs(
        self,
        accessions: list[str] | None = None,
        dry_run: bool = True,
        output_csv: str | Path | None = None,
    ) -> list[dict[str, str]]:
        """
        Deduplicate MGnify Run rows and emit a summary.

        :param accessions: Optional ENA accessions used to limit processing to matching groups.
        :param dry_run: If True, report intended changes without persisting them.
        :param output_csv: Destination for the CSV summary report.
        :return: Flat rows describing every processed duplicate group.
        """
        duplicated_runs = self.find_duplicated_runs(accessions=accessions)
        self.stdout.write(
            f"Found {len(duplicated_runs)} duplicate runs to process"
            + (" in dry-run mode" if dry_run else "")
        )

        results: list[GroupMergeResult] = []
        for index, runs in enumerate(duplicated_runs, start=1):
            self.stdout.write(
                f"Processing run group {index}/{len(duplicated_runs)}: "
                f"{', '.join(run.first_accession or str(run.id) for run in runs)}"
            )
            try:
                result = self.merge_duplicate_group(runs, dry_run=dry_run)
                results.append(result)
                self.stdout.write(
                    f"Finished group {index}/{len(duplicated_runs)} "
                    f"with canonical run {result.canonical_run}"
                )
            except Exception as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to deduplicate run group {[run.id for run in runs]}: {exc}"
                    )
                )
                results.append(
                    GroupMergeResult(
                        canonical_run=runs[0].first_accession or str(runs[0].id),
                        merged_runs=[
                            run.first_accession or str(run.id) for run in runs
                        ],
                        grouped_accessions=sorted(
                            {
                                accession
                                for run in runs
                                for accession in run.ena_accessions
                            }
                        ),
                        status="failed",
                        error=str(exc),
                    )
                )
                continue

        csv_rows = [result.as_csv_row() for result in results]
        output_path = (
            Path(output_csv)
            if output_csv
            else Path.cwd() / "run_deduplication_summary.csv"
        )
        self.write_results_csv(csv_rows, output_path)
        self.stdout.write(f"Wrote run deduplication summary to {output_path}")
        return csv_rows

    def handle(self, *args, **options):
        self.deduplicate_runs(
            accessions=options["accessions"],
            dry_run=options["dry_run"],
            output_csv=options["output_csv"],
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Run deduplication summary written to {options['output_csv']}"
            )
        )
