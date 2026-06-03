from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from pydantic import BaseModel, Field

from analyses.models import Analysis, Assembly
from genomes.models import AdditionalContainedGenomes, GenomeAssemblyLink


class GroupMergeResult(BaseModel):
    """Summary of one duplicate-assembly merge attempt."""

    canonical_assembly: str
    merged_assemblies: list[str]
    deleted_assemblies: list[str] = Field(default_factory=list)
    status: str = "dry_run"
    error: str | None = None
    grouped_accessions: list[str] = Field(default_factory=list)

    def as_csv_row(self) -> dict[str, str]:
        """Convert the merge result into one flat CSV row."""
        return {
            "grouped_accessions": ", ".join(self.grouped_accessions),
            "canonical_assembly": self.canonical_assembly,
            "merged_assemblies": ", ".join(self.merged_assemblies),
            "deleted_assemblies": ", ".join(self.deleted_assemblies),
            "status": self.status,
            "error": self.error or "",
        }


class Command(BaseCommand):
    help = "Deduplicate MGnify assemblies that share ENA accessions and write a CSV summary."

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
            default="assembly_deduplication_summary.csv",
            help="Path to the CSV report to write.",
        )

    def find_duplicated_assemblies(
        self,
        accessions: list[str] | None = None,
    ) -> list[list[Assembly]]:
        """
        Load duplicate Assembly groups by overlapping ena_accessions values.

        :param accessions: Optional ENA accessions used to limit which groups are returned.
        :return: Duplicate groups whose accession arrays overlap transitively.
        """
        groups: list[list[Assembly]] = []
        requested = set(accessions or [])

        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RECURSIVE
                -- 1. Expand each assembly row into one row per accession alias.
                assembly_accessions AS (
                    SELECT
                        a.id AS assembly_id,
                        unnest(a.ena_accessions) AS accession
                    FROM analyses_assembly a
                    WHERE cardinality(a.ena_accessions) > 0
                ),
                -- 2. Build graph edges between assemblies that share any accession.
                assembly_links AS (
                    SELECT DISTINCT
                        a.assembly_id AS source_id,
                        b.assembly_id AS target_id
                    FROM assembly_accessions a
                    JOIN assembly_accessions b
                        ON a.accession = b.accession
                ),
                -- 3. Recursively walk the overlap graph from each assembly.
                components AS (
                    SELECT
                        source_id AS root_id,
                        source_id AS assembly_id
                    FROM assembly_links
                    UNION
                    SELECT
                        c.root_id,
                        l.target_id AS assembly_id
                    FROM components c
                    JOIN assembly_links l
                        ON l.source_id = c.assembly_id
                ),
                -- 4. Collapse each connected component to a stable component id.
                grouped AS (
                    SELECT
                        MIN(root_id) AS component_id,
                        assembly_id
                    FROM components
                    GROUP BY assembly_id
                )
                -- 5. Return one row per duplicate Assembly group.
                SELECT
                    array_agg(assembly_id ORDER BY assembly_id) AS assembly_ids
                FROM grouped
                GROUP BY component_id
                HAVING COUNT(*) > 1
                """)
            duplicate_groups = [row[0] for row in cursor.fetchall()]

        for assembly_ids in duplicate_groups:
            group = list(
                Assembly.objects.select_related(
                    "sample",
                    "reads_study",
                    "assembly_study",
                    "assembler",
                    "ena_study",
                )
                .filter(id__in=assembly_ids)
                .order_by("id")
            )

            if requested and not any(
                bool(requested.intersection(set(assembly.ena_accessions)))
                for assembly in group
            ):
                continue

            groups.append(group)
        return groups

    def update_assembly_accessions(
        self, canonical: Assembly, duplicates: list[Assembly]
    ) -> None:
        """
        Union ENA accession aliases onto the canonical Assembly.

        :param canonical: The Assembly row that will be kept.
        :param duplicates: Assembly rows that will be merged into the canonical one.
        """
        all_accessions = set(canonical.ena_accessions or [])
        for duplicate in duplicates:
            all_accessions.update(duplicate.ena_accessions or [])

        merged_accessions = sorted(all_accessions)
        if sorted(canonical.ena_accessions or []) != merged_accessions:
            canonical.ena_accessions = merged_accessions
            canonical.save(update_fields=["ena_accessions"])

    def merge_assembly_fields(
        self, canonical: Assembly, duplicates: list[Assembly]
    ) -> None:
        """
        Preserve assembly-owned fields from duplicates before deleting them.

        :param canonical: The Assembly row that will be kept.
        :param duplicates: Assembly rows that will be merged into the canonical one.
        """
        updated = False

        for duplicate in duplicates:
            for attr_name, update_field in [
                ("dir", "dir"),
                ("sample_id", "sample"),
                ("reads_study_id", "reads_study"),
                ("assembly_study_id", "assembly_study"),
                ("assembler_id", "assembler"),
                ("ena_study_id", "ena_study"),
                ("webin_submitter", "webin_submitter"),
            ]:
                if not getattr(canonical, attr_name) and getattr(duplicate, attr_name):
                    setattr(canonical, attr_name, getattr(duplicate, attr_name))
                    updated = True

            merged_metadata = {
                **(duplicate.metadata or {}),
                **(canonical.metadata or {}),
            }
            if canonical.metadata != merged_metadata:
                canonical.metadata = merged_metadata
                updated = True

            merged_status = {
                **(canonical.status or {}),
                **{
                    key: value
                    for key, value in (duplicate.status or {}).items()
                    if key not in (canonical.status or {})
                },
            }
            for key, value in (duplicate.status or {}).items():
                if isinstance(value, bool) and value:
                    merged_status[key] = True

            if canonical.status != merged_status:
                canonical.status = merged_status
                updated = True

            if duplicate.is_private and not canonical.is_private:
                canonical.is_private = True
                updated = True
            if duplicate.is_suppressed and not canonical.is_suppressed:
                canonical.is_suppressed = True
                updated = True

        if updated:
            canonical.save()

    def rewire_assembly_relations(
        self, canonical_assembly: Assembly, duplicates: list[Assembly]
    ) -> None:
        """
        Reassign direct Assembly relations from duplicate Assemblies to the canonical Assembly.

        It reassigns the assembly links to Runs and Genomes.

        :param canonical_assembly: The Assembly row that will be kept.
        :param duplicates: Assembly rows that will be merged into the canonical one.
        """
        assembly_run_through = Assembly.runs.through

        for duplicate in duplicates:
            # -- Runs -- #
            duplicate_run_ids = list(
                assembly_run_through.objects.filter(
                    assembly_id=duplicate.id
                ).values_list("run_id", flat=True)
            )
            assembly_run_through.objects.bulk_create(
                [
                    assembly_run_through(
                        assembly_id=canonical_assembly.id,
                        run_id=run_id,
                    )
                    for run_id in duplicate_run_ids
                ],
                ignore_conflicts=True,
            )
            assembly_run_through.objects.filter(assembly_id=duplicate.id).delete()

            if Analysis.objects.filter(assembly=duplicate).exists():
                Analysis.objects.filter(assembly=duplicate).update(
                    assembly=canonical_assembly
                )

            # -- Genomes -- #
            duplicate_genome_links = list(
                GenomeAssemblyLink.objects.filter(assembly=duplicate)
            )
            GenomeAssemblyLink.objects.bulk_create(
                [
                    GenomeAssemblyLink(
                        genome=link.genome,
                        # Re-assign
                        assembly=canonical_assembly,
                        species_rep=link.species_rep,
                        mag_accession=link.mag_accession,
                    )
                    for link in duplicate_genome_links
                ],
                ignore_conflicts=True,
            )
            GenomeAssemblyLink.objects.filter(assembly=duplicate).delete()

            duplicate_contained_genomes = list(
                AdditionalContainedGenomes.objects.filter(assembly=duplicate)
            )
            AdditionalContainedGenomes.objects.bulk_create(
                [
                    AdditionalContainedGenomes(
                        run=contained.run,
                        genome=contained.genome,
                        assembly=canonical_assembly,
                        containment=contained.containment,
                        cani=contained.cani,
                    )
                    for contained in duplicate_contained_genomes
                ],
                ignore_conflicts=True,
            )
            AdditionalContainedGenomes.objects.filter(assembly=duplicate).delete()

    def assembly_is_fully_detached(self, assembly: Assembly) -> bool:
        """
        Check whether an Assembly has any remaining direct relations that block deletion.

        :param assembly: Assembly row being considered for deletion after rewiring.
        :return: True if no direct tracked relations remain attached.
        """
        return not any(
            [
                assembly.runs.exists(),
                assembly.analyses.exists(),
                assembly.genome_links.exists(),
                assembly.additional_contained_genomes.exists(),
            ]
        )

    def merge_duplicate_group(
        self,
        assemblies: list[Assembly],
        dry_run: bool = True,
    ) -> GroupMergeResult:
        """
        Merge one duplicate Assembly group onto a single canonical Assembly.

        :param assemblies: Duplicate Assembly rows in one connected component.
        :param dry_run: If True, calculate merge actions without persisting them.
        :return: Summary of the merge outcome for reporting.
        """
        canonical_assembly = sorted(assemblies, key=lambda assembly: assembly.id)[0]
        duplicates = [
            assembly for assembly in assemblies if assembly.pk != canonical_assembly.pk
        ]

        grouped_accessions = sorted(
            {
                accession
                for assembly in assemblies
                for accession in assembly.ena_accessions
            }
        )

        result = GroupMergeResult(
            canonical_assembly=canonical_assembly.first_accession
            or str(canonical_assembly.id),
            merged_assemblies=[
                assembly.first_accession or str(assembly.id) for assembly in assemblies
            ],
            deleted_assemblies=[
                assembly.first_accession or str(assembly.id) for assembly in duplicates
            ],
            grouped_accessions=grouped_accessions,
            status="dry_run" if dry_run else "applied",
        )

        if dry_run:
            self.stdout.write(
                f"Dry run: Would merge {len(duplicates)} assemblies into {canonical_assembly}"
            )
            return result

        with transaction.atomic():
            self.update_assembly_accessions(canonical_assembly, duplicates)
            self.merge_assembly_fields(canonical_assembly, duplicates)
            self.rewire_assembly_relations(canonical_assembly, duplicates)

            deleted_assemblies: list[str] = []
            for duplicate in duplicates:
                duplicate.refresh_from_db()
                if self.assembly_is_fully_detached(duplicate):
                    deleted_assemblies.append(
                        duplicate.first_accession or str(duplicate.id)
                    )
                    duplicate.delete()
                else:
                    raise ValueError(
                        f"Assembly {duplicate} is still connected to other objects and cannot be deleted."
                    )
            result.deleted_assemblies = deleted_assemblies

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
            "canonical_assembly",
            "merged_assemblies",
            "deleted_assemblies",
            "status",
            "error",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return output_path

    def deduplicate_assemblies(
        self,
        accessions: list[str] | None = None,
        dry_run: bool = True,
        output_csv: str | Path | None = None,
    ) -> list[dict[str, str]]:
        """
        Deduplicate MGnify Assembly rows and emit a summary.

        :param accessions: Optional ENA accessions used to limit processing to matching groups.
        :param dry_run: If True, report intended changes without persisting them.
        :param output_csv: Destination for the CSV summary report.
        :return: Flat rows describing every processed duplicate group.
        """
        duplicated_assemblies = self.find_duplicated_assemblies(accessions=accessions)
        self.stdout.write(
            f"Found {len(duplicated_assemblies)} duplicate assemblies to process"
            + (" in dry-run mode" if dry_run else "")
        )

        results: list[GroupMergeResult] = []
        for index, assemblies in enumerate(duplicated_assemblies, start=1):
            self.stdout.write(
                f"Processing assembly group {index}/{len(duplicated_assemblies)}: "
                f"{', '.join(assembly.first_accession or str(assembly.id) for assembly in assemblies)}"
            )
            try:
                result = self.merge_duplicate_group(assemblies, dry_run=dry_run)
                results.append(result)
                self.stdout.write(
                    f"Finished group {index}/{len(duplicated_assemblies)} "
                    f"with canonical assembly {result.canonical_assembly}"
                )
            except Exception as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to deduplicate assembly group {[assembly.id for assembly in assemblies]}: {exc}"
                    )
                )
                results.append(
                    GroupMergeResult(
                        canonical_assembly=assemblies[0].first_accession
                        or str(assemblies[0].id),
                        merged_assemblies=[
                            assembly.first_accession or str(assembly.id)
                            for assembly in assemblies
                        ],
                        grouped_accessions=sorted(
                            {
                                accession
                                for assembly in assemblies
                                for accession in assembly.ena_accessions
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
            else Path.cwd() / "assembly_deduplication_summary.csv"
        )
        self.write_results_csv(csv_rows, output_path)
        self.stdout.write(f"Wrote assembly deduplication summary to {output_path}")
        return csv_rows

    def handle(self, *args, **options):
        self.deduplicate_assemblies(
            accessions=options["accessions"],
            dry_run=options["dry_run"],
            output_csv=options["output_csv"],
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Assembly deduplication summary written to {options['output_csv']}"
            )
        )
