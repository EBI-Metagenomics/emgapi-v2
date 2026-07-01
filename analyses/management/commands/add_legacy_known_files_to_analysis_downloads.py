import json
from pathlib import Path
from typing import Iterable

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis


class LocalDirectoryManifest:
    def __init__(self, root: Path):
        self.root = root
        self._listing_cache: dict[str, set[str]] = {}

    def list_names(self, relative_dir: str) -> set[str]:
        relative_dir = str(Path(relative_dir)) if relative_dir else ""
        if relative_dir in self._listing_cache:
            return self._listing_cache[relative_dir]

        directory = self.root / relative_dir
        names = (
            {path.name for path in directory.iterdir()} if directory.is_dir() else set()
        )
        self._listing_cache[relative_dir] = names
        return names

    def file_exists(self, relative_path: str) -> bool:
        path = Path(relative_path)
        parent = "" if path.parent == Path(".") else str(path.parent)
        return path.name in self.list_names(parent)


class Command(BaseCommand):
    help = (
        "Add legacy V4.1/V5 known files (e.g. QC files) "
        "to Analysis.downloads."
        "THIS CAN ONLY RUN WHERE BOTH PUBLIC AND PRIVATE NFS"
        "ARE AVAILABLE"
    )

    V4_1 = Analysis.PipelineVersions.v4_1
    V5 = Analysis.PipelineVersions.v5
    SUPPORTED_VERSIONS = (V4_1, V5)
    ASSEMBLY_EXPERIMENT_TYPES = {
        Analysis.ExperimentTypes.ASSEMBLY,
        Analysis.ExperimentTypes.HYBRID_ASSEMBLY,
        Analysis.ExperimentTypes.LONG_READ_ASSEMBLY,
    }

    def add_arguments(self, parser):
        scope = parser.add_mutually_exclusive_group(required=True)
        scope.add_argument(
            "--accession",
            "--mgya",
            help="Only process one analysis accession, e.g. MGYA00000001.",
        )
        scope.add_argument(
            "--v4-1",
            action="store_true",
            help="Process all V4.1 analyses.",
        )
        scope.add_argument(
            "--v5",
            action="store_true",
            help="Process all V5 analyses.",
        )
        scope.add_argument(
            "--all-supported",
            "--v4-1-and-v5",
            action="store_true",
            help="Process all V4.1 and V5 analyses.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List downloads that would be created without updating analyses.",
        )
        parser.add_argument(
            "--public-results-root",
            required=True,
            help="NFS root containing public MGnify result directories.",
        )
        parser.add_argument(
            "--private-results-root",
            required=True,
            help="NFS root containing private MGnify result directories.",
        )

    @classmethod
    def _is_v4_1(cls, analysis: Analysis) -> bool:
        return analysis.pipeline_version == cls.V4_1

    @classmethod
    def _is_v5(cls, analysis: Analysis) -> bool:
        return analysis.pipeline_version == cls.V5

    @classmethod
    def _is_supported(cls, analysis: Analysis) -> bool:
        return analysis.pipeline_version in cls.SUPPORTED_VERSIONS

    @classmethod
    def _is_assembly(cls, analysis: Analysis) -> bool:
        return analysis.experiment_type in cls.ASSEMBLY_EXPERIMENT_TYPES

    @staticmethod
    def _download_dict(download: DownloadFile) -> dict:
        return download.model_dump(exclude={"parent_identifier"})

    @classmethod
    def _qc_known_files(cls, analysis: Analysis) -> Iterable[DownloadFile]:
        known_paths = [
            "qc-statistics/summary.out",
            "qc-statistics/summary.out" if cls._is_v4_1(analysis) else "qc_summary",
            "qc-statistics/seq-length.out",
            "qc-statistics/seq-length.out.full",
            "qc-statistics/seq-length.out.sub-set",
            "seq-length.out",
            "seq-length.out.full",
            "seq-length.out.sub-set",
            "qc-statistics/GC-distribution.out",
            "qc-statistics/GC-distribution.out.full",
            "qc-statistics/GC-distribution.out.sub-set",
            "GC-distribution.out",
            "GC-distribution.out.full",
            "GC-distribution.out.sub-set",
            "qc-statistics/nucleotide-distribution.out",
            "qc-statistics/nucleotide-distribution.out.full",
            "qc-statistics/nucleotide-distribution.out.sub-set",
            "nucleotide-distribution.out",
            "nucleotide-distribution.out.full",
            "nucleotide-distribution.out.sub-set",
        ]
        for path in known_paths:
            yield DownloadFile(
                path=path,
                alias=path,
                file_type=DownloadFileType.OTHER,
                download_type=DownloadType.QUALITY_CONTROL,
                download_group=Analysis.QC,
                short_description="Legacy quality control file",
                long_description=(
                    "Quality control file from a legacy MGnify analysis pipeline."
                ),
            )

    @staticmethod
    def _is_taxonomic_download(download: dict) -> bool:
        return (
            download.get("download_group") == "taxonomic_analysis"
            or download.get("download_type") == DownloadType.TAXONOMIC_ANALYSIS
        )

    @staticmethod
    def _taxonomy_file_type(path: str) -> DownloadFileType:
        if path.endswith(".html"):
            return DownloadFileType.HTML
        return DownloadFileType.TSV

    @classmethod
    def _taxonomy_known_files(cls, analysis: Analysis) -> Iterable[DownloadFile]:
        taxonomy_paths: dict[str, dict[str, str]] = {}
        detected_markers: set[str] = set()

        for download in analysis.downloads or []:
            if not cls._is_taxonomic_download(download):
                continue

            alias = (download.get("alias") or "").lower()
            marker = next(
                (
                    candidate
                    for candidate in ["ssu", "lsu", "itsonedb", "unite"]
                    if candidate in alias
                ),
                None,
            )
            if not marker:
                continue

            detected_markers.add(marker)
            marker_upper = marker.upper()
            taxonomy_paths.setdefault(
                marker,
                {
                    "krona": f"taxonomy-summary/{marker_upper}/krona.html",
                    "tsv": "",
                    "txt": "",
                },
            )

            path = download.get("path") or ""
            is_gzipped = path.endswith(".gz")
            file_type = download.get("file_type")

            if file_type == DownloadFileType.HTML and "krona" in alias:
                taxonomy_paths[marker]["krona"] = path
            elif file_type == DownloadFileType.TSV and not is_gzipped:
                if path.endswith(".txt"):
                    taxonomy_paths[marker]["txt"] = path
                else:
                    taxonomy_paths[marker]["tsv"] = path
            elif file_type == "txt" and not is_gzipped:
                taxonomy_paths[marker]["txt"] = path
            elif (
                file_type == DownloadFileType.BIOM and not taxonomy_paths[marker]["tsv"]
            ):
                taxonomy_paths[marker]["tsv"] = path.replace(".biom", ".tsv")

        if not detected_markers:
            for marker in ["ssu", "lsu"]:
                marker_upper = marker.upper()
                taxonomy_paths[marker] = {
                    "krona": f"taxonomy-summary/{marker_upper}/krona.html",
                    "tsv": (
                        f"taxonomy-summary/{marker_upper}/"
                        f"{analysis.accession}_{marker_upper}.fasta.mseq.tsv"
                    ),
                    "txt": (
                        f"taxonomy-summary/{marker_upper}/"
                        f"{analysis.accession}_{marker_upper}.fasta.mseq.txt"
                    ),
                }

        for marker, paths in taxonomy_paths.items():
            for path in paths.values():
                if not path:
                    continue
                yield DownloadFile(
                    path=path,
                    alias=path,
                    file_type=cls._taxonomy_file_type(path),
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=(
                        f"{Analysis.TAXONOMIES}.{Analysis.CLOSED_REFERENCE}.{marker}"
                    ),
                    short_description=f"Legacy {marker.upper()} taxonomy file",
                    long_description=(
                        "Taxonomic analysis file from a legacy MGnify analysis "
                        "pipeline."
                    ),
                )

    @classmethod
    def _functional_known_files(cls, analysis: Analysis) -> Iterable[DownloadFile]:
        if not cls._is_v4_1(analysis) or not analysis.assembly:
            return

        functional_accession = analysis.assembly.first_accession
        if not functional_accession:
            return

        functional_filename = (
            f"{functional_accession}_FASTA"
            if cls._is_assembly(analysis)
            else functional_accession
        )
        for suffix, download_group, short_description in [
            (
                "ipr",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.interpro",
                "Legacy InterPro summary",
            ),
            ("go", f"{Analysis.FUNCTIONAL_ANNOTATION}.go_slims", "Legacy GO summary"),
        ]:
            path = f"{functional_filename}_summary.{suffix}"
            yield DownloadFile(
                path=path,
                alias=path,
                file_type=DownloadFileType.TSV,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=download_group,
                short_description=short_description,
                long_description=(
                    "Functional analysis summary file from a legacy MGnify V4.1 "
                    "analysis pipeline."
                ),
            )

    @classmethod
    def _legacy_known_file_candidates(cls, analysis: Analysis) -> list[DownloadFile]:
        deduped = {}
        for generator in (
            cls._qc_known_files,
            cls._taxonomy_known_files,
            cls._functional_known_files,
        ):
            for download in generator(analysis):
                deduped.setdefault(str(download.path), download)
        return list(deduped.values())

    @classmethod
    def _legacy_known_files(
        cls, analysis: Analysis, manifest: LocalDirectoryManifest
    ) -> list[DownloadFile]:
        return [
            download
            for download in cls._legacy_known_file_candidates(analysis)
            if manifest.file_exists(str(download.path))
        ]

    @staticmethod
    def _would_duplicate(analysis: Analysis, download: DownloadFile) -> bool:
        existing_downloads = analysis.downloads or []
        existing_aliases = {dl.get("alias") for dl in existing_downloads}
        existing_paths = {dl.get("path") for dl in existing_downloads}
        return (
            download.alias in existing_aliases or str(download.path) in existing_paths
        )

    def _queryset(self, options):
        queryset = Analysis.objects.select_related("assembly").order_by("accession")
        accession = options["accession"]
        if accession:
            accession = accession.upper()
            if not queryset.filter(accession=accession).exists():
                raise CommandError(f"Analysis {accession} does not exist.")
            return queryset.filter(accession=accession)
        if options["v4_1"]:
            return queryset.filter(pipeline_version=self.V4_1)
        if options["v5"]:
            return queryset.filter(pipeline_version=self.V5)
        return queryset.filter(pipeline_version__in=self.SUPPORTED_VERSIONS)

    @staticmethod
    def _manifest_for_analysis(analysis: Analysis, options) -> LocalDirectoryManifest:
        root = (
            options["private_results_root"]
            if analysis.is_private
            else options["public_results_root"]
        )
        return LocalDirectoryManifest(Path(root) / analysis.external_results_dir)

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        analyses = self._queryset(options)

        scanned_count = 0
        updated_count = 0
        create_count = 0
        skipped_existing_count = 0
        skipped_unsupported_count = 0
        skipped_no_external_results_dir_count = 0

        for analysis in analyses.iterator():
            scanned_count += 1
            if not self._is_supported(analysis):
                skipped_unsupported_count += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"Skipping {analysis.accession}: unsupported pipeline "
                        f"version {analysis.pipeline_version}."
                    )
                )
                continue
            if not analysis.external_results_dir:
                skipped_no_external_results_dir_count += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"Skipping {analysis.accession}: external_results_dir is empty."
                    )
                )
                continue

            downloads_to_create = []
            legacy_known_files = self._legacy_known_files(
                analysis, self._manifest_for_analysis(analysis, options)
            )

            for download in legacy_known_files:
                if self._would_duplicate(analysis, download):
                    skipped_existing_count += 1
                    continue
                downloads_to_create.append(download)

            if not downloads_to_create:
                continue

            create_count += len(downloads_to_create)
            updated_count += 1

            if dry_run:
                for download in downloads_to_create:
                    output = {
                        "analysis_accession": analysis.accession,
                        "external_path": str(
                            Path(analysis.external_results_dir) / str(download.path)
                        ),
                        "download": self._download_dict(download),
                    }
                    self.stdout.write(json.dumps(output, sort_keys=True))
                continue

            with transaction.atomic():
                existing_downloads = list(analysis.downloads or [])
                existing_downloads.extend(
                    self._download_dict(download) for download in downloads_to_create
                )
                analysis.downloads = existing_downloads
                analysis.save(update_fields=["downloads"])
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Added {len(downloads_to_create)} downloads to "
                        f"{analysis.accession}."
                    )
                )

        action = "Would create" if dry_run else "Created"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} {create_count} downloads across {updated_count} analyses. "
                f"Scanned {scanned_count}; skipped existing {skipped_existing_count}; "
                f"skipped unsupported {skipped_unsupported_count}; skipped without "
                f"external_results_dir {skipped_no_external_results_dir_count}."
            )
        )
