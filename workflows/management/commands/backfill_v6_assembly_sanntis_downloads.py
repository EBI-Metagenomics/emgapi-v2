from pathlib import Path

from django.core.management.base import BaseCommand

from activate_django_first import EMG_CONFIG

from analyses.base_models.with_downloads_models import DownloadFile
from analyses.models import Analysis
from workflows.data_io_utils.schemas.assembly import AssemblyResultSchema

ASSEMBLY_EXPERIMENT_TYPES = (
    Analysis.ExperimentTypes.ASSEMBLY,
    Analysis.ExperimentTypes.HYBRID_ASSEMBLY,
    Analysis.ExperimentTypes.LONG_READ_ASSEMBLY,
)


def add_missing_sanntis_downloads(analysis, results_dir: Path, dry_run: bool) -> int:
    """Append downloads described by the SanntiS schema when their files exist."""
    schema = AssemblyResultSchema()
    pathways = next(
        directory
        for directory in schema.directories
        if directory.folder_name
        == EMG_CONFIG.assembly_analysis_pipeline.pathways_systems_folder
    )
    sanntis = next(
        directory
        for directory in pathways.subdirectories
        if directory.folder_name == "sanntis"
    )
    sanntis_dir = results_dir / pathways.folder_name / sanntis.folder_name
    existing_aliases = {download["alias"] for download in analysis.downloads}
    added = 0

    for file_schema in sanntis.files:
        download = DownloadFile.from_pipeline_file_schema(
            file_schema,
            analysis,
            sanntis_dir,
            results_dir,
            file_identifier_lookup_string="assembly.first_accession",
        )
        if download is None or download.alias in existing_aliases:
            continue
        added += 1
        if not dry_run:
            analysis.add_download(download)
            existing_aliases.add(download.alias)

    return added


class Command(BaseCommand):
    help = (
        "Backfill missing SanntiS downloads on v6 assembly analyses. "
        "DIRECT FILESYSTEM ACCESS TO THE FTP_RESULTS_DIR IS REQUIRED. "
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report additions without changing analyses.",
        )

    def handle(self, *args, **options):
        analyses = Analysis.objects.select_related("assembly").filter(
            experiment_type__in=ASSEMBLY_EXPERIMENT_TYPES,
            pipeline_version=Analysis.PipelineVersions.v6,
        )
        total = 0

        for analysis in analyses.iterator():
            if not analysis.external_results_dir:
                continue
            results_dir = (
                Path(EMG_CONFIG.slurm.ftp_results_dir) / analysis.external_results_dir
            )
            added = add_missing_sanntis_downloads(
                analysis, results_dir, options["dry_run"]
            )
            if added:
                total += added
                self.stdout.write(f"{analysis.accession}: {added}")

        action = "Would add" if options["dry_run"] else "Added"
        self.stdout.write(f"{action} {total} SanntiS download(s).")
