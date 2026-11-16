from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from analyses.models import Analysis

ASV_SEQUENCES_DOWNLOAD_GROUP = "asv.sequences"
DOWNLOAD_GROUP = "download_group"
PATH = "path"
# asv.sequences only exists from v6; never touch older analyses' downloads.
V6_OR_LATER = [v for v in Analysis.PipelineVersions if float(v.value.lstrip("V")) >= 6]


class Command(BaseCommand):
    help = (
        "Repair asv.sequences downloads that were registered with an absolute path. "
        "Download paths are relative to the analysis' results dir, so an absolute one "
        "made a URL to the pipeline's workdir instead of a servable file."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report the downloads that would be repaired, without saving.",
        )

    def handle(self, *args, **options):
        asv_folder = Path(settings.EMG_CONFIG.amplicon_pipeline.asv_folder)
        analyses = Analysis.objects.filter(
            pipeline_version__in=V6_OR_LATER,
            downloads__contains=[{DOWNLOAD_GROUP: ASV_SEQUENCES_DOWNLOAD_GROUP}],
        )
        repaired = 0
        for analysis in analyses:
            changed = False
            for download in analysis.downloads:
                path = Path(download.get(PATH, ""))
                if (
                    download.get(DOWNLOAD_GROUP) != ASV_SEQUENCES_DOWNLOAD_GROUP
                    or not path.is_absolute()
                ):
                    continue
                # import_asv always reads the file from <results_dir>/<asv_folder>/.
                download[PATH] = str(asv_folder / path.name)
                changed = True
                self.stdout.write(f"{analysis.accession}: {path} -> {download[PATH]}")
            if changed:
                repaired += 1
                if not options["dry_run"]:
                    analysis.save(update_fields=["downloads"])

        verb = "Would repair" if options["dry_run"] else "Repaired"
        self.stdout.write(
            self.style.SUCCESS(
                f"{verb} {repaired} of {analyses.count()} analyses with ASV sequence downloads."
            )
        )
