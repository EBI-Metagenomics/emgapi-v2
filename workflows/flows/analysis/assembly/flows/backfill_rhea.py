"""Populate molecule search from existing public ASAv6 downloads."""

from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from prefect import get_run_logger

import activate_django_first  # noqa

from analyses.models import Analysis
from analyses.schemas import MGnifyAnalysisDownloadFile
from workflows.flows.analysis.assembly.tasks.import_rhea import import_rhea_annotations
from workflows.prefect_utils.flows_utils import django_db_flow as flow


@flow
def backfill_rhea(study_accession: str | None = None, overwrite: bool = False) -> None:
    """Index existing public v6 assemblies, one analysis at a time.

    :param study_accession: Optional MGnify study restriction.
    :param overwrite: Replace existing annotations when true.
    """
    logger = get_run_logger()
    analyses = Analysis.public_objects.filter(
        assembly__isnull=False, pipeline_version__startswith="V6"
    )
    if study_accession:
        analyses = analyses.filter(study__accession=study_accession)
    if not overwrite:
        analyses = analyses.filter(molecule_index__isnull=True)
    failures = []
    for analysis in analyses.order_by("id").iterator(chunk_size=100):
        downloads = [
            d
            for d in analysis.downloads_as_objects
            if d.download_group == "functional_annotation.rhea_reactions"
        ]
        if not downloads:
            logger.warning("No Rhea download for %s", analysis.accession)
            continue
        try:
            download = MGnifyAnalysisDownloadFile.from_parent(analysis, downloads)[0]
            if not download.url:
                raise ValueError("Rhea download has no public URL")
            with TemporaryDirectory(prefix="rhea-") as directory:
                path = Path(directory) / "proteins2rhea.tsv.gz"
                with httpx.stream(
                    "GET", str(download.url), timeout=120, follow_redirects=True
                ) as response:
                    response.raise_for_status()
                    with path.open("wb") as output:
                        for chunk in response.iter_bytes():
                            output.write(chunk)
                count = import_rhea_annotations(analysis.id, path)
                logger.info("Indexed %s molecules for %s", count, analysis.accession)
        except Exception:
            logger.exception("Rhea import failed for %s", analysis.accession)
            failures.append(analysis.accession)
    if failures:
        raise RuntimeError(f"Rhea imports failed: {', '.join(failures)}")
