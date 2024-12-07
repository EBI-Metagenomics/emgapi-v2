import django
import pytest

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)

django.setup()

import analyses.models as mg_models

versions = {"metaspades": "3.15.5", "spades": "3.15.5", "megahit": "1.2.9"}


@pytest.fixture
def raw_read_analyses(raw_read_run):
    mgyas = []
    for run in raw_read_run:
        mgya, _ = mg_models.Analysis.objects_and_annotations.get_or_create(
            run=run,
            sample=run.sample,
            study=run.study,
            ena_study_id=run.ena_study_id,
        )
        mgya.annotations[mg_models.Analysis.PFAMS] = [
            {"count": 1, "description": "PFAM1"}
        ]
        mgya.results_dir = f"analyses/{mgya.accession}"
        mgya.save()
        mgyas.append(mgya)

    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    mgyas[1].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].save()
    mgyas[1].save()

    mgyas[0].add_download(
        DownloadFile(
            file_type=DownloadFileType.TSV,
            download_type=DownloadType.FUNCTIONAL_ANALYSIS,
            long_description="Some PFAMs that were found",
            short_description="PFAM table",
            alias=f"PFAMS_{mgyas[0].accession}.tsv",
            path="functional/pfam/annos.tsv",
        )
    )

    return mgyas
