from pathlib import Path

import django
import pytest

from workflows.data_io_utils.mgnify_v6_utils.amplicon import import_qc, import_taxonomy

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
        mgya.external_results_dir = f"analyses/{mgya.accession}"
        mgya.save()
        mgyas.append(mgya)

    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    mgyas[0].status[
        mg_models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    ] = True
    mgyas[1].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].save()
    mgyas[1].save()

    mgyas[0].results_dir = "/app/data/tests/amplicon_v6_output/SRR6180434"
    mgyas[0].save()

    import_qc(
        analysis=mgyas[0],
        dir_for_analysis=Path(mgyas[0].results_dir),
        allow_non_exist=False,
    )

    import_taxonomy(
        analysis=mgyas[0],
        dir_for_analysis=Path(mgyas[0].results_dir),
        source=mg_models.Analysis.TaxonomySources.SSU,
        allow_non_exist=False,
    )

    s = mgyas[0].study
    s.features.has_v6_analyses = True
    s.save()

    return mgyas
