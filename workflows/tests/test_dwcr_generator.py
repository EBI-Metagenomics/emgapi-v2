import pytest
import pathlib


from activate_django_first import EMG_CONFIG


from workflows.flows.analyse_study_tasks.shared.dwcr_generator import (
    generate_dwc_ready_summary_for_pipeline_run,
)
from analyses.models import Study


@pytest.mark.django_db(transaction=True)
def test_dwcr_generator(
    amplicon_analysis_with_downloads,
    raw_reads_mgnify_study: Study,
    prefect_harness,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
):
    amplicon_pipeline_outdir = pathlib.Path(
        amplicon_analysis_with_downloads.results_dir
    )

    # TODO: NEXT STEP IS MAKE A FILE CONTAINING COMPLETED RUNS TO REPLACE `"test"`, TESTING WON'T WORK LIKE THIS
    generate_dwc_ready_summary_for_pipeline_run(
        raw_reads_mgnify_study, amplicon_pipeline_outdir, completed_runs_filename
    )
