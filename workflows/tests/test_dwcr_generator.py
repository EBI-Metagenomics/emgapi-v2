import pytest
import pathlib
from unittest.mock import patch
from collections import defaultdict
import pandas as pd


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

    mock_dict = defaultdict()
    mock_dict["SRR1111111"] = pd.DataFrame(
        {
            "RunID": "SRR1111111",
            "StudyID": "SRP2222222",
            "SampleID": "SAMN3333333",
            "seq_meth": "Illumina MiSeq",
            "decimalLatitude": 12,
            "decimalLongitude": 22,
            "depth": 0.12,
            "center_name": "Devonshire Building",
            "temperature": 12,
            "salinity": 0.12,
            "country": "United Kingdom",
            "collectionDate": "2025-05-25",
        },
        index=[0],
    )
    with patch(
        "mgnify_pipelines_toolkit.analysis.shared.dwc_summary_generator.get_all_ena_metadata_from_runs",
        return_value=(mock_dict),
    ):
        generate_dwc_ready_summary_for_pipeline_run(
            raw_reads_mgnify_study, amplicon_pipeline_outdir, completed_runs_filename
        )
