import pytest
import pathlib

from workflows.flows.analyse_study_tasks.shared.dwca_generator import (
    convert_dwcr_to_dwca,
)
from analyses.models import Study, Analysis


@pytest.mark.django_db(transaction=True)
def test_dwca_generator(
    amplicon_analysis_with_downloads,
    raw_reads_mgnify_study: Study,
    tmp_path,
    study_downloads,
):

    # TODO!
    dwcr_file = (
        pathlib.Path(raw_reads_mgnify_study.results_dir)
        / "PRJNA398089_closedref_SILVA-SSU_dwcready.csv"
    )

    # amplicon_analysis_with_downloads

    convert_dwcr_to_dwca(dwcr_file, Analysis.PipelineVersions.v6, tmp_path)
