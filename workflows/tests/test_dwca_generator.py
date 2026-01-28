import pytest

from workflows.flows.analyse_study_tasks.shared.dwca_generator import (
    convert_dwcr_to_dwca,
)
from analyses.models import Analysis


@pytest.mark.django_db(transaction=True)
def test_dwca_generator(
    amplicon_analysis_with_downloads,
    tmp_path,
    study_downloads,
    prefect_harness,
    mgnify_study_full_metadata,
):
    study = amplicon_analysis_with_downloads.study

    # dwcr_file = (
    #     study.results_dir_path / "PRJNA398089_closedref_SILVA-SSU_dwcready.csv"
    # )

    # amplicon_analysis_with_downloads

    convert_dwcr_to_dwca(
        study.accession, Analysis.ExperimentTypes.AMPLICON, Analysis.PipelineVersions.v6
    )

    # dwca_eml_path = study.results_dir_path / "dwca" / "eml.xml"

    # assert dwca_eml_path.exists()
    # assert dwca_eml_path.read_text().startswith("<eml:eml")
