from pathlib import Path
from unittest.mock import patch

import pytest
from django.core.management import call_command

from analyses.models import Biome, Run, Analysis
from workflows.flows.analyse_study_tasks.import_completed_amplicon_analyses import (
    import_completed_analysis,
)


@pytest.fixture
@patch(
    "workflows.flows.analyse_study_tasks.import_completed_amplicon_analyses.copy_amplicon_pipeline_results"
)
def amplicon_analysis_with_downloads(
    mock_copy_flow, raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    study = raw_reads_mgnify_study

    run = Run.objects.create(
        ena_accessions=["SRR1111111"],
        study=study,
        ena_study=study.ena_study,
        sample=sample,
        experiment_type=Run.ExperimentTypes.AMPLICON,
        metadata={
            Run.CommonMetadataKeys.FASTQ_FTPS: ["ftp://example.org/SRR1111111.fastq"]
        },
    )

    analysis = Analysis.objects.create(
        ena_study=study.ena_study,
        study=study,
        experiment_type=Run.ExperimentTypes.AMPLICON,
        sample=sample,
        run=run,
    )
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)

    import_completed_analysis.fn(
        Path("/app/data/tests/amplicon_v6_output/"), [analysis]
    )


@pytest.mark.dev_data_maker
@pytest.mark.django_db(transaction=True)
def test_make_dev_data(
    top_level_biomes,
    assemblers,
    raw_read_analyses,
    mgnify_assemblies_completed,
    amplicon_analysis_with_downloads,
):
    """
    Dummy test that just sets up fixtures and dumps them to JSON for using as dev data.
    """

    assert Biome.objects.count() == 4

    call_command("dumpdata", "-o", "dev-db.json", "--indent", "2")
