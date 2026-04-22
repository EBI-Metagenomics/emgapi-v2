import pytest

from analyses.base_models.with_experiment_type_models import WithExperimentTypeModel
from analyses.models import Analysis
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy
from workflows.flows.analyse_study_tasks.shared.get_analyses_to_attempt import (
    get_analyses_to_attempt,
)


@pytest.mark.django_db
def test_get_analyses_to_attempt_filters_to_requested_pipeline_version(
    raw_reads_mgnify_study, raw_reads_mgnify_sample, raw_read_run
):
    sample = raw_reads_mgnify_sample[0]
    run = raw_read_run[0]

    analysis_v6 = Analysis.objects.create(
        accession="MGYA00002001",
        study=raw_reads_mgnify_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.AMPLICON,
        pipeline_version=Analysis.PipelineVersions.v6,
    )
    analysis_v6_1 = Analysis.objects.create(
        accession="MGYA00002002",
        study=raw_reads_mgnify_study,
        sample=sample,
        run=run,
        ena_study=raw_reads_mgnify_study.ena_study,
        experiment_type=WithExperimentTypeModel.ExperimentTypes.AMPLICON,
        pipeline_version=Analysis.PipelineVersions.v6_1,
    )

    analyses_to_attempt = get_analyses_to_attempt.fn(
        raw_reads_mgnify_study,
        for_experiment_type=WithExperimentTypeModel.ExperimentTypes.AMPLICON,
        pipeline=Analysis.PipelineVersions.v6_1,
        ena_library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
    )

    assert list(analyses_to_attempt) == [analysis_v6_1.id]
    assert analysis_v6.id not in analyses_to_attempt
