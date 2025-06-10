from typing import List

from django.db.models import Q
from prefect import task

import analyses.models
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy


@task(
    log_prints=True,
)
def create_analyses(
    study: analyses.models.Study,
    for_experiment_type: analyses.models.WithExperimentTypeModel.ExperimentTypes,
    pipeline: analyses.models.Analysis.PipelineVersions = analyses.models.Analysis.PipelineVersions.v6,
    ena_library_strategy_policy: ENALibraryStrategyPolicy = ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
) -> List[analyses.models.Analysis]:
    """
    Get or create analysis objects for each run in the study that matches the given experiment type.
    :param study: An MGYS study that already has runs to be analysed attached.
    :param for_experiment_type: E.g. AMPLICON or WGS
    :param pipeline: Pipeline version e.g. v6
    :param ena_library_strategy_policy: Optional policy for handling runs in the study that aren't labeled as for_experiment_type.
    :return: List of matching/created analysis objects.
    """
    analyses_list = []
    runs = study.runs
    if ena_library_strategy_policy == ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA:
        runs = runs.filter(experiment_type=for_experiment_type.value)
    elif (
        ena_library_strategy_policy
        == ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES
    ):
        runs = runs.filter(
            Q(experiment_type=for_experiment_type.value)
            | Q(
                experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.UNKNOWN.value
            )
        )
    for run in runs:
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study,
            sample=run.sample,
            run=run,
            ena_study=study.ena_study,
            pipeline_version=pipeline,
            is_private=run.is_private,
            webin_submitter=run.webin_submitter,
        )
        if created:
            print(
                f"Created analyses {analysis} {analysis.run.first_accession} {analysis.run.experiment_type}"
            )
        analysis.inherit_experiment_type()
        analyses_list.append(analysis)
    return analyses_list
