from typing import List

from prefect import task

import analyses.models


@task(
    log_prints=True,
)
def create_analyses(
    study: analyses.models.Study,
    for_experiment_type: analyses.models.WithExperimentTypeModel.ExperimentTypes,
    pipeline: analyses.models.Analysis.PipelineVersions = analyses.models.Analysis.PipelineVersions.v6,
) -> List[analyses.models.Analysis]:
    """
    Get or create analysis objects for each run in the study that matches the given experiment type.
    :param study: An MGYS study that already has runs to be analysed attached.
    :param for_experiment_type: E.g. AMPLICON or WGS
    :param pipeline: Pipeline version e.g. v6
    :return: List of matching/created analysis objects.
    """
    analyses_list = []
    for run in study.runs.filter(experiment_type=for_experiment_type):
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study,
            sample=run.sample,
            run=run,
            ena_study=study.ena_study,
            pipeline_version=pipeline,
        )
        if created:
            print(
                f"Created analyses {analysis} {analysis.run.first_accession} {analysis.run.experiment_type}"
            )
        analysis.inherit_experiment_type()
        analyses_list.append(analysis)
    return analyses_list
