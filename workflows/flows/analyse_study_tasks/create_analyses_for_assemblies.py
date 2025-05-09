from typing import List

from prefect import task

import analyses.models


@task(
    log_prints=True,
)
def create_analyses_for_assemblies(
    study: analyses.models.Study,
    pipeline: analyses.models.Analysis.PipelineVersions = analyses.models.Analysis.PipelineVersions.v6,
) -> List[analyses.models.Analysis]:
    """
    Get or create analysis objects for each assembly in the study.
    :param study: An MGYS study that already has assemblies to be analysed attached.
    :param pipeline: Pipeline version e.g. v6
    :return: List of matching/created analysis objects.
    """
    analyses_list = []
    for assembly in study.assemblies_assembly.all():
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study,
            sample=assembly.sample,
            assembly=assembly,
            ena_study=study.ena_study,
            pipeline_version=pipeline,
        )
        if created:
            print(
                f"Created analysis {analysis} for assembly {assembly.first_accession}"
            )
        analysis.inherit_experiment_type()
        analyses_list.append(analysis)
    return analyses_list
