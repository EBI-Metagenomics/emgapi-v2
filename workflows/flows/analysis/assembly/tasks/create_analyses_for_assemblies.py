from typing import List

from prefect import task, get_run_logger

import analyses.models


@task
def create_analyses_for_assemblies(
    study_id: int,
    assemblies_accessions: list[str],
    pipeline: analyses.models.Analysis.PipelineVersions = analyses.models.Analysis.PipelineVersions.v6,
) -> List[analyses.models.Analysis]:
    """
    Get or create analysis objects for each assembly in the study.
    :param study_id: An analysis study id.
    :param assemblies_accessions: List of assembly accessions to be analysed.
    :param pipeline: Pipeline version e.g. v6
    :return: List of matching/created analysis objects.
    """
    logger = get_run_logger()

    study: analyses.models.Study = analyses.models.Study.objects.get(id=study_id)
    analyses_list = []
    # Use __overlap to find assemblies where ena_accessions (JSONField array) has any match
    for assembly in study.assemblies_assembly.filter(
        ena_accessions__overlap=assemblies_accessions
    ).select_related("sample"):
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study,
            sample=assembly.sample,
            assembly=assembly,
            ena_study=study.ena_study,
            pipeline_version=pipeline,
        )
        if created:
            logger.info(
                f"Created analysis {analysis} for assembly {assembly.first_accession}"
            )
        analysis.inherit_experiment_type()
        analyses_list.append(analysis)
    return analyses_list
