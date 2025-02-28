from typing import List, Union

from prefect import task

import analyses.models


@task(
    log_prints=True,
)
def get_analyses_to_attempt(
    study: analyses.models.Study,
    for_experiment_type: analyses.models.WithExperimentTypeModel.ExperimentTypes,
) -> List[Union[str, int]]:
    """
    Determine the list of runs worth trying currently for this study.
    :param study: MGYS study to look for to-be-completed analyses in
    :param for_experiment_type: E.g. AMPLICON or WGS
    :return: List of analysis object IDs
    """
    study.refresh_from_db()
    analyses_worth_trying = (
        study.analyses.exclude_by_statuses(
            [
                analyses.models.Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
                analyses.models.Analysis.AnalysisStates.ANALYSIS_COMPLETED,
                analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED,
            ]
        )
        .filter(experiment_type=for_experiment_type.value)
        .order_by("id")
        .values_list("id", flat=True)
    )

    print(f"Got {len(analyses_worth_trying)} analyses to attempt")
    return analyses_worth_trying
