from typing import List, Union

from django.db.models import Q
from prefect import task

import analyses.base_models.with_experiment_type_models
import analyses.models
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy


@task(
    log_prints=True,
)
def get_analyses_to_attempt(
    study: analyses.models.Study,
    for_experiment_type: Union[
        analyses.base_models.with_experiment_type_models.WithExperimentTypeModel.ExperimentTypes,
        List[
            analyses.base_models.with_experiment_type_models.WithExperimentTypeModel.ExperimentTypes
        ],
    ],
    ena_library_strategy_policy: ENALibraryStrategyPolicy = ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
) -> List[Union[str, int]]:
    """
    Determine the list of runs worth trying currently for this study.
    :param study: MGYS study to look for to-be-completed analyses in
    :param for_experiment_type: E.g. AMPLICON or WGS.
    :param ena_library_strategy_policy: Optional policy for handling runs in the study that aren't labeled as for_experiment_type.
    :return: List of analysis object IDs
    """
    if isinstance(for_experiment_type, (list, set, tuple)):
        for_experiment_type_ = list(for_experiment_type)
    else:
        for_experiment_type_ = [for_experiment_type]
    study.refresh_from_db()
    analyses_worth_trying = study.analyses.exclude_by_statuses(
        [
            analyses.models.Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
            analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED,
            analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED,
        ]
    )

    if ena_library_strategy_policy == ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA:
        analyses_worth_trying = analyses_worth_trying.filter(
            experiment_type__in=[v.value for v in for_experiment_type_]
        )
    elif (
        ena_library_strategy_policy
        == ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES
    ):
        analyses_worth_trying = analyses_worth_trying.filter(
            Q(experiment_type__in=[v.value for v in for_experiment_type_])
            | Q(
                experiment_type=analyses.base_models.with_experiment_type_models.WithExperimentTypeModel.ExperimentTypes.UNKNOWN.value
            )
        )

    analyses_worth_trying = analyses_worth_trying.order_by("id").values_list(
        "id", flat=True
    )

    print(f"Got {len(analyses_worth_trying)} analyses to attempt")
    return analyses_worth_trying
