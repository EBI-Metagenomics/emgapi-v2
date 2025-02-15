from typing import List, Union

from prefect import task

import analyses.models


@task(
    log_prints=True,
)
def get_assemblies_to_attempt(study: analyses.models.Study) -> List[Union[str, int]]:
    """
    Determine the list of assemblies worth trying currently for this study.
    :param study:
    :return:
    """
    study.refresh_from_db()
    assemblies_worth_trying = study.assemblies_reads.exclude_by_statuses(
        [
            analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
            analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
            analyses.models.Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
        ]
    ).values_list("id", flat=True)
    print(f"Assemblies worth attempting are: {assemblies_worth_trying}")
    return assemblies_worth_trying
