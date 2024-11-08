from prefect import task

from analyses.models import Assembly, Analysis


@task(log_prints=True)
def task_mark_assembly_status(
    assembly: Assembly,
    status: Assembly.AssemblyStates,
    reason: str = None,
    unset_statuses: [Assembly.AssemblyStates] = None,
) -> None:
    """
    Logs and updates the status of a given assembly.
    :param assembly: The assembly object to update.
    :type assembly: Assembly
    :param status: The new status to assign to the assembly.
    :type status: One of Assembly.AssemblyStates
    :param reason: An optional reason for the status change, which will be recorded.
    :type reason: str, optional
    :param unset_statuses: An optional list of statuses to unset, if they are already set (e.g., [Assembly.AssemblyStates.ASSEMBLY_FAILED])
    :type unset_statuses: list of Assembly.AssemblyStates, optional
    :return: None
    :rtype: None
    :raises ValueError: If the status is not one of the predefined AssemblyStates.
    """
    if status not in Assembly.AssemblyStates.__dict__.values():
        raise ValueError(
            f"Invalid status '{status}'. Must be one of the predefined AssemblyStates."
        )

    print(f"Assembly {assembly} status is {status} now.")
    assembly.mark_status(status, reason=reason)
    for unset_status in unset_statuses or []:
        if assembly.status[unset_status]:
            assembly.mark_status(
                unset_status,
                set_status_as=False,
                reason=f"Explicitly unset when setting {status}",
            )


@task(log_prints=True)
def task_mark_analysis_status(
    analysis: Analysis,
    status: Analysis.AnalysisStates,
    reason: str = None,
    unset_statuses: [Analysis.AnalysisStates] = None,
) -> None:
    """
    Logs and updates the status of a given analysis.
    :param analysis: The Analysis object to update.
    :type analysis: Analysis
    :param status: The new status to assign to the analysis.
    :type status: One of Analysis.AnalysisStates
    :param reason: An optional reason for the status change, which will be recorded.
    :type reason: str, optional
    :param unset_statuses: An optional list of statuses to unset, if they are already set (e.g., [Analysis.AnalysisStates.ANALYSIS_FAILED])
    :type unset_statuses: list of Analysis.AnalysisStates, optional
    :return: None
    :rtype: None
    :raises ValueError: If the status is not one of the predefined AnalysisStates.
    """
    if status not in Analysis.AnalysisStates.__dict__.values():
        raise ValueError(
            f"Invalid status '{status}'. Must be one of the predefined AnalysisStates."
        )

    print(f"Analysis {analysis} status is {status} now.")
    analysis.mark_status(status, reason=reason)
    for unset_status in unset_statuses or []:
        if analysis.status[unset_status]:
            analysis.mark_status(
                unset_status,
                set_status_as=False,
                reason=f"Explicitly unset when setting {status}",
            )
