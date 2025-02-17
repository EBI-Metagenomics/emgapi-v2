from prefect import task

import analyses.models

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task(log_prints=True)
def mark_analysis_as_started(analysis: analyses.models.Analysis):
    analysis.mark_status(AnalysisStates.ANALYSIS_STARTED)


@task(log_prints=True)
def mark_analysis_as_failed(analysis: analyses.models.Analysis):
    analysis.mark_status(AnalysisStates.ANALYSIS_FAILED)
