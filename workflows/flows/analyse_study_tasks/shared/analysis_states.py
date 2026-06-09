import analyses.models
from workflows.prefect_utils.flows_utils import django_db_task as task

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task(log_prints=True)
def mark_analysis_as_started(analysis: analyses.models.Analysis):
    # TODO: we should deprecated this and do inline (less tasks - less moving parts)
    analysis.mark_status(AnalysisStates.ANALYSIS_STARTED)


@task(log_prints=True)
def mark_analysis_as_failed(analysis: analyses.models.Analysis):
    # TODO: we should deprecated this and do inline (less tasks - less moving parts)
    analysis.mark_status(AnalysisStates.ANALYSIS_FAILED)
