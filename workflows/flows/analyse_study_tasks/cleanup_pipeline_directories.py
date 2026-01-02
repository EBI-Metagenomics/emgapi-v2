import shutil
from pathlib import Path
from prefect import task
from typing import List, Union
import analyses.models

AnalysisStates = analyses.models.Analysis.AnalysisStates


@task()
def delete_pipeline_workdir(workdir: Path):
    # The pipeline uses a directory as a work directory. This should be deleted on completion.
    # This function deletes the directory passed to it.
    shutil.rmtree(workdir)


@task()
def delete_study_nextflow_workdir(
    study_workdir: Path,
    analyses_to_attempt: List[Union[str, int]],
):
    # delete work directory
    # check for failed analyses first AnalysisStates.ANALYSIS_FAILED
    failed_analyses = (
        analyses.models.Analysis.objects.select_related("run")
        .filter(
            id__in=analyses_to_attempt,
            run__metadata__fastq_ftps__isnull=False,
            status__equals=AnalysisStates.ANALYSIS_FAILED,
        )
        .order_by("id")
        .values_list("id", flat=True)
    )

    if not failed_analyses:
        shutil.rmtree(study_workdir)
