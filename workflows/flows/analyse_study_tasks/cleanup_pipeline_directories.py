import shutil
from pathlib import Path
from prefect import task


@task()
def delete_pipeline_workdir(workdir: Path):
    # The pipeline uses a directory as a work directory. This should be deleted on completion.
    # This function deletes the directory passed to it.
    shutil.rmtree(workdir)
