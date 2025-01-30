import shutil
from datetime import timedelta
from pathlib import Path

import django
from django.conf import settings
from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from workflows.prefect_utils.slurm_policies import (
    ResubmitWithCleanedNextflowIfFailedPolicy,
)

django.setup()

from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)


@flow(
    name="Run the assembler pipeline tests",
    log_prints=True,
    flow_run_name="Run the miassembler tests",
    task_runner=SequentialTaskRunner,
)
async def run_assembly_pipeline_tests():
    """
    It runs the miassembler pipeline test suite.
    """
    try:
        outdir = (
            Path(settings.EMG_CONFIG.slurm.default_workdir)
            / "miassembler"
            / "test-workdir"
        )

        # Remove any files in there, for example a failed previuos attempt
        # If the files are there, it could cause issues
        shutil.rmtree(outdir, ignore_errors=True)
        outdir.mkdir(parents=True, exist_ok=True)

        orchestrated_cluster_job = await run_cluster_job(
            name="Run the mi-assembler tests",
            command=(
                f"nextflow run ebi-metagenomics/miassembler -r dev "
                "-profile test,docker,arm "
                f"-resume "
                f"--samplesheet https://raw.githubusercontent.com/EBI-Metagenomics/miassembler/refs/heads/main/tests/samplesheet/test.csv "
                f"--outdir {outdir} "
                f"-ansi-log false "  # otherwise the logs in prefect/django are full of control characters
            ),
            expected_time=timedelta(hours=1),
            memory="500M",
            resubmit_policy=ResubmitWithCleanedNextflowIfFailedPolicy,
            # These policies control what happens when identical jobs are submitted in future,
            #   including when a flow crashes and is restarted.
            # This policy says that if an identical job is started in future, it won't actually start anything
            #   in slurm, unless the last identical job resulted in a FAILED slurm job, in which case we will
            #   start a new one to try again.
            working_dir=outdir,
            environment="ALL",  # copy env vars from the prefect agent into the slurm job
        )
        print(f"Running job - {orchestrated_cluster_job}")
    except ClusterJobFailedException as e:
        print(f"Something went wrong running pipeline, {e}")
