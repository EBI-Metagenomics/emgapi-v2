import shutil
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent as _

from prefect import flow

from activate_django_first import EMG_CONFIG

from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy
from workflows.prefect_utils.slurm_flow import run_cluster_job


@flow(
    name="Nexflow hello-world example",
    log_prints=True,
)
def hello_nextflow(with_trace_flag: True):
    command = "nextflow run hello -ansi-log false"

    workdir = Path(EMG_CONFIG.slurm.default_workdir) / "hello-nextflow"

    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(exist_ok=True)

    if with_trace_flag:
        command += " -with-trace trace-hello.txt"
    else:
        # This is what nf-core~like pipelines handle the trace files
        nf_config = workdir / "test-config.conf"
        trace_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Write the configuration to the file
        nf_config.write_text(
            _(
                f"""
            trace {{
                enabled = true
                file    = "{workdir}/pipeline_info/execution_trace_{trace_timestamp}.txt"
            }}
            """
            )
        )

        command += f" -c {nf_config}"

    run_cluster_job(
        name="Runs the nextflow hello world pipeline",
        command=command,
        expected_time=timedelta(minutes=10),
        memory="100M",
        resubmit_policy=ResubmitAlwaysPolicy,
        working_dir=workdir,
        environment="ALL",  # copy env vars from the prefect agent into the slurm job
    )
