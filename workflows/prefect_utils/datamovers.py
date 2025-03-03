from datetime import timedelta

from prefect import flow
from prefect.runtime import flow_run

import activate_django_first  # noqa

from workflows.data_io_utils.filenames import file_path_shortener
from workflows.prefect_utils.slurm_flow import EMG_CONFIG, run_cluster_job
from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy


def move_data_flow_name() -> str:
    source = flow_run.parameters["source"]
    target = flow_run.parameters["target"]
    return f"Move data: {file_path_shortener(source, 1, 10)} > {file_path_shortener(target, 1, 10)}"


@flow(flow_run_name=move_data_flow_name)
def move_data(
    source: str,
    target: str,
    move_command: str = "cp",
    make_target: bool = True,
    **kwargs,
):
    """
    Move files on the cluster filesystem.
    This uses a slurm job running on the datamover partition.

    :param source: fully qualified path of the source location (file or folder)
    :param target: fully qualified path of the target location (file or folder)
    :param move_command: tool command for the move. Default is `cp`, but could be `mv` or `rsync` etc.
    :param make_target: mkdir the target location path before copying.
    :param kwargs: Other keywords to pass to run_cluster_job
        (e.g. expected_time, memory, or other slurm job-description parameters)
    :return: Job ID of the datamover job.
    """
    expected_time = kwargs.pop("expected_time", timedelta(hours=2))
    memory = kwargs.pop("memory", "1G")

    if "environment" not in kwargs:
        kwargs["environment"] = {}

    if make_target:
        move_command = f'mkdir -p "{target}" && ' + move_command

    return run_cluster_job(
        name=f"Move {file_path_shortener(source)} to {file_path_shortener(target)}",
        command=f'{move_command} "{source}" "{target}"',
        expected_time=expected_time,
        memory=memory,
        resubmit_policy=ResubmitAlwaysPolicy,
        partitions=[EMG_CONFIG.slurm.datamover_paritition],
        **kwargs,
    )
