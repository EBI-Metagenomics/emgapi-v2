from prefect import task
from prefect_shell import ShellOperation


@task(
    retries=2,
    task_run_name="run shell command {command}",
)
def run_shell_command(command, env=None, workdir=None):
    cmd = ShellOperation(
        commands=[command], env=env or {}, working_dir=workdir, stream_output=True
    )
    result = cmd.run()
    return result
