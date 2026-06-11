from prefect.runtime import flow_run


def ask_every_time_suspend_for_input_key():
    return f"suspend-{flow_run.id}-attempt-{flow_run.run_count}"
