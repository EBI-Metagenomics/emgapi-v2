from prefect.runtime import flow_run


def ask_every_time_suspend_for_input_key():
    """
    Generate a flow-run input key that changes on each retry attempt.
    This avoids 409 conflicts when retrying flows that suspend and wait for input,
    by ensuring a new flow-run input is created for every attempt.
    """
    return f"suspend-{flow_run.id}-attempt-{flow_run.run_count}"
