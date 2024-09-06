from dataclasses import dataclass
from typing import Any, Callable

from asgiref.sync import async_to_sync
from prefect import State, get_client
from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId
from pydantic import UUID4


async def get_logs_for_flow_run(flow_run_id: UUID4) -> str:
    async with get_client() as client:
        logs = await client.read_logs(
            log_filter=LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]))
        )
        return " ".join(log.message for log in logs)


@dataclass
class LoggedFlowRunResult:
    logs: str
    result: Any


def run_flow_and_capture_logs(flow: Callable, *args, **kwargs):
    """
    Run a prefect flow, and then pull the logs for it from the prefect server API.
    This is a tedious workaround for buggy behaviour in capturing prefect logs with pytest caplog.

    E.g.

    @flow
    def my_flow(widget="blue"):
        logger = get_run_logger()
        logger.info(f"The widget will be {widget}")
        return f"{widget.upper()} WIDGET"

    my_logged_flow = run_flow_and_capture_logs(my_flow, "red")
    assert "will be red" in my_logged_flow.logs
    assert my_logged_flow.result == "RED WIDGET"
    """
    state: State = flow(*args, return_state=True, **kwargs)
    logs = async_to_sync(get_logs_for_flow_run)(state.state_details.flow_run_id)
    return LoggedFlowRunResult(logs=logs, result=state.result())


async def run_async_flow_and_capture_logs(flow: Callable, *args, **kwargs):
    """
    Run an async prefect flow, and then pull the logs for it from the prefect server API.
    This is a tedious workaround for buggy behaviour in capturing prefect logs with pytest caplog.

    E.g.

    @flow
    async def my_flow(widget="blue"):
        logger = get_run_logger()
        await something()
        logger.info(f"The widget will be {widget}")
        return f"{widget.upper()} WIDGET"

    my_logged_flow = await run_async_flow_and_capture_logs(my_flow, "red")
    assert "will be red" in my_logged_flow.logs
    assert my_logged_flow.result == "RED WIDGET"
    """
    state: State = await flow(*args, return_state=True, **kwargs)
    logs = await get_logs_for_flow_run(state.state_details.flow_run_id)
    return LoggedFlowRunResult(logs=logs, result=state.result())
