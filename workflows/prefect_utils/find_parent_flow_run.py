import logging

from prefect import get_client
from prefect.client.schemas import FlowRun
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterId

from analyses.management.commands.import_v5_analysis import logger


def find_parent_flow_run(subflow_run: FlowRun) -> FlowRun | None:
    if not subflow_run.parent_task_run_id:
        logger.warning(f"Subflow {subflow_run.id} has no dummy parent task run")
        return None
    with get_client(sync_client=True) as client:
        flow_runs_with_parent_task = client.read_flow_runs(
            task_run_filter=TaskRunFilter(
                id=TaskRunFilterId(any_=[subflow_run.parent_task_run_id])
            )
        )
    try:
        return flow_runs_with_parent_task[0]
    except IndexError:
        logging.warning(
            f"No flow run found with dummy parent task id {subflow_run.parent_task_run_id}"
        )
        return None


def find_parent_flow_runs_recursively(subflow_run: FlowRun) -> list[FlowRun]:
    top_most_flow_run_known = subflow_run
    parent_flow_runs = []

    with get_client(sync_client=True) as client:
        while True:
            flow_run = client.read_flow_run(top_most_flow_run_known.id)
            parent_flow_run = find_parent_flow_run(flow_run)
            if not parent_flow_run:
                break
            top_most_flow_run_known = parent_flow_run
            parent_flow_runs.append(parent_flow_run)

    return parent_flow_runs
