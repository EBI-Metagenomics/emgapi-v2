import os
from typing import Any, Dict, Optional, Union

from prefect import get_client
from prefect.exceptions import ObjectNotFound
from prefect.runtime import flow_run


def get_prefect_worker_type() -> Optional[str]:
    with get_client(sync_client=True) as client:
        try:
            root_flow_run = client.read_flow_run(flow_run.root_flow_run_id)
            return root_flow_run.labels.get("prefect.worker.type", None)
        except ObjectNotFound:
            return None


def make_environment(
    environment: Optional[Union[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    if environment is None or isinstance(environment, str):
        if environment is None or "all" in environment.casefold():
            return os.environ.copy()

        if "none" in environment.casefold() or "nil" in environment.casefold():
            return {
                k: v for k, v in os.environ.items() if k.startswith(("SLURM_", "SPANK"))
            }

        return {k: v for k, v in os.environ.items() if k in environment}
    else:
        return environment
