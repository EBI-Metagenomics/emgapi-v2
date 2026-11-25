import os
from typing import Any, Dict, Optional, Union


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
