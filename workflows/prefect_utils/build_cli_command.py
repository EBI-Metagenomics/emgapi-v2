import shlex
from typing import Optional, List


def cli_command(parts: List[Optional[str]]) -> str:
    """
    Construct a CLI command string from a list of parts.
    Uses `shlex.join`, but also allows for falsey parts to be ignored.
    This is helpful where you want to possibly specify a CLI flag, but its presnce should be dependent on python condition.

    Example:
    cli_command(["nextflow run my_pipe.nf", f"thing={value}", study.is_private and "--private-data", "-resume"])

    Might return:
    "nextflow run my_pipe.nf thing=private_data.csv --private-data -resume
    Or
    "nextflow run my_pipe.nf thing=public_data.csv -resume

    :param parts: List of command parts
    :return: A single string of the CLI command
    """
    non_null_parts = [p for p in parts if p not in [False, None]]
    return shlex.join(non_null_parts)
