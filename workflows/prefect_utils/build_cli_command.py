import shlex
from typing import Optional, List, Union


def cli_command(parts: List[Optional[Union[str, tuple[str]]]]) -> str:
    """
    Construct a CLI command string from a list of parts.
    Uses `shlex.join`, but also allows for falsey parts to be ignored.
    This is helpful where you want to possibly specify a CLI flag, but its presence should be dependent on python condition.

    Example:
    cli_command(["nextflow", "run", "my_pipe.nf", f"thing={value}", study.is_private and "--private-data", "-resume", ("-r", "main")])

    Might return:
    "nextflow run my_pipe.nf thing=private_data.csv --private-data -resume -r main
    Or
    "nextflow run my_pipe.nf thing=public_data.csv -resume

    :param parts: List of command parts
    :return: A single string of the CLI command
    """
    non_null_parts = [p for p in parts if p not in [False, None]]
    expanded_parts = [
        str(p)
        for possibly_compound_part in non_null_parts
        for p in (
            possibly_compound_part
            if isinstance(possibly_compound_part, tuple)
            else (possibly_compound_part,)
        )
    ]
    return shlex.join(expanded_parts)
