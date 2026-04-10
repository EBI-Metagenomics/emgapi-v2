"""
Pre-commit hook: activate_django_first must be imported before any models import.

Checks that every top-level ``*.models`` import is preceded by an
``activate_django_first`` import in the same file.
"""

import ast
import sys
from pathlib import Path


def check_file(path: str) -> list[str]:
    """
    Parse a Python file and return a list of violation messages.

    :param path: Path to the Python file to check.
    :return: List of error strings (empty if the file is clean).
    """
    source = Path(path).read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    activate_line: int | None = None
    errors: list[str] = []

    for node in tree.body:  # tree.body is in source order
        if isinstance(node, ast.ImportFrom):
            if node.module == "activate_django_first":
                activate_line = node.lineno
            elif node.module and ".models" in node.module:
                if activate_line is None:
                    errors.append(
                        f"{path}:{node.lineno}: models imported without activate_django_first"
                    )
                elif node.lineno < activate_line:
                    errors.append(
                        f"{path}:{node.lineno}: models imported before activate_django_first (line {activate_line})"
                    )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "activate_django_first":
                    activate_line = node.lineno
                elif ".models" in alias.name:
                    if activate_line is None:
                        errors.append(
                            f"{path}:{node.lineno}: models imported without activate_django_first"
                        )
                    elif node.lineno < activate_line:
                        errors.append(
                            f"{path}:{node.lineno}: models imported before activate_django_first (line {activate_line})"
                        )

    return errors


if __name__ == "__main__":
    violations = [error for path in sys.argv[1:] for error in check_file(path)]
    if violations:
        print("\n".join(violations))
        sys.exit(1)
