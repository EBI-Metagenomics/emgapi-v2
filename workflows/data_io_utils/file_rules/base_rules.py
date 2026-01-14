from pathlib import Path
from typing import Callable, Iterable

from pydantic import BaseModel, Field

__all__ = ["FileRule", "DirectoryRule", "GlobRule"]


class FileRule(BaseModel):
    rule_name: str = Field(..., description="Nice name of the rule to be applied")
    test: Callable[[Path], bool] = Field(
        ...,
        description="Function to call on file to check if rule passes",
        exclude=True,
    )


class DirectoryRule(FileRule): ...


class GlobRule(DirectoryRule):
    glob_pattern: str = Field(
        default="*", description="Glob pattern to match within the dir"
    )
    test: Callable[[Iterable[Path]], bool] = Field(
        ...,
        description="Function to call on the result of glob pattern to check it passes",
        exclude=True,
    )
