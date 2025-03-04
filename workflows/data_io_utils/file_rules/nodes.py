import inspect
import logging
from pathlib import Path
from typing import List

from pydantic import BaseModel, Field, model_validator

from workflows.data_io_utils.file_rules.base_rules import (
    DirectoryRule,
    FileRule,
    GlobRule,
)

__all__ = ["File", "Directory"]


class File(BaseModel):
    """
    A data file from a pipeline output, say.
    """

    path: Path = Field(..., description="pathlib.Path pointer to the file")
    rules: List[FileRule] = Field(
        default_factory=list, description="List of rules to be applied", repr=False
    )

    @model_validator(mode="after")
    def passes_all_rules(self):
        failures = []
        for rule in self.rules:
            try:
                passes = rule.test(self.path)
            except Exception as e:
                logging.error(
                    f"Unexpected failure applying rule <<{rule.__class__.__name__}: {rule.rule_name}>> to {self.path}. Treating as rule failure. {e}"
                )
                failures.append(rule)
            else:
                if not passes:
                    failures.append(rule)
        if failures:
            raise ValueError(
                f"Rules {[f.rule_name for f in failures]} failed for {self.path}"
            )
        return self


class Directory(File):
    files: List[File] = Field(
        default_factory=list,
        description="File objects to specifically check in the directory",
    )
    rules: List[DirectoryRule] = Field(
        default_factory=list,
        description="List of rules to be applied to the directory path",
        repr=False,
    )
    glob_rules: List[GlobRule] = Field(
        default_factory=list,
        description="List of glob rules to be applied to the dir",
        repr=False,
    )

    @model_validator(mode="after")
    def passes_all_glob_rules(self):
        failures = []
        for rule in self.glob_rules:
            try:
                passes = rule.test(self.path.glob(rule.glob_patten))
            except Exception as e:
                logging.error(
                    f"Unexpected failure applying rule <<{rule.__class__.__name__}: {rule.rule_name}>> to files of {self}. Treating as rule failure. {e}"
                )
                failures.append(rule)
            else:
                if not passes:
                    failures.append(rule)
        if failures:
            for failure in failures:
                matched_failed = "\n\t ├─> ".join(
                    [str(p) for p in self.path.glob(failure.glob_patten)]
                )
                logging.warning(
                    f"Glob rule failure for {failure.rule_name}:"
                    f"\n\t {failure.glob_patten}"
                    f"\n\t ├─> {matched_failed}"
                    f"\n\t Test: {inspect.getsource(failure.test)}"
                )
            raise ValueError(
                f"Rules {[f.rule_name for f in failures]} failed for {self}"
            )
        return self
