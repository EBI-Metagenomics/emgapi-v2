import inspect
import logging
from pathlib import Path
from typing import List, Union

from pydantic import BaseModel, Field, model_validator, field_validator

from workflows.data_io_utils.file_rules.base_rules import (
    DirectoryRule,
    FileRule,
    GlobRule,
)
from workflows.data_io_utils.file_rules.common_rules import (
    FileExistsRule,
    FileIsNotEmptyRule,
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

    files: List[Union[File, tuple, str]] = Field(
        default_factory=list,
        description="File objects to specifically check in the directory",
    )
    subdirectories: List["Directory"] = Field(
        default_factory=list, description="Subdirectories"
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

    @field_validator("files", mode="before")
    @classmethod
    def validate_files(cls, files_input, info):
        """
        Validate and transform the files input.

        This validator handles different types of file specifications:
        - File objects (used as-is)
        - Directory objects (used as-is)
        - Strings (converted to File objects with default rules)
        - Tuples of (filename, rules) where filename is a string and rules is a list of FileRule

        :param files_input: The input files list
        :param info: ValidationInfo object

        :return: List of File objects

        :raises ValueError: If an unsupported file specification is provided
        """
        if files_input is None:
            return []

        processed_files = []
        for file_item in files_input:
            if isinstance(file_item, File):
                # If it's already a File or Directory object, use it as-is
                processed_files.append(file_item)
            elif isinstance(file_item, tuple) and len(file_item) == 2:
                # If it's a tuple of (filename, rules), create a File with those rules
                filename, file_rules = file_item
                # Get the path from the model being validated
                model_path = info.data.get("path")
                if model_path:
                    processed_files.append(
                        File(
                            path=model_path.joinpath(filename),
                            rules=file_rules,
                        )
                    )
            elif isinstance(file_item, str):
                # If it's a string, create a File with default rules
                model_path = info.data.get("path")
                if model_path:
                    processed_files.append(
                        File(
                            path=model_path.joinpath(file_item),
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        )
                    )
            elif isinstance(file_item, Path):
                model_path = info.data.get("path")
                if model_path:
                    processed_files.append(
                        File(
                            path=model_path / file_item,
                            rules=[FileExistsRule, FileIsNotEmptyRule],
                        )
                    )
            else:
                raise ValueError(f"Unsupported file specification: {file_item}")

        return processed_files

    def add_file(self, *parts, rules=None) -> None:
        """
        Create a File object within this directory.

        :params  *parts: Path parts relative to this directory
        :param   rules: List of rules to apply to the file
        :return: File object
        """
        if rules is None:
            rules = [FileExistsRule, FileIsNotEmptyRule]

        self.files.append(
            File(
                path=self.path.joinpath(*parts),
                rules=rules,
            )
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
