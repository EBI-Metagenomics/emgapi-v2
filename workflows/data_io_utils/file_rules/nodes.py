import inspect
import logging
from pathlib import Path
from typing import List, Optional, TypeVar, Union

from pydantic import BaseModel, Field, model_validator

from workflows.data_io_utils.file_rules.base_rules import (
    DirectoryRule,
    FileRule,
    GlobRule,
)
from workflows.data_io_utils.file_rules.common_rules import (
    GlobHasFilesRule,
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)

__all__ = ["File", "Directory", "create_file", "create_directory"]

T = TypeVar("T", bound="File")


def create_file(
    base_dir: Path, *parts, rules: Optional[List[FileRule]] = None
) -> "File":
    """
    Factory function to create a File object with standard rules.

    This function replaces the class method approach with a more functional approach,
    which is generally preferred in Python when the method doesn't need to access instance attributes.
    It also makes the code more testable and easier to understand.

    Args:
        base_dir: Base directory path
        *parts: Additional path parts to join with base_dir
        rules: List of rules to apply (defaults to standard rules)

    Returns:
        File object
    """
    if rules is None:
        rules = [FileExistsRule, FileIsNotEmptyRule]

    return File(
        path=base_dir.joinpath(*parts),
        rules=rules,
    )


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

    def add_file(self, *parts, rules=None) -> None:
        """
        Create a File object within this directory.

        Args:
            *parts: Path parts relative to this directory
            rules: List of rules to apply to the file

        Returns:
            File object
        """
        self.files.append(create_file(self.path, *parts, rules=rules))

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


def create_directory(
    base_dir: Path,
    files: Optional[List[Union[File, tuple, str]]] = None,
    glob_rules: Optional[List[GlobRule]] = None,
    rules: Optional[List[DirectoryRule]] = None,
) -> Directory:
    """
    Factory function to create a Directory object with standard rules.

    This function replaces the class method approach with a more functional approach,
    which is generally preferred in Python when the method doesn't need to access instance attributes.
    It also makes the code more testable and easier to understand.

    The function handles different types of file specifications, making it flexible and easy to use.
    It also sets default rules for the directory and its files, ensuring consistent validation.

    Args:
        base_dir: Base directory path
        files: List of items to include in the directory. Can be:
            - File objects (used as-is)
            - Directory objects (used as-is)
            - Strings (converted to File objects with default rules)
            - Tuples of (filename, rules) where filename is a string and rules is a list of FileRule
        glob_rules: List of glob rules to apply
        rules: List of directory rules to apply

    Returns:
        Directory object
    """
    if files is None:
        files = []
    if glob_rules is None:
        glob_rules = [GlobHasFilesRule]
    if rules is None:
        rules = [DirectoryExistsRule]

    # Create the directory
    directory = Directory(
        path=base_dir,
        rules=rules,
        files=[],  # Start with an empty files list
        glob_rules=glob_rules,
    )

    for file_item in files:
        if isinstance(file_item, File):
            # If it's already a File or Directory object, use it as-is
            directory.files.append(file_item)
        elif isinstance(file_item, tuple) and len(file_item) == 2:
            # If it's a tuple of (filename, rules), create a File with those rules
            filename, file_rules = file_item
            directory.add_file(filename, rules=file_rules)
        elif isinstance(file_item, str):
            # If it's a string, create a File with default rules
            directory.add_file(file_item)
        else:
            raise ValueError(f"Unsupported file specification: {file_item}")

    return directory
