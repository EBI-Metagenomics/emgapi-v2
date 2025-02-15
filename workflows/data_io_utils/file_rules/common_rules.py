import math

from workflows.data_io_utils.file_rules.base_rules import (
    DirectoryRule,
    FileRule,
    GlobRule,
)

FileExistsRule = FileRule(
    rule_name="File should exist",
    test=lambda f: f.is_file(),
)
FileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty",
    test=lambda f: f.stat().st_size > 0,
)
DirectoryExistsRule = DirectoryRule(
    rule_name="Directory should exist",
    test=lambda f: f.is_dir(),
)
GlobHasFilesRule = GlobRule(
    rule_name="Dir should have at least one file",
    glob_patten="*",
    test=lambda matches: len(list(matches)) > 0,
)


class __GlobHasFilesCountMetaclass(type):
    def __getitem__(cls, expected_count):
        if isinstance(expected_count, slice):
            # A range of file counts are acceptable.
            if expected_count.step not in [None, 1]:
                raise ValueError(
                    "GlobHasFilesCount can only be used with a min and max number of files."
                )
            min_expected = expected_count.start or 0
            max_expected = expected_count.stop or math.inf

            test = (  # noqa: E731
                lambda matches: min_expected <= len(list(matches)) <= max_expected
            )
        elif isinstance(expected_count, int):
            # Assume a single expected_count means an exact match
            test = lambda matches: len(list(matches)) == expected_count  # noqa: E731
        else:
            raise TypeError("expected_count must be int or slice.")

        return GlobRule(
            rule_name=f"Glob should have {repr(expected_count)} file(s)",
            glob_patten="*",
            test=test,
        )


class GlobHasFilesCountRule(metaclass=__GlobHasFilesCountMetaclass):
    """
    A glob rule for how many nodes at a path must be present.
    Specify the expected counts via slicing:

    E.g.
    glob_rules=[GlobHasFilesCountRule[2]]  # The directory/* must give exactly 2 files
    glob_rules=[GlobHasFilesCountRule[:10]]  # The directory/* must have at most 10 files
    glob_rules=[GlobHasFilesCountRule[3:]]  # The directory/* must have at least 3 files
    glob_rules=[GlobHasFilesCountRule[2:4]]  # The directory/* must have either 2, 3 or 4 files
    """

    def __init__(self):
        raise NotImplementedError(
            "GlobHasFilesCountRule cannot be initialized. Use it via slicing instead."
        )
