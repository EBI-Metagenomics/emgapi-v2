import logging
from pathlib import Path
from typing import Type, Iterable, Hashable

from pydantic import BaseModel, ValidationError

from workflows.data_io_utils.csv.csv_comment_handler import (
    CommentAwareDictReader,
    CSVDelimiter,
)
from workflows.data_io_utils.file_rules.base_rules import FileRule


def generate_csv_schema_file_rule(
    row_schema: Type[BaseModel],
    delimiter: CSVDelimiter = CSVDelimiter.COMMA,
    none_values: Iterable[Hashable] = None,
    allow_trailing_delimiters: bool = True,
) -> FileRule:
    """
    Generate a FileRule in which the rule test checks if the CSV file at path follows the specified schema.
    :param row_schema: A Pydantic model to validate each CSV row against
    :param delimiter: Field separator for CSV file. E.g. CSVDelimiter.COMMA
    :param none_values: Optional list of values that should be parsed to None, e.g. ["", "NA"]
    :param allow_trailing_delimiters: Whether to allow (ignore) trailing delimiters after the final dataful column
    :return: A FileRule that can be applied to any path
    """
    schema_name = row_schema.__name__

    def tester(path: Path):
        with path.open("r") as f:
            reader = CommentAwareDictReader(
                f, delimiter=delimiter, none_values=none_values
            )
            rows_count = 0
            try:
                for row in reader:
                    if allow_trailing_delimiters:
                        # sometimes TSVs may have a trailing tab on data rows.
                        # it is probably strictly invalid, but sometimes seen.
                        if None in row and row[None] in [
                            "",
                            delimiter,
                            [],
                            [""],
                            [delimiter],
                        ]:
                            row.pop(None, None)
                    row_schema.model_validate(row)
                    rows_count += 1
            except ValidationError as e:
                logging.info(f"Validation failed on row {rows_count + 1}")
                logging.error(e)
                return False
            else:
                logging.info(
                    f"{rows_count} rows validated for {path} against {schema_name}"
                )
        return True

    return FileRule(
        rule_name=f"CSV should follow {schema_name} schema",
        test=tester,
    )
