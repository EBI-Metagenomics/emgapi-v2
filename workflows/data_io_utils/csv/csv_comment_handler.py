import csv

from emgapiv2.enum_utils import FutureStrEnum


def move_file_pointer_past_comment_lines(
    f, comment_char: str = "#", delimiter: str = ","
):
    """
    Take a file point, and move the current read pointer to the start of meaninful content.
    This skips over any pure comment lines (like "# Created by MGnify")
    and past any leading comment chars on a col header line like the "# " in "# id   name    count"
    :param f: File-like object.
    :param comment_char: Character used to start comment lines.
    :param delimiter: Delimiter used between fields.
    """
    f.seek(0)
    position_of_last_comment_line_with_delimiter = None
    chars_to_start_of_content_on_last_comment_line_with_delimiter = 0

    while True:
        pos = f.tell()
        line = f.readline()
        if not line:  # End of file
            break

        if line.startswith(comment_char):
            # a comment line which may either be a generic comment, or a commented header
            # e.g. `# created on: 1 January`
            # or `# id  name    count`
            if delimiter in line:
                # then it is probably a commented column header
                position_of_last_comment_line_with_delimiter = pos
                line_without_leading_comment_and_space = line.lstrip(
                    comment_char
                ).lstrip()
                chars_to_start_of_content_on_last_comment_line_with_delimiter = len(
                    line
                ) - len(line_without_leading_comment_and_space)
        else:
            # Stop seeking once a non-comment line is encountered
            # this may be a data line `1    bacteria    3`
            # or an uncommented header line `id     name    count`
            break

    if position_of_last_comment_line_with_delimiter is not None:
        # Seek back to the last commented line that was probably a header
        f.seek(
            position_of_last_comment_line_with_delimiter
            + chars_to_start_of_content_on_last_comment_line_with_delimiter
        )
    else:
        # Seek back start of first non-comment line
        f.seek(pos)


class CSVDelimiter(FutureStrEnum):
    COMMA = ","
    TAB = "\t"


class CommentAwareDictReader(csv.DictReader):
    """
    Like csv.DictReader, but handles CSV/TSV files where the leading line(s) may be comments rather than headers.
    """

    def __init__(
        self,
        f,
        delimiter: CSVDelimiter = CSVDelimiter.COMMA,
        comment_char="#",
        **kwargs
    ):
        """
        Initialize the CommentAwareDictReader.
        :param f: File-like object.
        :param delimiter: Delimiter used in the file.
        :param comment_char: Character indicating comment lines (col header line may be a comment line or not)
        :param kwargs: Additional arguments passed to DictReader.
        """
        self.delimiter = delimiter
        self.comment_char = comment_char

        # Adjust the file pointer to the lines after any meaningless comments (i.e. those that aren't a header row)
        move_file_pointer_past_comment_lines(f, self.comment_char, self.delimiter)
        super().__init__(f, delimiter=delimiter, **kwargs)
