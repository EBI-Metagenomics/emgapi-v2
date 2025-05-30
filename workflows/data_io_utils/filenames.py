from __future__ import annotations

from pathlib import Path

from django.utils.text import slugify as djangoslugify


def file_path_shortener(
    filepath: str | Path,
    shorten_dirs_to: int = 1,
    shorten_name_to: int = 10,
    slugify: bool = False,
) -> str:
    """
    Take a potentially long filepath, and make a somewhat meaningful short version of it for printing.
    Useful for e.g. naming cluster jobs or prefect flows.

    :param filepath: Full or relative path to file, e.g. /nfs/public/my_file.csv
    :param shorten_dirs_to: Limit directory names to this number of characters.
    :param shorten_name_to: Limit file names to this number of characters.
    :param slugify: If True, slugify the file name as well, suitable for URLs etc.

    Examples:
        shorten_dirs_to=1
        shorten_name_to=10
        /nfs/production/long/path/to/nested/directory/my_big_file_name_is_long.csv
            -> /n/p/l/p/t/n/d/m..ong.csv

        shorten_dirs_to=3
        shorten_name_to=20
        /nfs/production/long/path/to/nested/directory/my_big_file_name_is_long.csv
            -> /nfs/pro/lon/pat/to/nes/dir/my_big_file_name_is_long.csv
    """
    entire_path = Path(filepath)
    short_name = entire_path.name

    if len(short_name) > shorten_name_to:
        short_name = short_name[0] + ".." + short_name[-(shorten_name_to - 3) :]

    short_path = ""
    for dir in entire_path.parts[:-1]:
        short_path += dir[:shorten_dirs_to] + ("/" if dir != "/" else "")
    short_path += short_name

    if slugify:
        slugified = djangoslugify(short_path.replace("/", "_").replace(".", "_"))
        while "__" in slugified:
            # recursively remove multiple-underscores because they upset nextflow regex
            slugified = slugified.replace("__", "_")
        return slugified

    return short_path


def accession_prefix_separated_dir_path(accession: str, *chars_per_dir_level: int):
    """
    Builds a multi-level directory path for an accession, where each level of the dir path is the accession
    truncated to a certain number of characters.

    E.g.: PRJ123456 -> PRJ123/PRJ1234/PRJ123456
    This is commonly used to prevent any single dir having a large number of subdirs.

    :param accession: the full accession e.g. PRJ123456
    :*chars_per_dir_level: the number of characters to truncate to at each level, e.g. 6,7

    Note that the full accession is ALWAYS to the end, as a dir, AFTER the levels specified.
    E.g. accession_prefix_separated_dir_path(PRJ123456, 5, 6) -> PRJ12/PRJ123/PRJ123456
    But also accession_prefix_separated_dir_path(PRJ123456, 5, 6, 999) -> PRJ12/PRJ123/PRJ123456/PRJ123456

    If any chars_per_dir_level is a negative int, it truncates from the end instead:
    e.g. accession_prefix_separated_dir_path(PRJ123456, -6, -3) -> PRJ/PRJ123/PRJ123456

    Returns
    -------
    A non-pure path for the dir hierarchy.

    """
    path = Path()
    for chars_at_level in chars_per_dir_level:
        assert not chars_at_level == 0
        path = path / Path(accession[:chars_at_level])
    return path / Path(accession)


def next_enumerated_subdir(
    parent_dir: Path, mkdirs: bool = False, pad: int = 4
) -> Path:
    """
    Make a number-named folder inside dir. E.g. given:
    parent_dir/
      001/
        my_file.txt

    next_enumerated_subdir(dir) -> /path/to/dir/002

    :param parent_dir: Path to the parent dir
    :param mkdirs: Bool, which if true will also attempt to make the new enumerated dir and its parents
    :param pad: How many characters long should the subdir be. E.g. 4 > 0001/
    :return: Path to the next enumerated subdir
    """
    if not mkdirs and not parent_dir.is_dir():
        raise ValueError(
            f"The directory {parent_dir} does not exist or is not a directory."
        )

    if pad < 1:
        raise ValueError(f"Pad length {pad} must be greater than or equal to 1.")

    if mkdirs:
        parent_dir.mkdir(parents=True, exist_ok=True)

    if not parent_dir.is_dir():
        raise FileNotFoundError(
            f"The directory {parent_dir} does not exist or is not a directory."
        )

    max_int_dir = 0
    glob_pattern = "[0-9]" * pad
    for item in parent_dir.glob(glob_pattern):
        if int(item.name) > max_int_dir:
            max_int_dir = int(item.name)

    next_int_dir = parent_dir / f"{max_int_dir + 1:0{pad}d}"

    if mkdirs:
        next_int_dir.mkdir(parents=True, exist_ok=True)
    return next_int_dir


def trailing_slash_ensured_dir(dir_path: Path | str) -> str:
    d = dir_path
    if isinstance(d, Path):
        d = str(d.as_posix())
    return str(d).rstrip("/") + "/"
