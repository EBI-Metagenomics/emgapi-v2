from pathlib import Path

import pytest

from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    file_path_shortener,
    next_enumerated_subdir,
)


def test_file_path_shortener():
    assert file_path_shortener("my_file.csv", 1, 20) == "my_file.csv"

    assert file_path_shortener("my_file.csv", 2, 5) == "m..sv"
    assert file_path_shortener("my_file.csv", 2, 6) == "m..csv"

    assert file_path_shortener("/path/to/my_file.csv", 1, 11) == "/p/t/my_file.csv"
    assert file_path_shortener("/path/to/my_file.csv", 2, 10) == "/pa/to/m..ile.csv"

    assert (
        file_path_shortener("/path/to/my_file.csv", 3, 11, True) == "pat_to_my_file_csv"
    )
    assert (
        file_path_shortener("/path/to/my_very_long_file.csv", 3, 10, True)
        == "pat_to_m_ile_csv"
    )


def test_accession_prefix_separated_dir_path():
    assert accession_prefix_separated_dir_path("PRJ123456", 5, 6) == Path(
        "PRJ12/PRJ123/PRJ123456"
    )
    assert accession_prefix_separated_dir_path("PRJ123456", 5, 6, 99) == Path(
        "PRJ12/PRJ123/PRJ123456/PRJ123456"
    )
    assert accession_prefix_separated_dir_path("ERR1", 9, 9) == Path("ERR1/ERR1/ERR1")
    with pytest.raises(AssertionError):
        accession_prefix_separated_dir_path("PRJ1", 0, 5)
    assert accession_prefix_separated_dir_path("PRJ123456", 3, 6, 7) == Path(
        "PRJ/PRJ123/PRJ1234/PRJ123456"
    )
    assert accession_prefix_separated_dir_path("PRJ123456", -6, -3) == Path(
        "PRJ/PRJ123/PRJ123456"
    )


def test_next_enumerated_subdir(tmp_path):
    assert tmp_path.is_dir()

    # should find next dir but not make it
    d = next_enumerated_subdir(tmp_path)
    assert d == tmp_path / "0001"
    assert not d.is_dir()

    # should find same next dir since it was not made.
    # should now be made
    d = next_enumerated_subdir(tmp_path, mkdirs=True)
    assert d == tmp_path / "0001"
    assert d.is_dir()

    d = next_enumerated_subdir(tmp_path, mkdirs=True)
    assert d == tmp_path / "0002"
    assert d.is_dir()

    # make a file with clashing name
    (tmp_path / "0003").touch()

    # skipped over 0003 because it was a present file
    d = next_enumerated_subdir(tmp_path, mkdirs=True)
    assert d == tmp_path / "0004"
    assert d.is_dir()

    # check hierarchy works
    d = next_enumerated_subdir(d, mkdirs=True)
    assert d == tmp_path / "0004" / "0001"
    assert d.is_dir()

    # check differently formatted numbers dont interfere.
    (tmp_path / "05").mkdir()
    # should still make 0005 since 05 is different
    d = next_enumerated_subdir(tmp_path, mkdirs=True)
    assert d == tmp_path / "0005"
    assert d.is_dir()

    # check one digit strings
    d = next_enumerated_subdir(tmp_path, mkdirs=True, pad=1)
    assert d == tmp_path / "1"
    d = next_enumerated_subdir(tmp_path, mkdirs=True, pad=1)
    assert d == tmp_path / "2"

    # check zero digit strings
    with pytest.raises(ValueError):
        next_enumerated_subdir(tmp_path, mkdirs=True, pad=0)
