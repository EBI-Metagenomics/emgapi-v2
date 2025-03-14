import os
from pathlib import Path

from workflows.prefect_utils.dir_context import chdir


def test_dir_context(tmp_path):
    original_wd = os.getcwd()

    with chdir(tmp_path):
        assert os.getcwd() == str(tmp_path)
        assert not os.getcwd() == original_wd

        Path("test").touch()

    assert (tmp_path / "test").is_file()
    assert os.getcwd() == original_wd
    assert not Path("test").is_file()
