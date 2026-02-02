import pytest
from click.testing import CliRunner
import os
import datetime
import shutil
import logging
from pathlib import Path
from utils.clean_nextflow_workdirs import (
    get_directory_time_since_modification,
    parse_timedelta,
    print_timedelta,
    generate_report,
    delete_dirs,
)


@pytest.fixture
def base_dir() -> Path:
    return Path("fixtures/clean_nextflow_workdirs")


@pytest.fixture
def fp_mtimes() -> dict[Path, datetime.timedelta]:
    results_dir = Path("results")
    return {
        results_dir
        / "dir1"
        / "file1.txt": datetime.timedelta(days=100, hours=0, minutes=0, seconds=0),
        results_dir
        / "dir1"
        / "file2.txt": datetime.timedelta(days=0, hours=0, minutes=0, seconds=1),
        results_dir
        / "dir2"
        / "file1.txt": datetime.timedelta(days=200, hours=0, minutes=0, seconds=0),
        results_dir
        / "dir2"
        / "file2.txt": datetime.timedelta(days=100, hours=0, minutes=0, seconds=0),
        results_dir
        / "dir3"
        / "file1.txt": datetime.timedelta(days=100, hours=0, minutes=0, seconds=0),
        results_dir
        / "dir3"
        / "file2.txt": datetime.timedelta(days=1, hours=0, minutes=0, seconds=0),
        results_dir
        / "dir3"
        / "file3.txt": datetime.timedelta(days=5, hours=0, minutes=0, seconds=1),
    }


@pytest.fixture
def dir_mtimes() -> dict[Path, datetime.timedelta]:
    results_dir = Path("results")
    return {
        results_dir / "dir1": datetime.timedelta(days=0, hours=0, minutes=0, seconds=1),
        results_dir
        / "dir2": datetime.timedelta(days=100, hours=0, minutes=0, seconds=0),
        results_dir / "dir3": datetime.timedelta(days=1, hours=0, minutes=0, seconds=0),
    }


@pytest.fixture
def str2timedelta() -> dict[str, datetime.timedelta]:
    return {
        "0-00:00:01": datetime.timedelta(days=0, hours=0, minutes=0, seconds=1),
        "0-00:01:00": datetime.timedelta(days=0, hours=0, minutes=1, seconds=0),
        "0-01:00:00": datetime.timedelta(days=0, hours=1, minutes=0, seconds=0),
        "20-00:00:01": datetime.timedelta(days=20, hours=0, minutes=0, seconds=1),
        "20000-00:00:01": datetime.timedelta(
            days=20_000, hours=0, minutes=0, seconds=1
        ),
    }


def test_parse_timedelta(str2timedelta):
    for q, a in str2timedelta.items():
        assert a == parse_timedelta(q)


def test_print_timedelta(str2timedelta):
    for a, q in str2timedelta.items():
        assert a == print_timedelta(q)


def create_dir_mtime_fixtures(fp_mtimes: dict, dir_mtimes: dict, tmp_dir: Path):
    logger = logging.getLogger("create_dir_mtime_fixtures")
    now = datetime.datetime.now().timestamp()

    for fp, mtime in fp_mtimes.items():
        (tmp_dir / fp).parent.mkdir(exist_ok=True)
        with open(tmp_dir / fp, "wt") as f:
            f.write("Made you look")
        age = int(now - mtime.total_seconds())
        os.utime(tmp_dir / fp, times=(age, age))
        check_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
            os.path.getmtime(tmp_dir / fp)
        )
        logger.info(f"Created file at {tmp_dir / fp} with age {check_age}")

    for fp, mtime in dir_mtimes.items():
        age = int(now - mtime.total_seconds())
        os.utime(tmp_dir / fp, times=(age, age))
        check_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
            os.path.getmtime(tmp_dir / fp)
        )
        logger.info(f"Created directory at {tmp_dir / fp} with age {check_age}")


def tidy_up(base_dir: Path):
    shutil.rmtree(base_dir)


def test_get_directory_time_since_modification(base_dir, fp_mtimes, dir_mtimes):
    logger = logging.getLogger("test_get_directory_time_since_modification")
    create_dir_mtime_fixtures(fp_mtimes, dir_mtimes, base_dir)

    td = datetime.timedelta(days=2, hours=0, minutes=0, seconds=0)
    for dir_path, _ in dir_mtimes.items():
        dir_fp_mtimes = {
            k: v for k, v in fp_mtimes.items() if str(k.parent) == str(dir_path)
        }
        logger.info(f"Directory {dir_path} should have files: {dir_fp_mtimes}")
        assert get_directory_time_since_modification(
            str(base_dir / dir_path), td
        ).total_seconds() // 3600 in {
            v.total_seconds() // 3600 for _, v in dir_fp_mtimes.items()
        }

    tidy_up(base_dir)


def test_generate_report(base_dir, fp_mtimes, dir_mtimes):
    logger = logging.getLogger("test_generate_report")
    runner = CliRunner(mix_stderr=False)
    base_dir.mkdir(exist_ok=True)
    with runner.isolated_filesystem(base_dir) as tmp_dir:
        logger.info(f"Temporary directory: {tmp_dir}")
        create_dir_mtime_fixtures(fp_mtimes, dir_mtimes, Path(tmp_dir))

        manifest_fp = Path(tmp_dir) / "delete_manifest.csv"
        results_dir = Path(tmp_dir) / "results"
        result = runner.invoke(
            generate_report,
            [
                "--base_dir",
                str(results_dir),
                "--n_level",
                "1",
                "--min_age",
                "2-00:00:00",
                "--manifest_fp",
                str(manifest_fp),
            ],
        )

        logger.info("Running scan")

        logger.info(f"Exit code: {result.exit_code}")
        logger.info(f"StdOut: {result.stdout}")
        logger.info(f"StdErr: {result.stderr}")

        with open(manifest_fp, "rt") as f:
            manifest_lines = [line.strip() for line in f.readlines()]

        dirs_to_delete = {
            v.split()[0] for v in manifest_lines if (len(v) > 0) and (v[0] != "#")
        }
        logger.info(f"Directories to delete: {dirs_to_delete}")

        assert str(results_dir / "dir1") not in dirs_to_delete
        assert str(results_dir / "dir2") in dirs_to_delete
        assert str(results_dir / "dir3") not in dirs_to_delete

    tidy_up(base_dir)


def test_delete_dirs(base_dir, fp_mtimes, dir_mtimes):
    logger = logging.getLogger("test_delete_dirs")
    runner = CliRunner(mix_stderr=False)
    base_dir.mkdir(exist_ok=True)
    with runner.isolated_filesystem(base_dir) as tmp_dir:
        logger.info(f"Temporary directory: {tmp_dir}")
        create_dir_mtime_fixtures(fp_mtimes, dir_mtimes, Path(tmp_dir))

        manifest_fp = Path(tmp_dir) / "delete_manifest.csv"
        results_dir = Path(tmp_dir) / "results"
        result = runner.invoke(
            generate_report,
            [
                "--base_dir",
                str(results_dir),
                "--n_level",
                "1",
                "--min_age",
                "2-00:00:00",
                "--manifest_fp",
                str(manifest_fp),
            ],
        )

        logger.info("Running scan")
        logger.info(f"Exit code: {result.exit_code}")
        logger.info(f"StdOut: {result.stdout}")
        logger.info(f"StdErr: {result.stderr}")

        result2 = runner.invoke(
            delete_dirs,
            [
                "--manifest_fp",
                str(manifest_fp),
            ],
        )

        logger.info("Running delete")
        logger.info(f"Exit code: {result2.exit_code}")
        logger.info(f"StdOut: {result2.stdout}")
        logger.info(f"StdErr: {result2.stderr}")

        assert os.path.exists(results_dir / "dir1")
        assert not os.path.exists(results_dir / "dir2")
        assert os.path.exists(results_dir / "dir3")

    tidy_up(base_dir)
