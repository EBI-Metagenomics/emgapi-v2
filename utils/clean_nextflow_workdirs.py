import os
import datetime
import click
import shutil
import logging
import glob

logging.getLogger().setLevel(logging.INFO)


def get_directory_time_since_modification(dir_path: str) -> datetime.timedelta:
    max_mtime = 0.0
    for fp, _, _ in os.walk(dir_path):
        mtime = os.path.getmtime(fp)
        if mtime > max_mtime:
            max_mtime = float(mtime)
    return datetime.datetime.now() - datetime.datetime.fromtimestamp(max_mtime)


def parse_timedelta(s: str) -> datetime.timedelta:
    days, remainder = s.split("-")
    hours, minutes, seconds = remainder.split(":")
    td = datetime.timedelta(
        days=int(days), hours=int(hours), minutes=int(minutes), seconds=int(seconds)
    )
    return td


def print_timedelta(td: datetime.timedelta) -> str:
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    seconds = td.seconds & 60
    return f"{td.days}-{hours}:{minutes}:{seconds}"


@click.group("cli")
def cli(): ...


@cli.command("scan")
@click.option(
    "--base_dir",
    default="/hps/nobackup/rdf/metagenomics/service-team/nextflow-workdir",
    help="The base directory to search.",
)
@click.option(
    "--n_level",
    default=3,
    help="The number of subdirectory levels to search from the base directory.",
)
@click.option(
    "--min_age",
    default="7-00:00:00",
    help="The minimum age of any file contained within a directory for that directory to be flagged for deletion, in the following format: <days>-<hours>:<minutes>:<seconds>",
)
@click.option(
    "--manifest_fp",
    default="cleanup_manifest.tsv",
    help="Filepath to manifest of all directories to be deleted.",
)
def generate_report(base_dir: str, n_level: int, min_age: str, manifest_fp: str):
    logging.info(
        f"Looking for subdirectories of {base_dir} ({n_level} level depth) older than {min_age}"
    )
    min_age_td = parse_timedelta(min_age)
    dir_ages = {}
    for dir_path in glob.glob(f"{base_dir}/{'/'.join(['*' for _ in range(n_level)])}"):
        age = get_directory_time_since_modification(dir_path)
        if age > min_age_td:
            dir_ages[dir_path] = age
            logging.info(f"Found {dir_path} with age of {print_timedelta(age)}")

    logging.info(f"{len(dir_ages)} directories found")

    with open(manifest_fp, "wt") as f:
        f.write("# Directory path\tAge (%d-%H:%M:%S)\n")
        for dir_path, age in dir_ages.items():
            age_str = print_timedelta(age)
            f.write(f"{dir_path}\t{age_str}\n")
        f.write(
            "\n# Deletion manifest. Every row in this file is a directory flagged for deletion.\n"
        )
        f.write("# Modify manifest, save with ':w', exit with ':q'\n")
        f.write(
            f"# Then run python clean_v6_nextflow_workdirs.py delete --manifest_fp {manifest_fp}\n"
        )

    logging.info(f"Report generated at {manifest_fp}")
    logging.info(
        f"Edit with `vim {manifest_fp}` and delete directories with `python clean_v6_nextflow_workdirs.py delete --manifest_fp {manifest_fp}`"
    )


@cli.command("delete")
@click.option(
    "--manifest_fp",
    default="cleanup_manifest.tsv",
    help="Filepath to manifest of all directories to be deleted.",
)
@click.option(
    "--dryrun",
    default=False,
    is_flag=True,
    help="Do a dry run and don't actually delete anything.",
)
def delete_dirs(manifest_fp: str, dryrun: bool):
    logging.info(f"Reading report at {manifest_fp}")
    with open(manifest_fp, "rt") as f:
        for line in f:
            l_ = line.strip()
            if len(l_) == 0:
                continue
            if l_[0] == "#":
                continue
            dir_path, _ = [v.strip() for v in l_.split()]
            if not dryrun:
                shutil.rmtree(dir_path)
            logging.info(f"Deleted {dir_path}")


if __name__ == "__main__":
    cli()
