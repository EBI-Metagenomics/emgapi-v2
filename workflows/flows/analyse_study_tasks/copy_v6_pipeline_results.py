from pathlib import Path

from prefect import task

from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    trailing_slash_ensured_dir,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import STUDY_SUMMARY_TSV
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.datamovers import move_data

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study


@task(
    name="Copy V6 Pipeline Results",
    task_run_name="Copy V6 Pipeline Results for {analysis_accession}",
    log_prints=True,
)
def copy_v6_pipeline_results(analysis_accession: str):
    analysis = Analysis.objects.get(accession=analysis_accession)
    study = analysis.study
    source = trailing_slash_ensured_dir(analysis.results_dir)
    experiment_type_label = Analysis.ExperimentTypes(
        analysis.experiment_type
    ).label.lower()
    assert (
        analysis.is_private == study.is_private == analysis.assembly_or_run.is_private
    )  # Shouldn't ever be untrue, just a helper for the future

    target_root = (
        EMG_CONFIG.slurm.private_results_dir
        if analysis.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )

    target = f"{target_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/{accession_prefix_separated_dir_path(analysis.assembly_or_run.first_accession, -3)}/{analysis.pipeline_version}/{experiment_type_label}"
    print(
        f"Will copy results for {analysis_accession} from {analysis.results_dir} to {target}"
    )

    allowed_extensions = [
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "json",
        "gz",
        "fasta",
        "csv",
    ]

    command = cli_command(
        [
            "rsync",
            "-av",
            "--include=*/",
        ]
        + [f"--include=*.{ext}" for ext in allowed_extensions]
        + ["--exclude=*"]
    )
    move_data(source, target, command)
    analysis.external_results_dir = Path(target).relative_to(target_root)
    print(
        f"Analysis {analysis} now has results at {analysis.external_results_dir} in {EMG_CONFIG.slurm.ftp_results_dir}"
    )
    analysis.save()


@task(name="Copy V6 Study Summaries", log_prints=True)
def copy_v6_study_summaries(study_accession: str):
    study = Study.objects.get(accession=study_accession)
    if not study.results_dir:
        print(f"Study {study} has no results dir, skipping")
        return
    command = cli_command(
        [
            "rsync",
            "-av",
            f"--include=PRJ*{STUDY_SUMMARY_TSV}",
            f"--include=[DES]RP*{STUDY_SUMMARY_TSV}",
            "--exclude=*",
        ]
    )
    source = trailing_slash_ensured_dir(study.results_dir)
    target_root = (
        EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else EMG_CONFIG.slurm.ftp_results_dir
    )
    target = f"{target_root}/{accession_prefix_separated_dir_path(study.first_accession, -3)}/study-summaries/"
    move_data(source, target, command, make_target=True)
    study.external_results_dir = Path(target).parent.relative_to(target_root)
    study.save()
    print(
        f"Study {study} now has results at {study.external_results_dir} in {target_root}"
    )
