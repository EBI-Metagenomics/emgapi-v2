from pathlib import Path

from prefect import flow

from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.datamovers import move_data

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis


@flow(
    name="Copy Amplicon Pipeline Results",
    flow_run_name="Copy Amplicon Pipeline Results",
    log_prints=True,
)
def copy_amplicon_pipeline_results(analysis_accession: str):
    analysis = Analysis.objects.get(accession=analysis_accession)
    study = analysis.study
    run = analysis.run
    source = analysis.results_dir
    experiment_type_label = Analysis.ExperimentTypes(
        analysis.experiment_type
    ).label.lower()
    target = f"{EMG_CONFIG.slurm.ftp_results_dir}/{study.first_accession[:-3]}/{study.first_accession}/{run.first_accession[:-3]}/{run.first_accession}/{analysis.pipeline_version}/{experiment_type_label}"

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
    analysis.results_dir = Path(target).relative_to(EMG_CONFIG.slurm.ftp_results_dir)
    print(
        f"Analysis {analysis} now has results at {analysis.results_dir} in {EMG_CONFIG.slurm.ftp_results_dir}"
    )
    analysis.save()
