import shutil
from pathlib import Path

import click
from mgnify_pipelines_toolkit.analysis.shared.study_summary_generator import (
    summarise_analyses,
)
from prefect import flow

from activate_django_first import EMG_CONFIG

from analyses.models import Study
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File


@flow
def generate_study_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> [Path]:
    """
    Generate a study summary file for an analysis pipeline execution,
    e.g. a run of the V6 Amplicon pipeline on a samplesheet of runs.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param pipeline_outdir: The path to dir where pipeline published results are, e.g. /nfs/my/dir/abcedfg
    :param completed_runs_filename: E.g. qs_completed_runs.csv, expects to be found in pipeline_outdir
    :return: List of paths to the study summary files generated in the study dir
    """
    study = Study.objects.get(accession=mgnify_study_accession)

    # completed_runs_csv = Path(f"{pipeline_outdir}/{completed_runs_filename}"
    # )

    results_dir = Directory(
        path=Path(pipeline_outdir),
        rules=[DirectoryExistsRule],
    )
    results_dir.files.append(
        File(
            path=results_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    with click.Context(summarise_analyses) as ctx:
        ctx.invoke(
            summarise_analyses,
            runs=results_dir.files[0].path,
            analyses_dir=results_dir.path,
            non_insdc=EMG_CONFIG.amplicon_pipeline.allow_non_insdc_run_names,
            output_prefix=pipeline_outdir.name,  # e.g. a hash of the samplesheet
        )

    if not study.results_dir:
        study.results_dir = pipeline_outdir
        study.save()
        # TODO: need to sync this one too, maybe a better default?
    study_dir = Directory(
        path=Path(study.results_dir),
        rules=[DirectoryExistsRule],
    )

    for summary_file_created in results_dir.path.glob(
        "*_study_summary.tsv"
    ):  # hardcoded in study_summary_generator
        shutil.move(  # NB this will not support files if moved to transfer services area already
            src=summary_file_created, dst=study_dir.path / summary_file_created.name
        )

    return list(study_dir.path.glob(f"{pipeline_outdir.name}*_study_summary.tsv"))
