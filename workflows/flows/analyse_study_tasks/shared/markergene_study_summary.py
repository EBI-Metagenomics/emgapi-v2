import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from mgnify_pipelines_toolkit.analysis.shared.markergene_study_summary import (
    main as markergene_study_summary,
)
from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG
from analyses.models import Study, Analysis
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File
from workflows.prefect_utils.dir_context import chdir

STUDY_SUMMARY = "study_summary"
STUDY_SUMMARY_TSV = STUDY_SUMMARY + ".tsv"


@flow
def generate_markergene_summary_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
):
    """
    Generate a markergene summary file for an analysis pipeline execution,
    e.g. a run of the V6 Amplicon pipeline on a samplesheet of runs.

    :param mgnify_study_accession: e.g. MGYS0000001
    :param pipeline_outdir: The path to dir where pipeline published results are, e.g. /nfs/my/dir/abcedfg
    :param completed_runs_filename: E.g. qs_completed_runs.csv, expects to be found in pipeline_outdir
    :return: None
    """
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)
    logger.info(f"Generating marker gene summaries for a pipeline execution of {study}")

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
    logger.info(f"Expecting to find ASV summaries in {results_dir.path}")
    logger.info(f"Using runs from {results_dir.files[0].path}")

    with tempfile.TemporaryDirectory() as workdir:
        with chdir(workdir):
            logger.info(f"Using temporary workdir {workdir}")

            input_path = results_dir.path
            runs = results_dir.files[0].path
            prefix = pipeline_outdir.name

            logger.debug(
                f"For markergene summary, {input_path = }, {runs = }, {prefix = }"
            )

            logger.debug(f"Glob of input_path is {list(input_path.glob('*'))}")

            content = runs.read_text()
            logger.debug(f"Content of runs file is\n{content}")

            # TODO: update the toolkit function to refactor main into a library-callable method
            with patch(
                "mgnify_pipelines_toolkit.analysis.shared.markergene_study_summary.parse_args",
                return_value=(input_path, runs, prefix),
            ):
                markergene_study_summary()

            with (Path(workdir) / f"{prefix}_markergene_study_summary.json").open(
                "r"
            ) as f:
                summary_for_ss = json.load(f)

            if (
                ampregion_file := Path(workdir)
                / f"{prefix}_ampregion_study_summary.json"
            ).exists():
                with ampregion_file.open("r") as f:
                    asv_summary_for_ss = json.load(f)
            else:
                asv_summary_for_ss = {}

    for run_accession, summary_for_run in summary_for_ss.items():
        marker_gene_summary_for_run = {
            Analysis.CLOSED_REFERENCE: summary_for_run,
            Analysis.ASV: asv_summary_for_ss.get(run_accession, {}),
        }
        analysis = study.analyses.get(
            run__ena_accessions__icontains=run_accession,
            pipeline_version=Analysis.PipelineVersions.v6,
        )
        analysis.metadata[analysis.KnownMetadataKeys.MARKER_GENE_SUMMARY] = (
            marker_gene_summary_for_run
        )
        analysis.save()
