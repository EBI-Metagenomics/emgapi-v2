from pathlib import Path

import pandas as pd
from prefect import get_run_logger

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Study
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File
from workflows.flows.analysis.summaries.amplicon.shared.parquet.utils import (
    write_parquet_summary,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow


def build_amplicon_taxonomic_results_dataframe(
    mgnify_study_accession: str,
    runs: Path,
    analyses_dir: Path,
    otu_dir: Path,
) -> pd.DataFrame:
    _ = (mgnify_study_accession, runs, analyses_dir, otu_dir)
    # TODO: Implement the v5-specific parser in this module once the v5 file layout
    # and expected taxonomic outputs are confirmed. Keep the logic local to this
    # versioned flow rather than moving it into the shared parquet helper.
    raise NotImplementedError(
        "Amplicon v5 parquet summary generation is not implemented yet."
    )


@flow()
def generate_study_summary_parquet_for_pipeline_run(
    mgnify_study_accession: str,
    pipeline_outdir: Path,
    otu_dir: Path | str,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
) -> Path:
    logger = get_run_logger()
    study = Study.objects.get(accession=mgnify_study_accession)
    study.set_results_dir_default()

    pipeline_run_dir = Directory(path=pipeline_outdir, rules=[DirectoryExistsRule])
    summary_dir = Directory(path=study.results_dir_path / "amplicon_v5" / "summaries")
    summary_dir.path.mkdir(parents=True, exist_ok=True)
    pipeline_run_dir.files.append(
        File(
            path=pipeline_run_dir.path / completed_runs_filename,
            rules=[FileExistsRule, FileIsNotEmptyRule],
        )
    )

    # TODO: Once v5 implementation is added, use these validated inputs to build the
    # study-level dataframe from DB-backed metadata plus v5 pipeline result files.
    _ = Analysis.PipelineVersions.v5
    output_prefix = f"{summary_dir.path}/{pipeline_run_dir.path.name}"
    logger.info("Preparing amplicon v5 taxonomic parquet summary flow for %s", study)

    final_df = build_amplicon_taxonomic_results_dataframe(
        mgnify_study_accession=mgnify_study_accession,
        runs=pipeline_run_dir.files[0].path,
        analyses_dir=pipeline_run_dir.path,
        otu_dir=Path(otu_dir),
    )
    return write_parquet_summary(final_df, output_prefix)


__all__ = [
    "build_amplicon_taxonomic_results_dataframe",
    "generate_study_summary_parquet_for_pipeline_run",
]
