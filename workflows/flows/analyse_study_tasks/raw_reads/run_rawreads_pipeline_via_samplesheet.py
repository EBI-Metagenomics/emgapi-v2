import os
from datetime import timedelta
from pathlib import Path
from typing import List, Union

from django.conf import settings
from django.db import close_old_connections
from django.utils.text import slugify
from prefect import flow

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.raw_reads.import_completed_rawreads_analyses import (
    import_completed_analyses,
)
from workflows.flows.analyse_study_tasks.raw_reads.make_samplesheet_rawreads import (
    make_samplesheet_rawreads,
)
from workflows.flows.analyse_study_tasks.shared.analysis_states import (
    mark_analysis_as_started,
    mark_analysis_as_failed,
)
from workflows.flows.analyse_study_tasks.raw_reads.set_rawreads_post_analysis_states import (
    set_post_analysis_states,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    generate_study_summary_for_pipeline_run,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.slurm_flow import (
    run_cluster_job,
    ClusterJobFailedException,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy
from workflows.flows.analyse_study_tasks.cleanup_pipeline_directories import (
    delete_pipeline_workdir,
)


@flow(name="Run raw-reads analysis pipeline-v6 via samplesheet", log_prints=True)
def run_rawreads_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    rawreads_analysis_ids: List[Union[str, int]],
    workdir: Path,
):
    rawreads_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=rawreads_analysis_ids,
        run__metadata__fastq_ftps__isnull=False,
    )
    samplesheet, ss_hash = make_samplesheet_rawreads(mgnify_study, rawreads_analyses)

    for analysis in rawreads_analyses:
        mark_analysis_as_started(analysis)

    nextflow_outdir = (
        workdir / ss_hash[:6]  # uses samplesheet hash prefix as dir name for the chunk
    )
    print(f"Using output dir {nextflow_outdir} for this execution")

    nextflow_workdir = workdir / f"rawreads-v6-sheet-{slugify(samplesheet)[-10:]}"
    os.makedirs(nextflow_workdir, exist_ok=True)

    command = cli_command(
        [
            ("nextflow", "run", EMG_CONFIG.rawreads_pipeline.pipeline_repo),
            ("-r", EMG_CONFIG.rawreads_pipeline.pipeline_git_revision),
            "-latest",  # Pull changes from GitHub
            ("-config", EMG_CONFIG.rawreads_pipeline.pipeline_config_file),
            "-resume",
            ("--samplesheet", samplesheet),
            ("--outdir", nextflow_outdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            EMG_CONFIG.rawreads_pipeline.has_fire_access and "--use_fire_download",
            ("-work-dir", nextflow_workdir),
            ("-ansi-log", "false"),
        ]
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Analyse raw-reads study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.rawreads_pipeline.pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.rawreads_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=nextflow_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
        close_old_connections()
    except ClusterJobFailedException:
        close_old_connections()
        for analysis in rawreads_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_analysis_states(nextflow_outdir, rawreads_analyses)
        import_completed_analyses(nextflow_outdir, rawreads_analyses)
        generate_study_summary_for_pipeline_run(
            pipeline_outdir=nextflow_outdir,
            mgnify_study_accession=mgnify_study.accession,
            analysis_type="rawreads",
            completed_runs_filename=EMG_CONFIG.rawreads_pipeline.completed_runs_csv,
        )
        delete_pipeline_workdir(
            workdir
        )  # will also delete past "abandoned" nextflow files
