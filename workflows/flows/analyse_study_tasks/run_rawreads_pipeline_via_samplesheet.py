from datetime import timedelta
from pathlib import Path
from typing import List, Union

from django.conf import settings
from django.utils.text import slugify
from prefect import flow

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.import_completed_rawreads_analyses import (
    import_completed_analyses,
)
from workflows.flows.analyse_study_tasks.make_samplesheet_rawreads import (
    make_samplesheet_rawreads,
)
from workflows.flows.analyse_study_tasks.analysis_states import (
    mark_analysis_as_started,
    mark_analysis_as_failed,
)
from workflows.flows.analyse_study_tasks.set_rawreads_post_analysis_states import (
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


@flow(name="Run raw-reads analysis pipeline-v6 via samplesheet", log_prints=True)
def run_rawreads_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    rawreads_analysis_ids: List[Union[str, int]],
):
    rawreads_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=rawreads_analysis_ids,
        run__metadata__fastq_ftps__isnull=False,
    )
    samplesheet, ss_hash = make_samplesheet_rawreads(mgnify_study, rawreads_analyses)

    for analysis in rawreads_analyses:
        mark_analysis_as_started(analysis)

    rawreads_current_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_rawreads_v6"
    )

    rawreads_current_outdir = (
        rawreads_current_outdir_parent
        / ss_hash[:6]  # uses samplesheet hash prefix as dir name for the chunk
    )
    print(f"Using output dir {rawreads_current_outdir} for this execution")

    command = cli_command(
        [
            ("nextflow", "run", EMG_CONFIG.rawreads_pipeline.rawreads_pipeline_repo),
            ("-r", EMG_CONFIG.rawreads_pipeline.rawreads_pipeline_git_revision),
            "-latest",  # Pull changes from GitHub
            ("-profile", EMG_CONFIG.rawreads_pipeline.rawreads_pipeline_nf_profile),
            "-resume",
            ("--samplesheet", samplesheet),
            ("--outdir", rawreads_current_outdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-name", f"rawreads-v6-sheet-{slugify(samplesheet)[-10:]}"),
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
                days=EMG_CONFIG.rawreads_pipeline.rawreads_pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.rawreads_pipeline.rawreads_nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=rawreads_current_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        for analysis in rawreads_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_analysis_states(rawreads_current_outdir, rawreads_analyses)
        import_completed_analyses(rawreads_current_outdir, rawreads_analyses)
        generate_study_summary_for_pipeline_run(
            pipeline_outdir=rawreads_current_outdir,
            mgnify_study_accession=mgnify_study.accession,
            analysis_type="rawreads",
            completed_runs_filename=EMG_CONFIG.rawreads_pipeline.completed_runs_csv,
        )
