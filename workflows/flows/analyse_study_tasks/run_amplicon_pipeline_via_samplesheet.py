from datetime import timedelta
from pathlib import Path
from typing import List, Union

from django.conf import settings
from django.utils.text import slugify
from prefect import flow

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.import_completed_amplicon_analyses import (
    import_completed_analyses,
)
from workflows.flows.analyse_study_tasks.make_samplesheet_amplicon import (
    make_samplesheet_amplicon,
)
from workflows.flows.analyse_study_tasks.analysis_states import (
    mark_analysis_as_started,
    mark_analysis_as_failed,
)
from workflows.flows.analyse_study_tasks.set_post_analysies_states import (
    set_post_analysis_states,
)
from workflows.flows.analyse_study_tasks.shared.markergene_study_summary import (
    generate_markergene_summary_for_pipeline_run,
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


@flow(name="Run analysis pipeline-v6 via samplesheet", log_prints=True)
def run_amplicon_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    amplicon_analysis_ids: List[Union[str, int]],
):
    amplicon_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=amplicon_analysis_ids,
        run__metadata__fastq_ftps__isnull=False,
    )
    samplesheet, ss_hash = make_samplesheet_amplicon(mgnify_study, amplicon_analyses)

    for analysis in amplicon_analyses:
        mark_analysis_as_started(analysis)

    amplicon_current_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_amplicon_v6"
    )

    amplicon_current_outdir = (
        amplicon_current_outdir_parent
        / ss_hash[:6]  # uses samplesheet hash prefix as dir name for the chunk
    )
    print(f"Using output dir {amplicon_current_outdir} for this execution")

    command = cli_command(
        [
            ("nextflow", "run", EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_repo),
            ("-r", EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_git_revision),
            "-latest",  # Pull changes from GitHub
            ("-profile", EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_nf_profile),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", amplicon_current_outdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-ansi-log", "false"),
        ]
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Analyse amplicon study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.amplicon_pipeline.amplicon_nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=amplicon_current_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        for analysis in amplicon_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_analysis_states(amplicon_current_outdir, amplicon_analyses)
        import_completed_analyses(amplicon_current_outdir, amplicon_analyses)
        for analysis in amplicon_analyses:
            analysis.refresh_from_db()
            if analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED]:
                break
        else:
            print("Not creating summaries because ALL analyses of samplesheet failed")
            return
        generate_markergene_summary_for_pipeline_run(
            mgnify_study_accession=mgnify_study.accession,
            pipeline_outdir=amplicon_current_outdir,
        )
        generate_study_summary_for_pipeline_run(
            pipeline_outdir=amplicon_current_outdir,
            mgnify_study_accession=mgnify_study.accession,
            analysis_type="amplicon",
            completed_runs_filename=EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
        )
