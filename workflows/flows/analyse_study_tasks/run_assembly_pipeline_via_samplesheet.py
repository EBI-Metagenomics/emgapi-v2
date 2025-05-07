from datetime import timedelta
from pathlib import Path
from typing import List, Union

from django.conf import settings
from django.utils.text import slugify
from prefect import flow
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.import_completed_assembly_analyses import (
    import_completed_assembly_analyses,
)
from workflows.flows.analyse_study_tasks.make_samplesheet_assembly import (
    make_samplesheet_assembly,
)
from workflows.flows.analyse_study_tasks.analysis_states import (
    mark_analysis_as_started,
    mark_analysis_as_failed,
)
from workflows.flows.analyse_study_tasks.set_post_assembly_analysis_states import (
    set_post_assembly_analysis_states,
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


@flow(name="Run assembly analysis pipeline-v6 via samplesheet", log_prints=True)
def run_assembly_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_analysis_ids: List[Union[str, int]],
):
    """
    Run the assembly analysis pipeline for a set of assemblies via a samplesheet.
    :param mgnify_study: The MGnify study
    :param assembly_analysis_ids: List of assembly analysis IDs
    """
    assembly_analyses = analyses.models.Analysis.objects.select_related(
        "assembly"
    ).filter(
        id__in=assembly_analysis_ids,
        assembly__isnull=False,
    )
    samplesheet, ss_hash = make_samplesheet_assembly(mgnify_study, assembly_analyses)

    for analysis in assembly_analyses:
        mark_analysis_as_started(analysis)

    assembly_current_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_assembly_v6/{flow_run.root_flow_run_id}"
    )

    assembly_current_outdir = (
        assembly_current_outdir_parent
        / ss_hash[:6]  # uses samplesheet hash prefix as dir name for the chunk
    )
    print(f"Using output dir {assembly_current_outdir} for this execution")

    command = cli_command(
        [
            (
                "nextflow",
                "run",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_repo,
            ),
            (
                "-r",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_git_revision,
            ),
            "-latest",
            (
                "-profile",
                settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_nf_profile,
            ),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", assembly_current_outdir),
            settings.EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-ansi-log", "false"),
        ]
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Analyse assembly study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(
                days=settings.EMG_CONFIG.assembly_analysis_pipeline.pipeline_time_limit_days
            ),
            memory=f"{settings.EMG_CONFIG.assembly_analysis_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=assembly_current_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        for analysis in assembly_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_assembly_analysis_states(assembly_current_outdir, assembly_analyses)
        import_completed_assembly_analyses(assembly_current_outdir, assembly_analyses)
        generate_study_summary_for_pipeline_run(
            pipeline_outdir=assembly_current_outdir,
            mgnify_study_accession=mgnify_study.accession,
            analysis_type="assembly",
            completed_runs_filename=EMG_CONFIG.assembly_analysis_pipeline.completed_assemblies_csv,
        )
