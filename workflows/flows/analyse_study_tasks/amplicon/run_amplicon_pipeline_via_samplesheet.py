from datetime import timedelta
from pathlib import Path
from textwrap import dedent
from typing import List, Optional, Union

from django.conf import settings
from django.utils.text import slugify
from prefect import suspend_flow_run
from prefect.input import RunInput
from pydantic import Field

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.amplicon.import_completed_amplicon_analyses import (
    import_completed_analyses,
)
from workflows.flows.analyse_study_tasks.amplicon.make_samplesheet_amplicon import (
    make_samplesheet_amplicon,
)
from workflows.flows.analyse_study_tasks.amplicon.set_post_analysies_states import (
    set_post_analysis_states,
)
from workflows.flows.analyse_study_tasks.cleanup_pipeline_directories import (
    remove_dir,
)
from workflows.flows.analyse_study_tasks.shared.analysis_states import (
    mark_analysis_as_failed,
    mark_analysis_as_started,
)
from workflows.flows.analyse_study_tasks.shared.dwcr_generator import (
    generate_dwc_ready_summary_for_pipeline_run,
)
from workflows.flows.analyse_study_tasks.shared.markergene_study_summary import (
    generate_markergene_summary_for_pipeline_run,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    generate_study_summary_for_pipeline_run,
)
from workflows.flows.analysis import AnalysisType
from workflows.nextflow_utils.samplesheets import queryset_hash
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.input_helpers import ask_every_time_suspend_for_input_key
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy
from workflows.views import encode_samplesheet_path


@flow(name="Run amplicon analysis pipeline via samplesheet", log_prints=True)
def run_amplicon_pipeline_via_samplesheet(
    mgnify_study: analyses.models.Study,
    amplicon_analysis_ids: List[Union[str, int]],
    workdir: Optional[Path],
    outdir: Optional[Path],
    wait_for_samplesheet_editing: bool = False,
):
    if workdir is None:
        workdir = (
            Path(f"{EMG_CONFIG.slurm.default_nextflow_workdir}")
            / Path(f"{mgnify_study.ena_study.accession}")
            / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{EMG_CONFIG.amplicon_pipeline.pipeline_version}"
        )
    if outdir is None:
        outdir = (
            Path(f"{EMG_CONFIG.slurm.default_workdir}")
            / Path(f"{mgnify_study.ena_study.accession}")
            / f"{EMG_CONFIG.amplicon_pipeline.pipeline_name}_{EMG_CONFIG.amplicon_pipeline.pipeline_version}"
        )
    amplicon_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=amplicon_analysis_ids,
        run__metadata__fastq_ftps__isnull=False,
    )
    runs_ids = amplicon_analyses.values_list("run_id", flat=True)
    runs = analyses.models.Run.objects.filter(id__in=runs_ids)
    print(f"Making amplicon samplesheet for runs {runs_ids}")

    ss_hash = queryset_hash(runs, "id")

    nextflow_outdir = (
        outdir / ss_hash
    )  # uses samplesheet hash prefix as dir name for the chunk
    nextflow_outdir.mkdir(parents=True, exist_ok=True)
    print(f"Using output dir {nextflow_outdir} for this execution")

    nextflow_workdir = workdir / ss_hash
    nextflow_workdir.mkdir(parents=True, exist_ok=True)
    print(f"Using work dir {nextflow_workdir} for this execution")

    samplesheet = make_samplesheet_amplicon(runs, nextflow_outdir / "samplesheet.csv")

    if wait_for_samplesheet_editing:
        class ResumeAfterSamplesheetEditingInput(RunInput):
            okay: bool = Field(
                True, description="Resume the flow after editing the samplesheet."
            )

        suspend_flow_run(
            timeout=EMG_CONFIG.assembler.suspend_timeout_for_editing_samplesheets_secs,
            wait_for_input=ResumeAfterSamplesheetEditingInput.with_initial_data(
                description=dedent(f"""\
                    ## Amplicon samplesheet is ready for editing

                    Please edit the samplesheet:

                    * [Edit samplesheet {samplesheet.name}]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(samplesheet)})

                    **Edit the samplesheet as needed and then resume ("Submit") this flow.**
                    """)
            ),
            key=f"amplicon-samplesheet-editing-{ask_every_time_suspend_for_input_key()}",
        )

    for analysis in amplicon_analyses:
        mark_analysis_as_started(analysis)

    command = cli_command(
        [
            ("nextflow", "run", EMG_CONFIG.amplicon_pipeline.pipeline_repo),
            ("-r", EMG_CONFIG.amplicon_pipeline.pipeline_git_revision),
            "-latest",  # Pull changes from GitHub
            (
                "-c",
                settings.EMG_CONFIG.amplicon_pipeline.pipeline_config_file,
            ),
            (
                "-profile",
                settings.EMG_CONFIG.amplicon_pipeline.pipeline_nf_profile,
            ),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", nextflow_outdir),
            EMG_CONFIG.amplicon_pipeline.has_fire_access and "--use_fire_download",
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
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
            name=f"Analyse amplicon study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.amplicon_pipeline.pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.amplicon_pipeline.nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=nextflow_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        for analysis in amplicon_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_analysis_states(nextflow_outdir, amplicon_analyses)
        import_completed_analyses(nextflow_outdir, amplicon_analyses)
        for analysis in amplicon_analyses:
            analysis.refresh_from_db()
            if analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED]:
                break
        else:
            print("Not creating summaries because ALL analyses of samplesheet failed")
            return
        generate_markergene_summary_for_pipeline_run(
            mgnify_study_accession=mgnify_study.accession,
            pipeline_outdir=nextflow_outdir,
        )
        generate_study_summary_for_pipeline_run(
            pipeline_outdir=nextflow_outdir,
            mgnify_study_accession=mgnify_study.accession,
            analysis_type=AnalysisType.AMPLICON,
            completed_runs_filename=EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
        )
        generate_dwc_ready_summary_for_pipeline_run(
            mgnify_study_accession=mgnify_study.accession,
            pipeline_outdir=nextflow_outdir,
            refdb_otus_dir=Path(EMG_CONFIG.amplicon_pipeline.refdb_otus_dir),
            completed_runs_filename=EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
        )
        remove_dir(nextflow_workdir)  # will also delete past "abandoned" nextflow files
