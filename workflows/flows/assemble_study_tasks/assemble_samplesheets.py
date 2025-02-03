import csv
import json
from datetime import timedelta
from pathlib import Path
from typing import Iterable

import django
import pandas as pd
from django.conf import settings
from prefect import flow, get_run_logger, task

django.setup()

import analyses.models
from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    file_path_shortener,
)
from workflows.prefect_utils.analyses_models_helpers import task_mark_assembly_status
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)
from workflows.prefect_utils.slurm_policies import (
    ResubmitWithCleanedNextflowIfFailedPolicy,
)

EMG_CONFIG = settings.EMG_CONFIG


@task
def update_assembly_metadata(
    miassembler_outdir: Path,
    assembly: analyses.models.Assembly,
    assembler: analyses.models.Assembler,
) -> None:
    """
    Update assembly with post-assembly metadata like assembler and coverage.
    """
    logger = get_run_logger()
    run_accession = assembly.run.first_accession
    study_accession = assembly.reads_study.ena_study.accession

    assembly.assembler = assembler
    assembly.dir = (
        miassembler_outdir
        / accession_prefix_separated_dir_path(study_accession, 7)
        / accession_prefix_separated_dir_path(run_accession, 7)
    )
    assembly.save()
    logger.info(f"Assembly directory is {assembly.dir}")

    coverage_report_path = Path(assembly.dir) / Path(
        f"assembly/{assembly.assembler.name.lower()}/{assembly.assembler.version}/coverage/{run_accession}_coverage.json"
    )
    if not coverage_report_path.is_file():
        raise Exception(f"Assembly coverage file not found at {coverage_report_path}")

    with open(coverage_report_path, "r") as json_file:
        coverage_report = json.load(json_file)

    for key in [
        assembly.CommonMetadataKeys.COVERAGE,
        assembly.CommonMetadataKeys.COVERAGE_DEPTH,
    ]:
        if not key in coverage_report:
            logger.warning(f"No '{key}' found in {coverage_report_path}")
        assembly.metadata[key] = coverage_report.get(key)

    logger.info(f"Assembly metadata of {assembly} is now {assembly.metadata}")

    assembly.save()


@task
def update_assemblies_assemblers_from_samplesheet(
    samplesheet_df: pd.DataFrame,
):
    """
    Updates the assemblers associated with each assembly, based on what is in the samplesheet.
    This is done because a samplesheet CSV may have been edited since first created.
    :param samplesheet_df: Pandas dataframe of the samplesheet content
    :return:
    """
    logger = get_run_logger()
    for _, assembly_row in samplesheet_df.iterrows():
        try:
            latest_assembly = (
                analyses.models.Assembly.objects.filter(
                    run__ena_accessions__0=assembly_row["reads_accession"]
                )
                .order_by("-created_at")
                .first()
            )
        except analyses.models.Assembly.DoesNotExist:
            logger.warning(
                f"Could not find unique assembly for {assembly_row['reads_accession']}"
            )
            continue
        if not latest_assembly:
            logger.warning(
                f"Could not find unique assembly for {assembly_row['reads_accession']}"
            )
            continue
        try:
            assembler_in_samplesheet = assembly_row["assembler"]
        except KeyError:
            logger.warning("Could not find assembler in samplesheet")
            continue
        latest_assembly.assembler = (
            analyses.models.Assembler.objects.filter(
                name__iexact=assembler_in_samplesheet
            )
            .order_by("-version")
            .first()
        )
        latest_assembly.save()


@flow(flow_run_name="Assemble {samplesheet_csv}", persist_result=True)
def run_assembler_for_samplesheet(
    mgnify_study: analyses.models.Study,
    samplesheet_csv: Path,
    assembler: analyses.models.Assembler,
):
    samplesheet_df = pd.read_csv(samplesheet_csv, sep=",")
    assemblies: Iterable[analyses.models.Assembly] = (
        mgnify_study.assemblies_reads.filter(
            run__ena_accessions__0__in=samplesheet_df["reads_accession"]
        )
    )

    update_assemblies_assemblers_from_samplesheet(samplesheet_df)

    for assembly in assemblies:
        # Mark assembly as started
        task_mark_assembly_status(
            assembly,
            status=assembly.AssemblyStates.ASSEMBLY_STARTED,
            unset_statuses=[
                assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                assembly.AssemblyStates.ASSEMBLY_FAILED,
            ],
        )

    miassembler_outdir = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_miassembler"
    )
    command = (
        f"nextflow run {EMG_CONFIG.assembler.assembly_pipeline_repo} "
        f"-r {EMG_CONFIG.assembler.miassemebler_git_revision} "
        f"-latest "  # Pull changes from GitHub
        f"-profile {EMG_CONFIG.assembler.miassembler_nf_profile} "
        f"-resume "
        f"--samplesheet {samplesheet_csv} "
        f"--outdir {miassembler_outdir} "
        f"--assembler {assembler.name.lower()} "
        f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        f"-name miassembler-samplesheet-{file_path_shortener(samplesheet_csv, 1, 15, True)} "
    )

    try:
        run_cluster_job(
            name=f"Assemble study {mgnify_study.ena_study.accession} via samplesheet {file_path_shortener(samplesheet_csv, 1, 15, True)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.assembler.assembly_pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.assembler.assembly_nextflow_master_job_memory_gb}G",
            environment="ALL,TOWER_ACCESS_TOKEN,TOWER_WORKSPACE_ID",
            input_files_to_hash=[samplesheet_csv],
            resubmit_policy=ResubmitWithCleanedNextflowIfFailedPolicy,
            working_dir=miassembler_outdir,
        )
    except ClusterJobFailedException:
        for assembly in assemblies:
            task_mark_assembly_status(
                assembly, status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED
            )
    else:
        # The pipeline produces top level end of execution reports, which contain
        # the list of the runs that were assembled, and those that were not.
        # For more information: https://github.com/EBI-Metagenomics/miassembler?tab=readme-ov-file#top-level-reports

        # QC failed / not assembled runs: qc_failed_runs.csv
        # Assembled runs: assembled_runs.csv

        qc_failed_csv = miassembler_outdir / Path("qc_failed_runs.csv")
        qc_failed_runs = {}  # Stores {run_accession, qc_fail_reason}

        if qc_failed_csv.is_file():
            with qc_failed_csv.open(mode="r") as file_handle:
                for row in csv.reader(file_handle, delimiter=","):
                    run_accession, fail_reason = row
                    qc_failed_runs[run_accession] = fail_reason

        assembled_runs_csv = miassembler_outdir / Path("assembled_runs.csv")
        assembled_runs = set()

        if not assembled_runs_csv.is_file():
            for assembly in assemblies:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason=f"The miassembler output is missing the {assembled_runs_csv} file.",
                )
            raise Exception(
                f"Missing end of execution assembled runs csv file. Expected path {assembled_runs_csv}."
            )

        with assembled_runs_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, assembler_software, assembler_version = row
                assembled_runs.add(run_accession)

        for assembly in assemblies:
            if assembly.run.first_accession in qc_failed_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
                    reason=qc_failed_runs[assembly.run.first_accession],
                )
            elif assembly.run.first_accession in assembled_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    unset_statuses=[
                        analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED
                    ],
                )
                update_assembly_metadata(miassembler_outdir, assembly, assembler)
            else:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason="The assembly is missing from the pipeline end-of-run reports",
                )
