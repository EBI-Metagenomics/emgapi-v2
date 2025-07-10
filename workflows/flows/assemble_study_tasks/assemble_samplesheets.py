import csv
import json
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from django.db.models import Q
from prefect import flow, get_run_logger, task

from workflows.prefect_utils.build_cli_command import cli_command

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    file_path_shortener,
)
from workflows.prefect_utils.analyses_models_helpers import mark_assembly_status
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)
from workflows.prefect_utils.slurm_policies import (
    ResubmitIfFailedPolicy,
)

# TODO: move to a constants file
HOST_TAXON_TO_REFERENCE_GENOME = {
    "9031": "chicken.fna",  # Gallus gallus
    "8030": "salmon.fna",  # Salmo salar
    "8049": "cod.fna",  # Gadus morhua
    "9823": "pig.fna",  # Sus scrofa
    "9913": "cow.fna",  # Bos taurus
    "10090": "mouse.fna",  # Mus musculus
    "7460": "honeybee.fna",  # Apis mellifera
    "10116": "rat.fna",  # Rattus norvegicus
    "8022": "rainbow_trout.fna",  # Oncorhynchus mykiss
    "9940": "sheep.fna",  # Ovis aries
    "3847": "soybean.fna",  # Glycine max
    "7955": "zebrafish.fna",  # Danio rerio
    "Gallus gallus": "chicken.fna",
    "Salmo salar": "salmon.fna",
    "Gadus morhua": "cod.fna",
    "Sus scrofa": "pig.fna",
    "Bos taurus": "cow.fna",
    "Mus musculus": "mouse.fna",
    "Apis mellifera": "honeybee.fna",
    "Rattus norvegicus": "rat.fna",
    "Oncorhynchus mykiss": "rainbow_trout.fna",
    "Ovis aries": "sheep.fna",
    "Glycine max": "soybean.fna",
    "Danio rerio": "zebrafish.fna",
}


@task
def get_reference_genome(
    reads_study: analyses.models.Study,
) -> Optional[str]:
    logger = get_run_logger()
    first_run_with_host_taxon = reads_study.runs.filter(
        Q(
            **{
                f"metadata__{analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID}__isnull": False
            }
        )
        | Q(
            **{
                f"metadata__{analyses.models.Run.CommonMetadataKeys.HOST_SCIENTIFIC_NAME}__isnull": False
            }
        )
    ).first()
    if not first_run_with_host_taxon:
        logger.warning(f"Found no run in {reads_study} with host taxon info")
        return None
    logger.info(
        f"Using run {first_run_with_host_taxon} for determining host reference genome"
    )

    reference_genome = None
    host_taxon = first_run_with_host_taxon.metadata.get(
        analyses.models.Run.CommonMetadataKeys.HOST_TAX_ID
    ) or first_run_with_host_taxon.metadata.get(
        analyses.models.Run.CommonMetadataKeys.HOST_SCIENTIFIC_NAME
    )
    if host_taxon:
        logger.info(f"Host taxon is {host_taxon}")
        reference_genome = HOST_TAXON_TO_REFERENCE_GENOME.get(str(host_taxon), None)
        logger.info(f"Reference genome will be: {reference_genome}")

    return reference_genome


@task
def update_assembly_metadata(
    miassembler_outdir: Path,
    assembly: analyses.models.Assembly,
) -> None:
    """
    Update assembly with post-assembly metadata like assembler and coverage.
    """
    logger = get_run_logger()
    run_accession = assembly.run.first_accession
    study_accession = assembly.reads_study.ena_study.accession

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
        if key not in coverage_report:
            logger.warning(f"No '{key}' found in {coverage_report_path}")
        assembly.metadata[key] = coverage_report.get(key)

    logger.info(f"Assembly metadata of {assembly} is now {assembly.metadata}")

    assembly.save()


@task
def update_assemblers_and_contaminant_ref_of_assemblies_from_samplesheet(
    samplesheet_df: pd.DataFrame,
):
    """
    Updates the assemblers and contaminant reference associated with each assembly, based on what is in the samplesheet.
    This is done because a samplesheet CSV may have been edited since first created.
    :param samplesheet_df: Pandas dataframe of the samplesheet content
    :return:
    """
    logger = get_run_logger()
    for _, assembly_row in samplesheet_df.iterrows():
        latest_assembly: analyses.models.Assembly = (
            analyses.models.Assembly.objects.filter(
                run__ena_accessions__0=assembly_row["reads_accession"]
            )
            .order_by("-created_at")
            .first()
        )
        if not latest_assembly:
            logger.warning(
                f"Could not find unique assembly for {assembly_row['reads_accession']}"
            )
            continue

        update = False
        try:
            assembler_in_samplesheet = assembly_row["assembler"]
            latest_assembly.assembler = (
                analyses.models.Assembler.objects.filter(
                    name__iexact=assembler_in_samplesheet
                )
                .order_by("-version")
                .first()
            )
            update = True
        except KeyError:
            logger.warning("Could not find assembler in samplesheet")

        try:
            contaminant_reference = assembly_row.get("contaminant_reference")
            if contaminant_reference and not pd.isna(contaminant_reference):
                latest_assembly.metadata[
                    analyses.models.Assembly.CommonMetadataKeys.CONTAMINANT_REFERENCE
                ] = contaminant_reference
                update = True
        except KeyError:
            logger.warning("Could not find contaminant reference in samplesheet")

        if update:
            latest_assembly.save()


@flow(flow_run_name="Assemble {samplesheet_csv}", persist_result=True)
def run_assembler_for_samplesheet(
    mgnify_study: analyses.models.Study, samplesheet_csv: Path, samplesheet_hash: str
):
    samplesheet_df = pd.read_csv(samplesheet_csv, sep=",")
    assemblies: Iterable[analyses.models.Assembly] = (
        mgnify_study.assemblies_reads.filter(
            run__ena_accessions__0__in=samplesheet_df["reads_accession"]
        )
    )

    update_assemblers_and_contaminant_ref_of_assemblies_from_samplesheet(samplesheet_df)

    for assembly in assemblies:
        # Mark assembly as started
        mark_assembly_status(
            assembly,
            status=assembly.AssemblyStates.ASSEMBLY_STARTED,
            unset_statuses=[
                assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                assembly.AssemblyStates.ASSEMBLY_FAILED,
            ],
        )

    miassembler_outdir = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / f"{mgnify_study.ena_study.accession}_miassembler"
        / samplesheet_hash
    )

    command = cli_command(
        [
            ("nextflow", "run", EMG_CONFIG.assembler.assembly_pipeline_repo),
            ("-r", EMG_CONFIG.assembler.miassemebler_git_revision),
            "-latest",  # Pull changes from GitHub
            ("-profile", EMG_CONFIG.assembler.miassembler_nf_profile),
            ("-config", EMG_CONFIG.assembler.miassembler_config_file),
            "-resume",
            ("--samplesheet", samplesheet_csv),
            mgnify_study.is_private and "--private_study",
            ("--outdir", miassembler_outdir),
            ("--workdir", miassembler_outdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
        ]
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
            resubmit_policy=ResubmitIfFailedPolicy,
            working_dir=miassembler_outdir,
        )
    except ClusterJobFailedException:
        for assembly in assemblies:
            mark_assembly_status(
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
                mark_assembly_status(
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
                mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
                    reason=qc_failed_runs[assembly.run.first_accession],
                )
            elif assembly.run.first_accession in assembled_runs:
                mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    unset_statuses=[assembly.AssemblyStates.ASSEMBLY_FAILED],
                )
                update_assembly_metadata(miassembler_outdir, assembly)
            else:
                mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason="The assembly is missing from the pipeline end-of-run reports",
                )
