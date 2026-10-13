import csv
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional

import httpx
from django.db import transaction
from prefect import get_run_logger
from prefect.concurrency.sync import concurrency
from prefect.deployments import run_deployment
from prefect.runtime import flow_run

from activate_django_first import EMG_CONFIG

from analyses.models import Analysis, Assembly
from proteins.ingestion.gene_calling import accession_gene_calling
from workflows.ena_utils.analysis import ENAAnalysisFields
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisBatchAnalysis,
    AssemblyAnalysisPipelineStatus,
    AssemblyGeneCalling,
)
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task
from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy

ASSEMBLY_BATCH_DEPLOYMENT = "run-assembly-batch/run_assembly_batch_deployment"

COMPLETED = AssemblyAnalysisPipelineStatus.COMPLETED
FAILED = AssemblyAnalysisPipelineStatus.FAILED
RUNNING = AssemblyAnalysisPipelineStatus.RUNNING


class GeneCallingAlreadyRunning(Exception):
    """Another flow run works on the same gene calling."""


class GeneCallingRefused(Exception):
    """The analysis of the assembly cannot take its proteins from a gene calling."""


@task(task_run_name="Claim the gene calling of {analysis_id}")
def claim_gene_calling(analysis_id: int, workspace_dir: str) -> int:
    """The tracker of the gene calling for the analysis, with the current pipeline version. Claims it for this flow run.

    :param analysis_id: The id of the V6 assembly analysis that takes the proteins.
    :param workspace_dir: The root of the workspaces.
    :return: The id of the AssemblyGeneCalling.
    :raises GeneCallingRefused: When the analysis has started without a gene calling.
    :raises GeneCallingAlreadyRunning: When another flow run has a step of it running.
    """
    analysis = Analysis.objects.select_related("assembly", "study__ena_study").get(
        id=analysis_id
    )
    assembly = analysis.assembly
    version = EMG_CONFIG.gene_calling_pipeline.pipeline_version
    with transaction.atomic():
        gene_calling = (
            AssemblyGeneCalling.objects.select_for_update()
            .filter(analysis=analysis)
            .first()
        )
        if gene_calling is None:
            # Only an analysis that has not started can take its proteins from a gene calling.
            started = AssemblyAnalysisBatchAnalysis.all_objects.filter(
                analysis=analysis
            ).exists() or analysis.status.get(
                Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
            )
            if started:
                raise GeneCallingRefused(
                    f"{analysis.accession} has started without a gene calling"
                )
            gene_calling = AssemblyGeneCalling.objects.create(
                assembly=assembly,
                analysis=analysis,
                pipeline_version=version,
                workspace_dir=str(
                    Path(workspace_dir)
                    / analysis.study.ena_study.accession
                    / f"genecalling_{version}"
                    / assembly.first_accession
                ),
            )
        elif gene_calling.pipeline_version != version:
            raise GeneCallingRefused(
                f"{analysis.accession} has a gene calling with version {gene_calling.pipeline_version}, not {version}"
            )
        running = RUNNING in (
            gene_calling.gene_calling_status,
            gene_calling.accessioning_status,
        )
        if running and gene_calling.prefect_flow_run_id != str(flow_run.id):
            # A crashed run leaves the status RUNNING: set it to FAILED in the admin to start again.
            raise GeneCallingAlreadyRunning(
                f"{gene_calling} has a step running in flow run {gene_calling.prefect_flow_run_id}"
            )
        gene_calling.prefect_flow_run_id = str(flow_run.id)
        gene_calling.save()
    get_run_logger().info(
        f"{gene_calling} for {analysis.accession}, in {gene_calling.workspace_dir}"
    )
    return gene_calling.id


def mark(gene_calling: AssemblyGeneCalling, step: str, status: str, error=None):
    """Sets the status of one step, and the error if it failed.

    :param gene_calling: The tracker.
    :param step: "gene_calling" or "accessioning".
    :param status: An AssemblyAnalysisPipelineStatus.
    :param error: The exception, when the step failed.
    """
    setattr(gene_calling, f"{step}_status", status)
    gene_calling.last_error = str(error) if error else None
    gene_calling.save()


@task(task_run_name="Download the assembly of {gene_calling_id}", retries=2)
def download_assembly(gene_calling_id: int) -> Path:
    """Downloads the contigs of the assembly from ENA, unless they are already in the workspace.

    The gene calling pipeline needs a local FASTA, and the accessioning reads the same file.

    :param gene_calling_id: The id of the AssemblyGeneCalling.
    :return: The local FASTA.
    """
    gene_calling = AssemblyGeneCalling.objects.select_related("assembly").get(
        id=gene_calling_id
    )
    path = gene_calling.assembly_fasta()
    if path.exists():
        return path
    url = "https://" + gene_calling.assembly.metadata[ENAAnalysisFields.GENERATED_FTP]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as response:
        response.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
    os.replace(tmp, path)
    get_run_logger().info(f"Downloaded {url} to {path}")
    return path


@task(task_run_name="Run the gene calling pipeline on {gene_calling_id}")
def run_gene_calling_pipeline(gene_calling_id: int) -> None:
    """Runs genecalling-pipeline on one assembly: Pyrodigal, FragGeneScanRS and their merge.

    :param gene_calling_id: The id of the AssemblyGeneCalling.
    :raises FileNotFoundError: When the pipeline did not write the expected outputs.
    """
    logger = get_run_logger()
    config = EMG_CONFIG.gene_calling_pipeline
    gene_calling = AssemblyGeneCalling.objects.select_related("assembly").get(
        id=gene_calling_id
    )
    erz = gene_calling.assembly.first_accession
    outdir = Path(gene_calling.workspace_dir)

    samplesheet = outdir / "samplesheet.csv"
    with open(samplesheet, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sample", "fasta"])
        writer.writeheader()
        writer.writerow({"sample": erz, "fasta": gene_calling.assembly_fasta()})

    nextflow_workdir = (
        Path(EMG_CONFIG.slurm.default_nextflow_workdir)
        / f"{config.pipeline_name}_{config.pipeline_version}"
        / erz
    )
    nextflow_workdir.mkdir(parents=True, exist_ok=True)
    command = cli_command(
        [
            ("nextflow", "run", config.pipeline_repo),
            ("-c", config.pipeline_config_file),
            ("-r", config.pipeline_git_revision),
            ("-profile", config.pipeline_nf_profile),
            "-resume",
            ("--input", samplesheet),
            ("--outdir", outdir),
            ("-work-dir", nextflow_workdir),
            EMG_CONFIG.slurm.use_nextflow_tower and "-with-tower",
            ("-ansi-log", "false"),
        ]
    )

    mark(gene_calling, "gene_calling", RUNNING)
    try:
        run_cluster_job(
            name=f"Gene calling of {erz}",
            command=command,
            expected_time=timedelta(days=config.pipeline_time_limit_days),
            memory=f"{config.nextflow_master_job_memory_gb}G",
            environment="ALL,TOWER_WORKSPACE_ID"
            + (",TOWER_ACCESS_TOKEN" if EMG_CONFIG.slurm.use_nextflow_tower else ""),
            input_files_to_hash=[samplesheet],
            working_dir=outdir,
            resubmit_policy=ResubmitAlwaysPolicy,  # Let Nextflow handle resubmissions
        )
        files = gene_calling.outputs()
        if missing := [path for path in files if path and not path.exists()]:
            raise FileNotFoundError(
                f"The gene calling pipeline did not write {', '.join(map(str, missing))}"
            )
    except Exception as e:
        mark(gene_calling, "gene_calling", FAILED, e)
        raise
    mark(gene_calling, "gene_calling", COMPLETED)
    logger.info(f"Gene calling of {erz} completed in {outdir}")


@task(task_run_name="Accession the proteins of {gene_calling_id}")
def accession_proteins(gene_calling_id: int) -> None:
    """Gives MGYPs to the proteins of the gene calling, stages their rows, and writes the FAA with MGYPs.

    Holds a slot of the accessioning concurrency limit, because each run uses up to 16 connections
    to the MGYP database.

    :param gene_calling_id: The id of the AssemblyGeneCalling.
    """
    logger = get_run_logger()
    gene_calling = AssemblyGeneCalling.objects.select_related(
        "assembly", "analysis"
    ).get(id=gene_calling_id)
    mark(gene_calling, "accessioning", RUNNING)
    try:
        with concurrency(
            EMG_CONFIG.proteins.accessioning_concurrency_limit, occupy=1, strict=True
        ):
            result = accession_gene_calling(
                gene_calling.assembly.first_accession,
                gene_calling.pipeline_version,
                gene_calling.analysis.accession,
                gene_calling.outputs(),
                gene_calling.proteins_with_mgyps(),
            )
    except Exception as e:
        mark(gene_calling, "accessioning", FAILED, e)
        raise
    gene_calling.assembly_proteins_id = result.assembly_proteins_id
    mark(gene_calling, "accessioning", COMPLETED)
    logger.info(
        f"{result.proteins} proteins of {gene_calling} have MGYPs, in {result.proteins_with_mgyps}"
    )


@task(task_run_name="Start the assembly analysis of {gene_calling_id}")
def start_assembly_analysis(
    gene_calling_id: int,
    workspace_dir: str,
    contaminant_reference: Optional[str] = None,
) -> None:
    """Puts the analysis in a batch of its own and starts the assembly analysis batch flow. Does not wait.

    When the analysis is already in a batch (a rerun of this flow), it starts nothing.

    :param gene_calling_id: The id of the AssemblyGeneCalling.
    :param workspace_dir: The root of the workspaces.
    :param contaminant_reference: Optional contaminant reference for ASA.
    """
    logger = get_run_logger()
    analysis = (
        AssemblyGeneCalling.objects.select_related("analysis__study")
        .get(id=gene_calling_id)
        .analysis
    )
    if AssemblyAnalysisBatchAnalysis.all_objects.filter(analysis=analysis).exists():
        logger.info(f"{analysis.accession} is already in a batch; nothing to start")
        return
    # get_or_create_batches_for_study leaves out analyses with a gene calling, so this makes a
    # batch for this one analysis directly.
    batch = AssemblyAnalysisBatch.objects._create_for_analyses(
        analysis.study,
        [analysis.id],
        workspace_dir=Path(workspace_dir),
        contaminant_reference=contaminant_reference,
    )
    run_deployment(
        name=ASSEMBLY_BATCH_DEPLOYMENT,
        parameters={"assembly_analyses_batch_id": batch.id},
        timeout=0,
    )
    logger.info(
        f"Started the assembly analysis batch {batch.id} for {analysis.accession}"
    )


@flow(flow_run_name="Gene call assembly: {assembly_accession}")
def gene_call_assembly(
    assembly_accession: str,
    workspace_dir: str = EMG_CONFIG.slurm.default_workdir,
    contaminant_reference: Optional[str] = None,
) -> None:
    """Gene calling + assembly analysis of one assembly.

    Makes the V6 assembly analysis (MGYA), runs genecalling-pipeline on the assembly, gives MGYPs to the
    proteins, writes the FAA with MGYPs, and starts the assembly analysis batch of the analysis.

    Each step is skipped when its status is COMPLETED, so the flow can run again after a failure.
    The tracker is workflows.AssemblyGeneCalling.

    :param assembly_accession: The ERZ. The assembly must be in emgapi-v2, with an assembly study.
    :param workspace_dir: The root of the workspaces. Defaults to the configured SLURM default workdir.
    :param contaminant_reference: Optional contaminant reference for the ASA decontamination.
    """
    logger = get_run_logger()
    assembly = Assembly.objects.get_by_accession(assembly_accession)
    study = assembly.assembly_study
    if study is None:
        raise ValueError(f"The assembly {assembly_accession} has no assembly study")
    analyses = create_analyses_for_assemblies(
        study.id, [assembly_accession], pipeline=Analysis.PipelineVersions.v6
    )
    if len(analyses) != 1:
        raise GeneCallingRefused(
            f"{assembly_accession} has {len(analyses)} V6 analyses in {study.accession}, not one"
        )

    gene_calling_id = claim_gene_calling(analyses[0].id, workspace_dir)
    gene_calling = AssemblyGeneCalling.objects.get(id=gene_calling_id)
    if gene_calling.accessioning_status != COMPLETED:
        download_assembly(gene_calling_id)
        if gene_calling.gene_calling_status != COMPLETED:
            run_gene_calling_pipeline(gene_calling_id)
        accession_proteins(gene_calling_id)

    # TODO (ASA team): ASA does not take the FAA with MGYPs yet. Until it does,
    # make_samplesheet_assembly gives ASA the assembly only, and ASA calls the genes again.
    start_assembly_analysis(gene_calling_id, workspace_dir, contaminant_reference)
    logger.info(
        f"{assembly_accession}: gene calling done, handed over to {analyses[0].accession}"
    )
