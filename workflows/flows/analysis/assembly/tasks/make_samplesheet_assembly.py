import csv
import uuid
from pathlib import Path

from django.db.models import QuerySet
from prefect import task, get_run_logger

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.ena_utils.analysis import ENAAnalysisFields
from workflows.models import (
    AssemblyAnalysisBatch,
    AssemblyAnalysisPipeline,
    AssemblyAnalysisPipelineStatus,
)
from workflows.nextflow_utils.samplesheets import (
    queryset_hash,
    queryset_to_samplesheet,
    SamplesheetColumnSource,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash


@task(cache_key_fn=context_agnostic_task_input_hash)
def make_samplesheet_assembly(
    mgnify_study: analyses.models.Study,
    assembly_analyses: QuerySet,
    output_dir: Path = None,
) -> (Path, str):
    """
    Makes a samplesheet CSV file for a set of assembly analyses, suitable for the assembly analysis pipeline.

    The samplesheet is stored in the batch working directory, not in a temporary location.

    :param mgnify_study: MGYS study
    :param assembly_analyses: QuerySet of the assembly analyses to be executed
    :param output_dir: Directory where the samplesheet should be saved. If None, uses default workdir.
    :return: Tuple of the Path to the samplesheet file, and a hash of the assembly IDs which is used in the SS filename.
    """

    logger = get_run_logger()

    assembly_ids = assembly_analyses.values_list("assembly_id", flat=True)
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    logger.info(f"Making assembly samplesheet for assemblies {assembly_ids}")

    ss_hash = queryset_hash(assemblies, "id")

    # Use provided output_dir or fail back to the default workdir
    if output_dir is None:
        output_dir = Path(EMG_CONFIG.slurm.default_workdir)
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    sample_sheet_csv = queryset_to_samplesheet(
        queryset=assemblies,
        filename=output_dir
        / f"{mgnify_study.ena_study.accession}_samplesheet_assembly-v6_{ss_hash}.csv",
        column_map={
            "sample": SamplesheetColumnSource(
                lookup_string="ena_accessions",
                renderer=lambda accessions: accessions[0] if accessions else "",
            ),
            "assembly_fasta": SamplesheetColumnSource(
                lookup_string=f"metadata__{ENAAnalysisFields.GENERATED_FTP}",
                renderer=lambda ftp_path: (
                    # convert_ena_ftp_to_fire_fastq(ftp_path) if ftp_path else ""  # TODO: once ASA supports FIRE
                    "https://"
                    + ftp_path
                ),
            ),
        },
        bludgeon=True,
    )

    logger.info(f"Created assembly samplesheet at {sample_sheet_csv}")
    return sample_sheet_csv, ss_hash


@task
def make_samplesheet_for_map(
    assembly_analysis_batch_id: uuid.UUID,
    analysis_batch_job_ids: list[int],
    output_dir: Path = None,
) -> (Path, str):
    """
    Makes a samplesheet for a batch of assembly analyses for MAP.

    Uses the provided analysis_batch_job_ids to select specific analyses from the batch.
    The task validates that mandatory ASA files (filtered contigs FASTA and predicted CDS GFF)
    exist for each selected analysis.

    The samplesheet has the following columns:
    - sample: the assembly first_accession
    - assembly: the assembly filtered contigs fasta (from ASA)
    - user_proteins_gff: the assembly analysis predicted CDS GFF file (from ASA)
    - virify_gff: the VIRify GFF file (optional, only included if VIRify is COMPLETED and file exists)

    :param assembly_analysis_batch_id: The ID of the AssemblyAnalysisBatch
    :param analysis_batch_job_ids: List of AssemblyAnalysisBatchAnalysis IDs to include in the samplesheet
    :param output_dir: Directory where the samplesheet should be saved. If None, uses default workdir.
    :return: Tuple of the Path to the samplesheet file, and a hash of the assembly IDs.
    """
    logger = get_run_logger()
    batch = AssemblyAnalysisBatch.objects.get(id=assembly_analysis_batch_id)
    mgnify_study = batch.study

    # Use the provided IDs to get the specific batch analyses to process
    # select_related('analysis__assembly') avoids N+1 queries in the loop
    batch_analyses = batch.batch_analyses.filter(
        id__in=analysis_batch_job_ids
    ).select_related("analysis__assembly")

    if not batch_analyses.exists():
        logger.warning(
            f"No analyses found matching IDs {analysis_batch_job_ids} for MAP"
        )
        return None, None

    # Get assemblies for hashing
    assembly_ids = [ba.analysis.assembly_id for ba in batch_analyses]
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    ss_hash = queryset_hash(assemblies, "id")

    # Use provided output_dir or fall back to the default workdir
    if output_dir is None:
        output_dir = Path(EMG_CONFIG.slurm.default_workdir)
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    # Get pipeline workspaces from batch
    asa_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.ASA)
    virify_workspace = batch.get_pipeline_workspace(AssemblyAnalysisPipeline.VIRIFY)

    # Create a filename for the MAP samplesheet
    map_samplesheet_filename = (
        output_dir / f"{mgnify_study.ena_study.accession}_samplesheet_map_{ss_hash}.csv"
    )

    # Create the samplesheet with the required columns
    with open(map_samplesheet_filename, "w", newline="") as csvfile:
        fieldnames = ["sample", "assembly", "user_proteins_gff", "virify_gff"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for batch_analysis in batch_analyses:
            analysis = batch_analysis.analysis
            assembly = analysis.assembly
            assembly_accession = assembly.first_accession

            # Mandatory ASA files
            assembly_fasta = (
                asa_workspace
                / assembly_accession
                / EMG_CONFIG.assembly_analysis_pipeline.qc_folder
                / f"{assembly_accession}_filtered_contigs.fasta.gz"
            )
            user_proteins_gff = (
                asa_workspace
                / assembly_accession
                / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
                / f"{assembly_accession}_predicted_cds.gff.gz"
            )

            # Validate mandatory files
            if not assembly_fasta.exists() or not user_proteins_gff.exists():
                raise ValueError(
                    f"Mandatory ASA files missing for {assembly_accession}"
                )

            # VIRify GFF is optional: only provide path if COMPLETED and file exists
            virify_gff_str = ""
            if batch_analysis.virify_status == AssemblyAnalysisPipelineStatus.COMPLETED:
                virify_gff = (
                    virify_workspace
                    / assembly_accession
                    / EMG_CONFIG.virify_pipeline.final_gff_folder
                    / f"{assembly_accession}_virify.gff.gz"
                )
                if virify_gff.exists():
                    virify_gff_str = str(virify_gff)
                else:
                    logger.warning(
                        f"VIRify GFF not found for {assembly_accession} despite COMPLETED status"
                    )

            writer.writerow(
                {
                    "sample": assembly_accession,
                    "assembly": str(assembly_fasta),
                    "user_proteins_gff": str(user_proteins_gff),
                    "virify_gff": virify_gff_str,
                }
            )

    logger.info(f"Created MAP samplesheet at {map_samplesheet_filename}")
    return map_samplesheet_filename, ss_hash
