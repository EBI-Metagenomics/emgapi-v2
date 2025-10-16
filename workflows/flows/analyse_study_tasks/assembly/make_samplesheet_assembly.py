import csv
from pathlib import Path

from django.db.models import QuerySet
from prefect import task

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.ena_utils.analysis import ENAAnalysisFields
from workflows.nextflow_utils.samplesheets import (
    queryset_hash,
    queryset_to_samplesheet,
    SamplesheetColumnSource,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
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

    assembly_ids = assembly_analyses.values_list("assembly_id", flat=True)
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    print(f"Making assembly samplesheet for assemblies {assembly_ids}")

    ss_hash = queryset_hash(assemblies, "id")

    # Use provided output_dir or fall back to default workdir
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
                    "http://"
                    + ftp_path
                ),
            ),
        },
        bludgeon=True,
    )

    print(f"Created assembly samplesheet at {sample_sheet_csv}")
    return sample_sheet_csv, ss_hash


@task(cache_key_fn=context_agnostic_task_input_hash)
def make_samplesheet_for_map(
    batch: "analyses.models.AssemblyAnalysisBatch",
    output_dir: Path = None,
) -> (Path, str):
    """
    Makes a samplesheet for a batch of assembly analyses for MAP.

    Uses filesystem structure instead of database queries to find required files.
    Assumes results follow the expected pipeline structure:
    - ASA CDS GFF: {asa_workspace}/{assembly_accession}/cds/{assembly_accession}_predicted_cds.gff.gz
    - VIRify GFF: {virify_workspace}/{assembly_accession}/08-final/gff/{assembly_accession}_virify.gff.gz

    The samplesheet has the following columns:
    - sample: the assembly first_accession
    - assembly: the assembly fasta (from ENA metadata)
    - user_proteins_gff: the assembly analysis CDS GFF file
    - virify_gff: the VIRify GFF file

    :param batch: The AssemblyAnalysisBatch containing analyses to process
    :param output_dir: Directory where the samplesheet should be saved. If None, uses default workdir.
    :return: Tuple of the Path to the samplesheet file, and a hash of the assembly IDs which is used in the SS filename.
    """
    mgnify_study = batch.study
    assembly_analyses = list(
        batch.assembly_analysis_set.select_related("assembly").all()
    )

    assembly_ids = [analysis.assembly_id for analysis in assembly_analyses]
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    print(f"Making MAP samplesheet for assemblies {assembly_ids}")

    ss_hash = queryset_hash(assemblies, "id")

    # Use provided output_dir or fall back to default workdir
    if output_dir is None:
        output_dir = Path(EMG_CONFIG.slurm.default_workdir)
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    # Get pipeline workspaces from batch
    asa_workspace = batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.ASA
    )
    virify_workspace = batch.get_pipeline_workspace(
        analyses.models.AssemblyAnalysisBatch.PipelineType.VIRIFY
    )

    # Create a filename for the MAP samplesheet
    map_samplesheet_filename = (
        output_dir / f"{mgnify_study.ena_study.accession}_samplesheet_map_{ss_hash}.csv"
    )

    # Create the samplesheet with the required columns
    with open(map_samplesheet_filename, "w", newline="") as csvfile:
        fieldnames = ["sample", "assembly", "user_proteins_gff", "virify_gff"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for analysis in assembly_analyses:
            assembly = analysis.assembly
            assembly_accession = assembly.first_accession

            # Get the assembly fasta for the assembly column (from ENA metadata)
            assembly_ftp_path = assembly.metadata.get(
                ENAAnalysisFields.GENERATED_FTP, ""
            )
            assembly_fasta = "http://" + assembly_ftp_path if assembly_ftp_path else ""

            # Construct expected file paths based on the pipeline structure
            user_proteins_gff = (
                asa_workspace
                / assembly_accession
                / EMG_CONFIG.assembly_analysis_pipeline.cds_folder
                / f"{assembly_accession}_predicted_cds.gff.gz"
            )

            virify_gff = (
                virify_workspace
                / assembly_accession
                / EMG_CONFIG.virify_pipeline.final_gff_folder
                / f"{assembly_accession}_virify.gff.gz"
            )

            # Verify files exist
            if not user_proteins_gff.exists():
                raise ValueError(
                    f"Can't run MAP for {analysis.accession} because CDS GFF file not found at {user_proteins_gff}"
                )

            if not virify_gff.exists():
                raise ValueError(
                    f"Can't run MAP for {analysis.accession} because VIRify GFF file not found at {virify_gff}"
                )

            print(f"User proteins GFF: {user_proteins_gff}")
            print(f"VIRify GFF: {virify_gff}")

            # Write the row to the samplesheet
            writer.writerow(
                {
                    "sample": assembly_accession,
                    "assembly": assembly_fasta,
                    "user_proteins_gff": str(user_proteins_gff),
                    "virify_gff": str(virify_gff),
                }
            )

    print(f"Created MAP samplesheet at {map_samplesheet_filename}")
    return map_samplesheet_filename, ss_hash
