import csv
from pathlib import Path
from textwrap import dedent as _
from typing import List

from django.db.models import QuerySet
from prefect import task
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG

import analyses.models
from analyses.base_models.with_downloads_models import DownloadFileType
from workflows.ena_utils.analysis import ENAAnalysisFields
from workflows.nextflow_utils.samplesheets import (
    queryset_hash,
    queryset_to_samplesheet,
    SamplesheetColumnSource,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.views import encode_samplesheet_path


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
def make_samplesheet_assembly(
    mgnify_study: analyses.models.Study,
    assembly_analyses: QuerySet,
) -> (Path, str):
    """
    Makes a samplesheet CSV file for a set of assembly analyses, suitable for the assembly analysis pipeline.
    :param mgnify_study: MGYS study
    :param assembly_analyses: QuerySet of the assembly analyses to be executed
    :return: Tuple of the Path to the samplesheet file, and a hash of the assembly IDs which is used in the SS filename.
    """

    assembly_ids = assembly_analyses.values_list("assembly_id", flat=True)
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    print(f"Making assembly samplesheet for assemblies {assembly_ids}")

    ss_hash = queryset_hash(assemblies, "id")

    sample_sheet_csv = queryset_to_samplesheet(
        queryset=assemblies,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
                 / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_assembly-v6_{ss_hash}.csv"
        ),
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

    with open(sample_sheet_csv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="assembly-v6-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of assembly-v6.
            Saved to `{sample_sheet_csv}`
            **Warning!** This table is the *initial* content of the samplesheet, when it was first made. Any edits made since are not shown here.
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_csv)})
            """
        ),
    )
    return sample_sheet_csv, ss_hash


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
def make_samplesheet_for_map(
    mgnify_study: analyses.models.Study,
    assembly_analyses: List[analyses.models.Analysis],
) -> (Path, str):
    """
    Makes a samplesheet CSV file for a set of assembly analyses, suitable for the MAP pipeline.
    The samplesheet has the following columns:
    - sample: the assembly first_accession
    - assembly: the assembly fasta
    - user_proteins_gff: the assembly analysis CDC gff file (from the downloads)
    - virify_gff: the Virify GFF file

    :param mgnify_study: MGYS study
    :param assembly_analyses: QuerySet of the assembly analyses to be executed
    :return: Tuple of the Path to the samplesheet file, and a hash of the assembly IDs which is used in the SS filename.
    """

    assembly_ids = [assembly.assembly_id for assembly in assembly_analyses]
    assemblies = analyses.models.Assembly.objects.filter(id__in=assembly_ids)
    print(f"Making MAP samplesheet for assemblies {assembly_ids}")

    ss_hash = queryset_hash(assemblies, "id")

    # Create a filename for the MAP samplesheet
    map_samplesheet_filename = Path(EMG_CONFIG.slurm.default_workdir) / Path(
        f"{mgnify_study.ena_study.accession}_samplesheet_map_{ss_hash}.csv"
    )

    # Create the samplesheet with the required columns
    with open(map_samplesheet_filename, "w", newline="") as csvfile:
        fieldnames = ["sample", "assembly", "user_proteins_gff", "virify_gff"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for assembly in assemblies:
            # Get the assembly analysis for this assembly
            analysis = next(
                (a for a in assembly_analyses if a.assembly_id == assembly.id), None
            )
            if not analysis:
                print(f"No analysis found for assembly {assembly.id}. Skipping.")
                continue

            # Get the assembly fasta for the assembly column
            assembly_ftp_path = assembly.metadata.get(
                ENAAnalysisFields.GENERATED_FTP, ""
            )
            assembly_fasta = "http://" + assembly_ftp_path if assembly_ftp_path else ""

            # Get the assembly analysis CDS GFF file for the user_proteins_gff column
            # This is expected to be in the analysis downloads with a specific pattern
            user_proteins_gff = ""
            virify_gff = ""
            for download in analysis.downloads_as_objects:
                print(download.file_type)
                print(download.download_group)
                if (
                    download.file_type == DownloadFileType.GFF
                    and download.download_group
                    == analyses.models.Analysis.CODING_SEQUENCES
                ):
                    user_proteins_gff = download.path
                if (
                    download.file_type == DownloadFileType.GFF
                    and download.download_group == analyses.models.Analysis.VIRIFY
                ):
                    virify_gff = download.path

            print(f"User proteins GFF {user_proteins_gff}")
            print(f"VIRIfy GFF {virify_gff}")

            if not user_proteins_gff:
                # TODO: is this the best way to handle this? Or should we just skip this row?
                raise ValueError(
                    f"Can't run MAP for {analysis} because there is no predicted cds file"
                )

            if not virify_gff:
                # TODO: is this the best way to handle this? Or should we just skip this row?
                raise ValueError(
                    f"Can't run MAP for {analysis} because there is no VIRify GFF file"
                )

            # Write the row to the samplesheet
            writer.writerow(
                {
                    "sample": assembly.first_accession,
                    "assembly": assembly_fasta,
                    "user_proteins_gff": user_proteins_gff,
                    "virify_gff": virify_gff,
                }
            )

    # Create a table artifact for the samplesheet
    with open(map_samplesheet_filename) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="map-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
                Sample sheet created for run of MAP (Mobilome Annotation Pipeline).
                Saved to `{map_samplesheet_filename}`
                **Warning!** This table is the *initial* content of the samplesheet, when it was first made. Any edits made since are not shown here.
                [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(map_samplesheet_filename)})
                """
        ),
    )
    return map_samplesheet_filename, ss_hash
