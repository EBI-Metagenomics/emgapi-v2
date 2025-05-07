import csv
from pathlib import Path
from textwrap import dedent as _

from django.db.models import QuerySet
from prefect import task
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq
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
    Makes a samplesheet CSV file for a set of assembly analyses, suitable for assembly analysis pipeline.
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
                lookup_string="metadata__ftp_path",
                renderer=lambda ftp_path: (
                    convert_ena_ftp_to_fire_fastq(ftp_path) if ftp_path else ""
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
