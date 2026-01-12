import csv
from pathlib import Path
from textwrap import dedent as _

from django.db.models import QuerySet
from prefect import task
from prefect.artifacts import create_table_artifact

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.nextflow_utils.samplesheets import (
    queryset_to_samplesheet,
    SamplesheetColumnSource,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.views import encode_samplesheet_path

FASTQ_FTPS = analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS
METADATA__FASTQ_FTPS = f"{analyses.models.Run.metadata.field.name}__{FASTQ_FTPS}"


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
def make_samplesheet_rawreads(runs: QuerySet, samplesheet_path: Path) -> Path:
    """
    Makes a samplesheet CSV file for a set of WGS raw-reads analyses, suitable for raw-reads pipeline.
    :param mgnify_study: MGYS study
    :param rawreads_analyses: QuerySet of the raw-reads analyses to be executed
    :return: Tuple of the Path to the samplesheet file, and a hash of the run IDs which is used in the SS filename.
    """

    sample_sheet_csv = queryset_to_samplesheet(
        queryset=runs,
        filename=samplesheet_path,
        column_map={
            "sample": SamplesheetColumnSource(
                lookup_string="ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: ftps[0],
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: (ftps[1] if len(ftps) > 1 else ""),
            ),
            "single_end": SamplesheetColumnSource(
                pass_whole_object=True,
                renderer=lambda run: (
                    "true"
                    if run.metadata_preferring_inferred.get(
                        analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                    )
                    == "SINGLE"
                    else "false"
                ),
            ),
            "instrument_platform": SamplesheetColumnSource(
                lookup_string=f"{analyses.models.Run.metadata.field.name}__instrument_platform",
            ),
        },
        bludgeon=True,
    )

    with open(sample_sheet_csv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="rawreads-v6-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of rawreads-v6.
            Saved to `{sample_sheet_csv}`
            **Warning!** This table is the *initial* content of the samplesheet, when it was first made. Any edits made since are not shown here.
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_csv)})
            """
        ),
    )
    return sample_sheet_csv
