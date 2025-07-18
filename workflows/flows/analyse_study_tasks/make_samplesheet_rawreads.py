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


FASTQ_FTPS = analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS
METADATA__FASTQ_FTPS = f"{analyses.models.Run.metadata.field.name}__{FASTQ_FTPS}"


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
def make_samplesheet_rawreads(
    mgnify_study: analyses.models.Study,
    rawreads_analyses: QuerySet,
) -> (Path, str):
    """
    Makes a samplesheet CSV file for a set of WGS raw-reads analyses, suitable for raw-reads pipeline.
    :param mgnify_study: MGYS study
    :param rawreads_analyses: QuerySet of the raw-reads analyses to be executed
    :return: Tuple of the Path to the samplesheet file, and a hash of the run IDs which is used in the SS filename.
    """

    runs_ids = rawreads_analyses.values_list("run_id", flat=True)
    runs = analyses.models.Run.objects.filter(id__in=runs_ids)
    print(f"Making raw-reads samplesheet for runs {runs_ids}")

    ss_hash = queryset_hash(runs, "id")

    sample_sheet_csv = queryset_to_samplesheet(
        queryset=runs,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_rawreads-v6_{ss_hash}.csv"
        ),
        column_map={
            "study": SamplesheetColumnSource(
                lookup_string="study__ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "sample": SamplesheetColumnSource(
                lookup_string="ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: convert_ena_ftp_to_fire_fastq(ftps[0]),
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: (
                    convert_ena_ftp_to_fire_fastq(ftps[1]) if len(ftps) > 1 else ""
                ),
            ),
            "library_layout": SamplesheetColumnSource(
                pass_whole_object=True,
                renderer=lambda run: run.metadata_preferring_inferred.get(
                    analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                ),
            ),
            "library_strategy": SamplesheetColumnSource(
                lookup_string=f"{analyses.models.Run.metadata.field.name}__library_strategy",
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
    return sample_sheet_csv, ss_hash
