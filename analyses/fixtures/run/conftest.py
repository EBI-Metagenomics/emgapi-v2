import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def raw_read_run(raw_reads_mgnify_study, raw_reads_mgnify_sample):
    runs_list = [
        {"accession": "SRR6180434", "experiment_type": "METAG"},
        {"accession": "SRR6180435", "experiment_type": "METAG"},
        {"accession": "SRR6704248", "experiment_type": "AMPLI"},
    ]
    run_objects = []
    for sample, run in zip(raw_reads_mgnify_sample, runs_list):
        run_obj, _ = mg_models.Run.objects.get_or_create(
            ena_accessions=[run["accession"]],
            study=raw_reads_mgnify_study,
            ena_study=raw_reads_mgnify_study.ena_study,
            sample=sample,
            experiment_type=run["experiment_type"],
            metadata={
                mg_models.Run.CommonMetadataKeys.FASTQ_FTPS: [
                    "ftp://example.org/fastq1",
                    "ftp://example.org/fastq2",
                ]
            },
        )
        run_objects.append(run_obj)
    return run_objects
