import pytest
import responses
import pathlib


from activate_django_first import EMG_CONFIG


from workflows.flows.analyse_study_tasks.shared.dwcr_generator import (
    generate_dwc_ready_summary_for_pipeline_run,
)
from analyses.models import Study


@pytest.mark.django_db(transaction=True)
@responses.activate
def test_dwcr_generator(
    amplicon_analysis_with_downloads,
    raw_reads_mgnify_study: Study,
    prefect_harness,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
):
    amplicon_pipeline_outdir = pathlib.Path(
        amplicon_analysis_with_downloads.pipeline_outdir
    )
    refdb_otus = pathlib.Path(EMG_CONFIG.amplicon_pipeline.refdb_otus_dir)

    # Mock the read_run ENA API requests
    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "run_accession": "SRR1111111",
                "secondary_study_accession": "SRP2222222",
                "sample_accession": "SAMN3333333",
                "instrument_model": "Illumina MiSeq",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "read_run",
                    "includeAccessions": "SRR1111111",
                    "fields": "secondary_study_accession,sample_accession,instrument_model",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "run_accession": "SRR6180434",
                "secondary_study_accession": "SRP4444444",
                "sample_accession": "SAMN5555555",
                "instrument_model": "Illumina MiSeq",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "read_run",
                    "includeAccessions": "SRR6180434",
                    "fields": "secondary_study_accession,sample_accession,instrument_model",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    # mock the sample ENA API requests
    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "lat": 12,
                "lon": 22,
                "collection_date": "2025-05-25",
                "depth": 0.12,
                "center_name": "Devonshire Building",
                "temperature": 12,
                "salinity": 0.12,
                "country": "United Kingdom",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "sample",
                    "includeAccessions": "SAMN3333333",
                    "fields": "lat,lon,collection_date,depth,center_name,temperature,salinity,country",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "lat": 12,
                "lon": 22,
                "collection_date": "2025-05-25",
                "depth": 0.12,
                "center_name": "Devonshire Building",
                "temperature": 12,
                "salinity": 0.12,
                "country": "United Kingdom",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "sample",
                    "includeAccessions": "SAMN5555555",
                    "fields": "lat,lon,collection_date,depth,center_name,temperature,salinity,country",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    generate_dwc_ready_summary_for_pipeline_run(
        raw_reads_mgnify_study.accession,
        amplicon_pipeline_outdir,
        refdb_otus,
        completed_runs_filename,
    )
