import pytest
import responses
from responses import matchers

import analyses.models as mg_models
import ena.models
from workflows.flows.upload_assembly import handle_tpa_study, upload_assembly
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.django_db(transaction=True)
def test_prefect_assembly_upload_flow_assembly_metaspades(
    prefect_harness,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_ena_study,
    raw_reads_mgnify_study,
    raw_read_run,
    mgnify_assemblies_completed,
    assemblers,
    tmp_path,
):
    """
    This test mocks the requests made by assembly_uploader lib functions.
    Flow is running 1 metaspades assembly upload
    """

    run_accession = "SRR6180434"
    sample_accession = "SAMN07793787"

    registered_study = "PRJNA567089"

    assembly = mgnify_assemblies_completed.first()

    ena_api_study = responses.add(
        responses.POST,
        "https://www.ebi.ac.uk/ena/portal/api/v2.0/search",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "result": "study",
                    "format": "json",
                    "fields": "study_accession,study_title,study_description,first_public",
                    "query": f'study_accession="{raw_reads_mgnify_study.first_accession}"',
                },
                allow_blank=True,
            )
        ],
        json=[
            {
                "study_accession": raw_reads_mgnify_study.first_accession,
                "study_title": raw_read_ena_study.title,
                "study_description": "anything",
                "first_public": "2024-01-11",
            }
        ],
    )

    ena_dropbox = responses.add(
        responses.POST,
        "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit",
        body=f"""
        This is a long receipt from the dropbox.
        success="true"
        Your new study has accession="{registered_study}"
        """,
    )

    ena_api_read_run = responses.add(
        responses.POST,
        "https://www.ebi.ac.uk/ena/portal/api/v2.0/search",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "result": "read_run",
                    "format": "json",
                    "fields": "run_accession,sample_accession,instrument_model,instrument_platform",
                    "query": f'run_accession="{run_accession}"',
                },
                allow_blank=True,
            )
        ],
        json=[
            {
                "run_accession": run_accession,
                "sample_accession": sample_accession,
                "instrument_model": "Illumina HiSeq 2500",
                "instrument_platform": "ILLUMINA",
            }
        ],
    )

    logged_uploader_result = run_flow_and_capture_logs(
        upload_assembly,
        assembly_id=assembly.id,
        dry_run=True,
    )

    captured_logging = logged_uploader_result.logs

    # sanity check
    assert f"Assembly {assembly} passed sanity check" in captured_logging
    assert f"{run_accession}.assembly_graph.fastg.gz does not exist" in captured_logging
    assert "params.txt does not exist" not in captured_logging

    assert ena_api_study.call_count == 1
    assert ena_dropbox.call_count == 1
    assert ena_api_read_run.call_count == 1

    # webin-cli cluster job
    mock_start_cluster_job.assert_called()
    mock_check_cluster_job_all_completed.assert_called()

    assert (
        mg_models.Assembly.objects.filter(status__assembly_uploaded=True).count() == 1
    )

    assert (
        mg_models.Assembly.objects.filter(status__assembly_upload_failed=True).count()
        == 0
    )


@pytest.mark.django_db(transaction=True)
def test_prefect_assembly_upload_flow_post_assembly_sanity_check_not_passed(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    mgnify_assembly_completed_uploader_sanity_check,
    assemblers,
    raw_read_ena_study,
    tmp_path,
):
    """
    This test mocks all assembly_uploader functions and just checks steps execution.
    Flow is running 1 metaspades assembly
    """

    assembly = mgnify_assembly_completed_uploader_sanity_check.first()

    logged_uploader_result = run_flow_and_capture_logs(
        upload_assembly,
        assembly_id=assembly.id,
        dry_run=True,
    )

    assert (
        f"Assembly {assembly} did not pass sanity check. No further action."
        in logged_uploader_result.logs
    )

    assert (
        mg_models.Assembly.objects.filter(status__post_assembly_qc_failed=True).count()
        == 1
    )

    assert (
        mg_models.Assembly.objects.filter(status__post_assembly_completed=True).count()
        == 0
    )


@pytest.mark.django_db(transaction=True)
def test_process_study_multiple_assemblies(
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    raw_read_ena_study,
    tmp_path,
    mgnify_assemblies,
    prefect_harness,
):
    # On call of handle_tpa_study for first assembly, a study should be made.
    # Test that a call of if for SECOND assembly picks up the first assembly's assembly-study,
    #  by virtue of sharing the same reads_study.

    all_assemblies = list(mgnify_assemblies)
    first_assembly = all_assemblies[0]
    second_assembly = all_assemblies[1]

    assembly_study_ena = ena.models.Study.objects.create(
        title="TPA study",
        accession="PRJ1",
    )
    assembly_study_mgnify = mg_models.Study.objects.create(
        ena_study=assembly_study_ena,
        title="TPA study",
    )

    first_assembly.assembly_study = assembly_study_mgnify
    first_assembly.save()

    should_be_same_as_assembly_study = handle_tpa_study(
        assembly=second_assembly,
        upload_folder=tmp_path,
        dry_run=True,
    )

    assert should_be_same_as_assembly_study == first_assembly.assembly_study
