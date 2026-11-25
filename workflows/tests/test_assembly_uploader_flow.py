import pytest
import responses
from assembly_uploader.release_study import StudyReleaseError
from django.urls import reverse
from responses import matchers

import analyses.models as mg_models
import ena.models
from workflows.flows.upload_assembly import (
    get_tpa_study,
    handle_tpa_study,
    release_tpa_study_if_complete,
    upload_assembly,
)
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.flaky(
    reruns=2
)  # sometimes fails due to logging on closed file - maybe a race condition with prefect harness?
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
        "https://www.ebi.ac.uk/ena/portal/api/search",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "result": "study",
                    "format": "json",
                    "fields": "study_accession,study_title,first_public",
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
        "https://www.ebi.ac.uk/ena/portal/api/search",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "result": "read_run",
                    "format": "json",
                    "fields": "run_accession,sample_accession,instrument_model",
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

    assembly.refresh_from_db()
    assert assembly.assembly_study.first_accession == registered_study
    # new TPA study is private until released
    assert assembly.assembly_study.ena_study.is_private
    assert assembly.assembly_study.is_private
    assert round(assembly.metadata[assembly.CommonMetadataKeys.COVERAGE], 4) == 0.0476
    assert (
        round(assembly.metadata[assembly.CommonMetadataKeys.COVERAGE_DEPTH], 3)
        == 273.694
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
def test_prefect_assembly_upload_flow_fails_if_contigs_file_missing(
    prefect_harness,
    mock_start_cluster_job,
    mgnify_assemblies_completed,
    tmp_path,
):
    """
    A missing contigs file should fail the flow loudly,
    not mark the assembly as post-assembly QC failed (which silently excludes it from upload).
    """
    assembly = mgnify_assemblies_completed.first()
    assembly.dir = str(tmp_path / "moved-or-archived")
    assembly.save()

    with pytest.raises(FileNotFoundError, match="Contigs file for assembly"):
        upload_assembly(assembly_id=assembly.id, dry_run=True)

    mock_start_cluster_job.assert_not_called()

    assembly.refresh_from_db()
    assert not assembly.status.get(
        mg_models.Assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED
    )
    assert not assembly.status.get(mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED)


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


@pytest.mark.django_db(transaction=True)
def test_prefect_assembly_upload_flow_blocked_for_unsupported_experiment_type(
    prefect_harness,
    mock_start_cluster_job,
    raw_reads_mgnify_study,
    raw_read_run,
    mgnify_assemblies_completed,
    assemblers,
    tmp_path,
):
    """
    An assembly whose runs have an experiment type that cannot be uploaded should fail the flow,
    marking the assembly as blocked and submitting no webin-cli job.
    """
    assembly = mgnify_assemblies_completed.first()
    assembly.runs.update(experiment_type=mg_models.Run.ExperimentTypes.UNKNOWN)

    curation_url = reverse(
        "admin:analyses_study_curate_run_experiment_types",
        args=[raw_reads_mgnify_study.pk],
    )
    with pytest.raises(Exception, match=curation_url):
        upload_assembly(assembly_id=assembly.id, dry_run=True)

    mock_start_cluster_job.assert_not_called()

    assembly.refresh_from_db()
    assert assembly.status.get(
        mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOAD_BLOCKED
    )
    assert not assembly.status.get(mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED)


DROPBOX_DEV = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit"


def _make_tpa_study(accession="PRJEB999001", is_private=True):
    ena_study = ena.models.Study.objects.create(
        title="TPA study", accession=accession, is_private=is_private
    )
    return mg_models.Study.objects.create(
        ena_study=ena_study, title="TPA study", is_private=is_private
    )


def _mark_completed(assemblies, uploaded=True, assembly_study=None):
    for assembly in assemblies:
        assembly.mark_status(mg_models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED)
        if uploaded:
            assembly.mark_status(mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED)
        if assembly_study:
            assembly.assembly_study = assembly_study
            assembly.save()


@pytest.mark.django_db(transaction=True)
def test_get_tpa_study_ignores_assemblies_brokered_into_reads_study(
    raw_reads_mgnify_study, mgnify_assemblies
):
    brokered, public = mgnify_assemblies[0], mgnify_assemblies[1]
    brokered.assembly_study = raw_reads_mgnify_study
    brokered.save()
    assert get_tpa_study(raw_reads_mgnify_study) is None

    tpa_study = _make_tpa_study()
    public.assembly_study = tpa_study
    public.save()
    assert get_tpa_study(raw_reads_mgnify_study) == tpa_study


@pytest.mark.django_db(transaction=True)
def test_release_tpa_study_when_all_assemblies_uploaded(
    raw_reads_mgnify_study, mgnify_assemblies, prefect_harness
):
    tpa_study = _make_tpa_study()
    _mark_completed(mgnify_assemblies, assembly_study=tpa_study)
    dropbox = responses.add(responses.POST, DROPBOX_DEV, body='success="true"')

    released = release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True)

    assert released == tpa_study
    assert dropbox.call_count == 1
    assert b'<RELEASE target="PRJEB999001"/>' in dropbox.calls[0].request.body
    tpa_study.refresh_from_db()
    assert not tpa_study.is_private
    assert not tpa_study.ena_study.is_private

    # already released: calling again does not resubmit
    assert release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True) is None
    assert dropbox.call_count == 1


@pytest.mark.django_db(transaction=True)
def test_release_tpa_study_not_released_while_upload_pending(
    raw_reads_mgnify_study, mgnify_assemblies, prefect_harness
):
    tpa_study = _make_tpa_study()
    _mark_completed(mgnify_assemblies, assembly_study=tpa_study)
    pending = mgnify_assemblies[0]
    pending.mark_status(
        mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED, set_status_as=False
    )
    pending.mark_status(mg_models.Assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED)
    dropbox = responses.add(responses.POST, DROPBOX_DEV, body='success="true"')

    assert release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True) is None
    assert dropbox.call_count == 0
    tpa_study.refresh_from_db()
    assert tpa_study.is_private

    # a QC-failed assembly is not expected to be uploaded, so it does not block release
    pending.mark_status(mg_models.Assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED)
    assert (
        release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True) == tpa_study
    )
    assert dropbox.call_count == 1


@pytest.mark.django_db(transaction=True)
def test_release_tpa_study_never_releases_brokered_reads_study(
    raw_reads_mgnify_study, mgnify_assemblies, prefect_harness
):
    # private assemblies are brokered into the reads study itself
    _mark_completed(mgnify_assemblies, assembly_study=raw_reads_mgnify_study)
    dropbox = responses.add(responses.POST, DROPBOX_DEV, body='success="true"')

    assert release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True) is None
    assert dropbox.call_count == 0


@pytest.mark.django_db(transaction=True)
def test_release_tpa_study_stays_private_if_ena_rejects(
    raw_reads_mgnify_study, mgnify_assemblies, prefect_harness
):
    tpa_study = _make_tpa_study()
    _mark_completed(mgnify_assemblies, assembly_study=tpa_study)
    responses.add(responses.POST, DROPBOX_DEV, body='success="false"')

    with pytest.raises(StudyReleaseError):
        release_tpa_study_if_complete(raw_reads_mgnify_study, dry_run=True)
    tpa_study.refresh_from_db()
    assert tpa_study.is_private
    assert tpa_study.ena_study.is_private
