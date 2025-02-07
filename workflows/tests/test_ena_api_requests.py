import pytest
from django.conf import settings
from prefect.logging import disable_run_logger

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    ENAAPIRequest,
    ENAPortalResultType,
    ENAQueryClause,
    ENAQueryOperators,
    ENAQueryPair,
    ENAStudyFields,
    ENAStudyQuery,
    get_study_from_ena,
    get_study_readruns_from_ena,
    is_ena_study_available_privately,
    is_ena_study_public,
    sync_privacy_state_of_ena_study_and_derived_objects,
)

EMG_CONFIG = settings.EMG_CONFIG


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_study_from_ena_no_primary_accession(httpx_mock):
    """
    Study doesn't have primary accession and returns empty json
    """
    study_accession = "SRP012064"  # study doesn't have primary accession
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession={study_accession}%20OR%20secondary_study_accession={study_accession}%29%22&limit=10&format=json&fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
        json=[],
    )
    with disable_run_logger():
        with pytest.raises(
            Exception, match=f"No study found for accession {study_accession}"
        ):
            await get_study_from_ena.fn(study_accession, limit=10)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_study_from_ena_two_secondary_accessions(httpx_mock):
    """
    Study has two secondary accessions
    """
    study_accession = "PRJNA109315"  # has SRP000903;SRP001212 secondary accessions
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession={study_accession}%20OR%20secondary_study_accession={study_accession}%29%22&limit=10&format=json&fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
        json=[
            {
                "study_title": "Weird study",
                "study_accession": "PRJNA109315",
                "secondary_study_accession": "SRP000903;SRP001212",
            },
        ],
    )
    with disable_run_logger():
        await get_study_from_ena.fn(study_accession, limit=10)
        assert (
            await ena.models.Study.objects.filter(accession=study_accession).acount()
            == 1
        )
        created_study = await ena.models.Study.objects.get_ena_study(study_accession)
        assert created_study.accession == study_accession
        assert len(created_study.additional_accessions) == 2


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_study_from_ena_use_secondary_as_primary(httpx_mock):
    """
    Study doesn't have primary accession
    """
    sec_study_accession = "SRP0009034"
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession={sec_study_accession}%20OR%20secondary_study_accession={sec_study_accession}%29%22&limit=10&format=json&fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
        json=[
            {
                "study_title": "More weird study",
                "study_accession": "",
                "secondary_study_accession": "SRP0009034",
            },
        ],
    )
    with disable_run_logger():
        await get_study_from_ena.fn(sec_study_accession, limit=10)
        assert (
            await ena.models.Study.objects.filter(
                accession=sec_study_accession
            ).acount()
            == 1
        )
        created_study = await ena.models.Study.objects.get_ena_study(
            sec_study_accession
        )
        assert created_study.accession == sec_study_accession
        assert len(created_study.additional_accessions) == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_study_from_ena_no_secondary_accession(httpx_mock):
    """
    Study has no secondary accessions
    """
    study_accession = "PRJNA109315"
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession={study_accession}%20OR%20secondary_study_accession={study_accession}%29%22&limit=10&format=json&fields={','.join(EMG_CONFIG.ena.study_metadata_fields)}",
        json=[
            {
                "study_title": "Weird study without secondary accession",
                "study_accession": "PRJNA109315",
                "secondary_study_accession": "",
            },
        ],
    )
    with disable_run_logger():
        await get_study_from_ena.fn(study_accession, limit=10)
        assert (
            await ena.models.Study.objects.filter(accession=study_accession).acount()
            == 1
        )
        created_study = await ena.models.Study.objects.get_ena_study(study_accession)
        assert created_study.accession == study_accession
        assert len(created_study.additional_accessions) == 0


@pytest.mark.django_db(transaction=True)
def test_get_study_readruns_from_ena(
    httpx_mock, raw_read_ena_study, raw_reads_mgnify_study
):
    """
    run1 is not metagenomic/metatranscriptomic data
    run2 is correct
    run3 doesn't have correct library_sourec
    run4 single but has 2 fqs
    run5 paired but has 1 fq
    run6 paired has additional fqs
    """
    study_accession = "PRJNA398089"
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=read_run&query=%22(study_accession={study_accession}%20OR%20secondary_study_accession={study_accession})%22&limit=10&format=json&fields={','.join(EMG_CONFIG.ena.readrun_metadata_fields)}&dataPortal=metagenome",
        json=[
            {
                "run_accession": "RUN1",
                "sample_accession": "SAMPLE1",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP1",
                "fastq_md5": "md5kbshdk",
                "fastq_ftp": "fq.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "AMPLICON",
                "library_source": "GENOMIC",
                "scientific_name": "genome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
            {
                "run_accession": "RUN2",
                "sample_accession": "SAMPLE2",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP2",
                "fastq_md5": "md5kjdndk",
                "fastq_ftp": "fq_1.fastq.gz;fq_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
            {
                "run_accession": "RUN3",
                "sample_accession": "SAMPLE3",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP3",
                "fastq_md5": "md5kjdndk",
                "fastq_ftp": "fq_1.fastq.gz;fq_2.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOME",
                "scientific_name": "uncultured bacteria",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
            {
                "run_accession": "RUN4",
                "sample_accession": "SAMPLE4",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP4",
                "fastq_md5": "md5kjdndk",
                "fastq_ftp": "fq_1.fastq.gz;fq_2.fastq.gz",
                "library_layout": "SINGLE",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
            {
                "run_accession": "RUN5",
                "sample_accession": "SAMPLE5",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP5",
                "fastq_md5": "md5kjdndk",
                "fastq_ftp": "fq.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
            {
                "run_accession": "RUN6",
                "sample_accession": "SAMPLE6",
                "sample_title": "sample title",
                "secondary_sample_accession": "SAMP6",
                "fastq_md5": "md5kjdndk",
                "fastq_ftp": "fq_2.fastq.gz;fq_1.fastq.gz;fq_merged.fastq.gz;fq_3.fastq.gz",
                "library_layout": "PAIRED",
                "library_strategy": "AMPLICON",
                "library_source": "METAGENOMIC",
                "scientific_name": "metagenome",
                "host_tax_id": "7460",
                "host_scientific_name": "Apis mellifera",
            },
        ],
    )
    with disable_run_logger():
        get_study_readruns_from_ena.fn(study_accession, limit=10)
        # run is not metagenome in scientific_name
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN1").count()
            == 0
        )
        # correct run
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN2").count()
            == 1
        )
        # incorrect library_source and scientific name
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN3").count()
            == 0
        )
        # incorrect library_layout single
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN4").count()
            == 0
        )
        # incorrect library_layout paired
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN5").count()
            == 0
        )
        # should return only 2 fq files in correct order
        assert (
            analyses.models.Run.objects.filter(ena_accessions__contains="RUN6").count()
            == 1
        )
        run = analyses.models.Run.objects.get(ena_accessions__contains="RUN6")
        assert (
            len(run.metadata["fastq_ftps"]) == 2
            and "_1" in run.metadata["fastq_ftps"][0]
            and "_2" in run.metadata["fastq_ftps"][1]
        )
        assert run.metadata["host_tax_id"] == "7460"


def test_ena_api_query_maker(httpx_mock):
    # test generic part combiners
    planet_is_tatooine = ENAQueryClause(search_field="planet", value="tatooine")
    assert str(planet_is_tatooine) == "planet=tatooine"
    assert str(~planet_is_tatooine) == "NOT planet=tatooine"
    planet_is_naboo = ENAQueryClause(search_field="planet", value="naboo")
    assert str(planet_is_naboo) == "planet=naboo"
    planet_is_either = planet_is_tatooine | planet_is_naboo
    assert isinstance(planet_is_either, ENAQueryPair)
    assert str(planet_is_either) == "(planet=tatooine OR planet=naboo)"
    assert (
        str(planet_is_tatooine | planet_is_naboo) == "(planet=tatooine OR planet=naboo)"
    )

    species_is_jawas = ENAQueryClause(search_field="species", value="jawas")
    assert (
        str(planet_is_tatooine & species_is_jawas)
        == "(planet=tatooine AND species=jawas)"
    )
    assert (
        str(planet_is_tatooine & ~planet_is_naboo)
        == "(planet=tatooine AND NOT planet=naboo)"
    )

    tatooine_and_jawas = ENAQueryPair(
        left=planet_is_tatooine, right=species_is_jawas, operator=ENAQueryOperators.AND
    )
    assert str(tatooine_and_jawas) == "(planet=tatooine AND species=jawas)"

    tatooine_or_naboo = ENAQueryPair(
        left=planet_is_tatooine, right=planet_is_naboo, operator=ENAQueryOperators.OR
    )
    assert str(tatooine_or_naboo) == "(planet=tatooine OR planet=naboo)"

    not_tatooine_or_naboo = ENAQueryPair(
        left=planet_is_tatooine,
        right=planet_is_naboo,
        operator=ENAQueryOperators.OR,
        is_not=True,
    )
    assert str(not_tatooine_or_naboo) == "NOT (planet=tatooine OR planet=naboo)"

    not_tatooine_or_naboo = ~tatooine_or_naboo
    assert str(not_tatooine_or_naboo) == "NOT (planet=tatooine OR planet=naboo)"

    # test ena study query maker
    # default combination is AND (like django filter)
    study_is_erp1_and_public = ENAStudyQuery(study_accession="ERP1", status=1)
    assert str(study_is_erp1_and_public) == "(status=1 AND study_accession=ERP1)"
    # order because of field declaration order on ENAStudyQueyr

    one_accession_is_erp1 = ENAStudyQuery(study_accession="ERP1") | ENAStudyQuery(
        secondary_study_accession="ERP1"
    )
    assert (
        str(one_accession_is_erp1)
        == "(study_accession=ERP1 OR secondary_study_accession=ERP1)"
    )

    # constructing full query params
    request = ENAAPIRequest(
        result=ENAPortalResultType.STUDY,
        query=(
            ENAStudyQuery(study_accession="ERP1")
            | ENAStudyQuery(secondary_study_accession="ERP1")
        )
        & ENAStudyQuery(tax_id="408170"),
        fields=[
            ENAStudyFields.STUDY_NAME,
            ENAStudyFields.STUDY_ACCESSION,
            ENAStudyFields.TAX_ID,
            ENAStudyFields.SECONDARY_STUDY_ACCESSION,
        ],
        limit=10,
    )

    assert request.model_dump() == {
        "query": '"((study_accession=ERP1 OR secondary_study_accession=ERP1) AND tax_id=408170)"',
        "fields": "study_name,study_accession,tax_id,secondary_study_accession",
        "limit": 10,
        "format": "json",
        "result": "study",
    }

    # calling API
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28%28study_accession%3DERP1%20OR%20secondary_study_accession%3DERP1%29%20AND%20tax_id%3D408170%29%22&fields=study_name,study_accession,tax_id,secondary_study_accession&limit=10&format=json",
        json=[
            {"study_accession": "ERP1"},
        ],
    )

    response = request.get()
    assert response.status_code == 200
    assert response.json() == [{"study_accession": "ERP1"}]


def test_is_study_public(httpx_mock, prefect_harness):
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession%3DERP1+OR+secondary_study_accession%3DERP1%29%22&fields=study_accession&limit=&format=json",
        json=[
            {"study_accession": "ERP1"},
        ],
    )
    with disable_run_logger():
        assert is_ena_study_public("ERP1") == True

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession%3DERP1+OR+secondary_study_accession%3DERP1%29%22&fields=study_accession&limit=&format=json",
        json=[],
    )
    with disable_run_logger():
        assert is_ena_study_public("ERP1") == False

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession%3DERP1+OR+secondary_study_accession%3DERP1%29%22&fields=study_accession&limit=&format=json",
        json={"message": "bad call"},
    )
    with disable_run_logger():
        assert is_ena_study_public("ERP1") == False


def test_is_study_private(httpx_mock, prefect_harness):
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=%22%28study_accession%3DERP1+OR+secondary_study_accession%3DERP1%29%22&fields=study_accession&limit=&format=json",
        json=[
            {"study_accession": "ERP1"},
        ],
    )
    with disable_run_logger():
        assert is_ena_study_available_privately("ERP1") == True


@pytest.mark.django_db
def test_sync_privacy_state_of_ena_study_and_derived_objects(
    httpx_mock, prefect_harness, raw_read_run
):

    ena_study: ena.models.Study = ena.models.Study.objects.first()

    httpx_mock.add_response(
        match_headers={
            "Authorization": "Basic d2ViaW4tZmFrZTpub3QtYS1wdw=="
        },  # webin-fake:not-a-pw
        json=[
            {"study_accession": "ERP1"},
        ],
    )  # is available privately

    httpx_mock.add_response(json=[])  # not available publicly

    sync_privacy_state_of_ena_study_and_derived_objects(ena_study)
    ena_study.refresh_from_db()

    assert ena_study.is_private
    assert not ena_study.is_suppressed
