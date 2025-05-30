import json
from typing import Callable, Optional, TypeVar, Union

import pytest
from ninja.testing import TestClient

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
    DownloadFileIndexFile,
)
from analyses.models import Analysis

R = TypeVar("R")


def _whole_object(j):
    return j


def call_endpoint_and_get_data(
    client: TestClient,
    endpoint: str,
    status_code: int = 200,
    count: Optional[int] = None,
    getter: Callable[[Union[dict, list]], R] = lambda response: response["items"],
) -> R:
    """
    Call an endpoint of the API. Check the status and response is expected.
    :param status_code: Expected value for status code, e.g. 200
    :param count: Expected value for `count` property at top level, if expected (e.g. a list endpoint)
    :param getter: Function/lambda that takes the API response JSON dict/list and return data you want
    :param client: Ninja API client fixture
    :param endpoint: path from APIP root, e.g. "/studies"
    :return: the API response passed through getter
    """
    response = client.get(endpoint)
    assert response.status_code == status_code
    j = response.json()
    if count is not None:
        assert j.get("count") == count
    return getter(j)


@pytest.mark.django_db
def test_api_study(raw_reads_mgnify_study, ninja_api_client):
    items = call_endpoint_and_get_data(ninja_api_client, "/studies/", count=1)
    assert items[0]["accession"] == raw_reads_mgnify_study.accession


@pytest.mark.django_db
def test_api_study_filtering(
    raw_reads_mgnify_study, ninja_api_client, top_level_biomes
):
    call_endpoint_and_get_data(ninja_api_client, "/studies/", count=1)
    call_endpoint_and_get_data(ninja_api_client, "/studies/?biome_lineage=", count=1)
    raw_reads_mgnify_study.biome = top_level_biomes[-1]
    raw_reads_mgnify_study.save()
    assert raw_reads_mgnify_study.biome.biome_name == "Human"
    call_endpoint_and_get_data(ninja_api_client, "/studies/?biome_lineage=", count=1)
    call_endpoint_and_get_data(
        ninja_api_client, "/studies/?biome_lineage=root", count=1
    )
    call_endpoint_and_get_data(
        ninja_api_client, "/studies/?biome_lineage=root:host-associated", count=1
    )
    call_endpoint_and_get_data(
        ninja_api_client, "/studies/?biome_lineage=root:host-associated:human", count=1
    )
    call_endpoint_and_get_data(
        ninja_api_client,
        "/studies/?biome_lineage=root:host-associated:human:gut",
        count=0,
    )
    call_endpoint_and_get_data(
        ninja_api_client, "/studies/?biome_lineage=root:engineered", count=0
    )
    call_endpoint_and_get_data(
        ninja_api_client, "/studies/?require_v6_analyses", count=1
    )


@pytest.mark.django_db
def test_api_analyses_list(raw_read_analyses, ninja_api_client):

    raw_read_analyses[1].status[
        Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    ] = True
    raw_read_analyses[1].save()

    raw_read_analyses[2].status[
        Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    ] = True
    raw_read_analyses[2].save()

    items = call_endpoint_and_get_data(
        ninja_api_client, "/analyses/", count=len(raw_read_analyses)
    )
    assert items[0]["accession"] in [a.accession for a in raw_read_analyses]
    assert sorted([a["experiment_type"] for a in items]) == [
        "Amplicon",
        "Metagenomic",
        "Metagenomic",
    ]


@pytest.mark.django_db
def test_api_study_analyses_list(raw_read_analyses, ninja_api_client):
    items = call_endpoint_and_get_data(
        ninja_api_client,
        f"/studies/{raw_read_analyses[0].study.accession}/analyses/",
        count=len(raw_read_analyses),
    )
    assert items[0]["accession"] in [a.accession for a in raw_read_analyses]
    assert sorted([a["experiment_type"] for a in items]) == [
        "Amplicon",
        "Metagenomic",
        "Metagenomic",
    ]


@pytest.mark.django_db
def test_api_analysis_detail(raw_read_analyses, ninja_api_client):
    analysis = call_endpoint_and_get_data(
        ninja_api_client,
        f"/analyses/{raw_read_analyses[0].accession}",
        getter=_whole_object,
    )
    assert analysis["accession"] == raw_read_analyses[0].accession
    assert analysis["study_accession"] == raw_read_analyses[0].study.accession
    assert (
        analysis["quality_control_summary"]["before_filtering"]["total_reads"] == 66124
    )


@pytest.mark.django_db
def test_api_analysis_downloads(raw_read_analyses, ninja_api_client):
    analysis = raw_read_analyses[0]
    dl = DownloadFile(
        alias="taxonomies-ssu.tsv.gz",
        short_description="Test file",
        file_type=DownloadFileType.TSV,
        download_group="taxonomies.closed_reference.ssu",
        download_type=DownloadType.TAXONOMIC_ANALYSIS,
        path="results/taxonomies.tsv.gz",
        long_description="This is a test file for taxonomies",
        file_size_bytes=1024,
        index_file=DownloadFileIndexFile(
            path="results/taxonomies.tsv.gz.gzi", index_type="gzi"
        ),
    )
    analysis.add_download(dl)
    analysis.refresh_from_db()
    api_analysis = call_endpoint_and_get_data(
        ninja_api_client, f"/analyses/{analysis.accession}", getter=_whole_object
    )

    assert api_analysis["accession"] == analysis.accession
    dl_api = next(
        d for d in api_analysis["downloads"] if d["alias"] == "taxonomies-ssu.tsv.gz"
    )
    print(json.dumps(dl_api, indent=2))
    assert (
        dl_api["url"]
        == f"http://localhost:8080/pub/databases/metagenomics/mgnify_results/analyses/{analysis.accession}/results/taxonomies.tsv.gz"
    )
    assert dl_api["index_file"]["relative_url"] == "taxonomies.tsv.gz.gzi"
    assert "path" not in dl_api


@pytest.mark.django_db
def test_api_samples_list(raw_reads_mgnify_sample, ninja_api_client):
    items = call_endpoint_and_get_data(
        ninja_api_client, "/samples/", count=len(raw_reads_mgnify_sample)
    )
    assert items[0]["accession"] in [s.first_accession for s in raw_reads_mgnify_sample]
    assert items[0]["ena_accessions"] in [
        s.ena_accessions for s in raw_reads_mgnify_sample
    ]


@pytest.mark.django_db
def test_api_sample_detail(raw_reads_mgnify_sample, ninja_api_client):
    db_sample = raw_reads_mgnify_sample[0]
    sample = call_endpoint_and_get_data(
        ninja_api_client, f"/samples/{db_sample.first_accession}", getter=_whole_object
    )
    assert sample["accession"] == db_sample.first_accession
    assert sample["ena_accessions"] == db_sample.ena_accessions
    assert len(sample["studies"]) == 1
    assert sample["studies"][0]["accession"] == db_sample.studies.first().accession
