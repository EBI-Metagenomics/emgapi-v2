"""Tests for chemical providers, visibility, pagination, and atomic imports."""

import gzip
from unittest.mock import patch

import httpx
import pytest
from django.core.cache import cache
from ninja.errors import HttpError

from analyses.models import Analysis, AnalysisMoleculeIndex
from analyses.molecule_search import (
    CHEBI_API,
    IDSM,
    chebi_candidates,
    similar_molecules,
)
from analyses.utils.rhea import import_rhea


@pytest.fixture(autouse=True)
def clear_chemistry_cache():
    cache.clear()
    yield
    cache.clear()


def test_chebi_candidates_only_returns_structures(httpx_mock):
    httpx_mock.add_response(
        url=httpx.URL(
            CHEBI_API + "es_search/", params={"term": "caffeine", "size": 10}
        ),
        json={
            "results": [
                {
                    "_source": {
                        "chebi_accession": "CHEBI:27732",
                        "ascii_name": "caffeine",
                        "smiles": "CCO",
                    }
                },
                {"_source": {"chebi_accession": "CHEBI:1", "ascii_name": "class"}},
            ]
        },
    )
    assert chebi_candidates("caffeine") == [
        {"chebi_id": "CHEBI:27732", "name": "caffeine", "smiles": "CCO"}
    ]


def test_idsm_scores_and_cache(httpx_mock):
    httpx_mock.add_response(
        json={
            "results": {
                "bindings": [
                    {
                        "compound": {
                            "value": "http://purl.obolibrary.org/obo/CHEBI_16236"
                        },
                        "score": {"value": "1.0E0"},
                    }
                ]
            }
        }
    )
    expected = {
        "molecules": [{"chebi_id": "CHEBI:16236", "score": 1.0}],
        "truncated": False,
    }
    assert similar_molecules("CCO", 0.8) == expected
    assert similar_molecules("CCO", 0.8) == expected
    assert str(httpx_mock.get_request().url).startswith(IDSM)


def test_provider_failure_is_not_zero_hits(httpx_mock):
    httpx_mock.add_response(status_code=503)
    with pytest.raises(HttpError, match="unavailable"):
        similar_molecules("CCO", 0.8)


@pytest.mark.django_db
@pytest.mark.parametrize("header", [False, True])
def test_import_retry_and_rollback(assembly_analysis, tmp_path, header):
    path = tmp_path / "rhea.tsv.gz"
    row = "ERZ1\tp1\thash\tRHEA:123\tCHEBI:1 + CHEBI:1 = CHEBI:2\tequation\ttop hit\n"
    with gzip.open(path, "wt") as stream:
        stream.write(
            (
                "contig_id\tprotein_id\tprotein_hash\trhea_id\tchebi_reaction\treaction\ttop_hit\n"
                if header
                else ""
            )
            + row * 2
            + row.replace("\tp1\t", "\tp2\t")
        )
    assert import_rhea(assembly_analysis.id, path) == 2
    assert import_rhea(assembly_analysis.id, path) == 2
    with gzip.open(path, "wt") as stream:
        stream.write(row + "broken\n")
    with pytest.raises(ValueError, match="row 2"):
        import_rhea(assembly_analysis.id, path)
    assert AnalysisMoleculeIndex.objects.get(analysis=assembly_analysis).chebi_ids == [
        1,
        2,
    ]
    with gzip.open(path, "wt") as stream:
        stream.write("")
    assert import_rhea(assembly_analysis.id, path) == 0
    assert AnalysisMoleculeIndex.objects.get(analysis=assembly_analysis).chebi_ids == []


@pytest.mark.django_db
def test_search_visibility_and_cursor(ninja_api_client, assembly_analysis):
    analysis = assembly_analysis
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    # Fifty-one analyses exercise the analysis-ID cursor, not protein pagination.
    analyses = [analysis] + [
        Analysis.objects.create(
            study=analysis.study,
            sample=analysis.sample,
            ena_study=analysis.ena_study,
            assembly=analysis.assembly,
            status=dict(analysis.status),
        )
        for _ in range(50)
    ]
    AnalysisMoleculeIndex.objects.bulk_create(
        [AnalysisMoleculeIndex(analysis=item, chebi_ids=[1, 2]) for item in analyses]
    )
    chemistry = {
        "molecules": [{"chebi_id": "CHEBI:1", "score": 1.0}],
        "truncated": False,
    }
    with patch("analyses.chemical_api.similar_molecules", return_value=chemistry):
        response = ninja_api_client.post("/molecule-search/", json={"smiles": "CCO"})
        assert response.status_code == 200
        first = response.json()
        assert len(first["items"]) == 50
        assert first["items"][0]["molecules"] == [{"chebi_id": "CHEBI:1", "score": 1.0}]
        second = ninja_api_client.post(
            "/molecule-search/", json={"smiles": "CCO", "after": first["next_cursor"]}
        ).json()
        assert len(second["items"]) == 1
        assert second["next_cursor"] is None
        Analysis.objects.filter(pk__in=[item.pk for item in analyses[1:]]).delete()
        for field in ("is_private", "is_suppressed"):
            setattr(analysis, field, True)
            analysis.save()
            assert (
                ninja_api_client.post(
                    "/molecule-search/", json={"smiles": "CCO"}
                ).json()["items"]
                == []
            )
            setattr(analysis, field, False)
        analysis.status = {}
        analysis.save()
        assert (
            ninja_api_client.post("/molecule-search/", json={"smiles": "CCO"}).json()[
                "items"
            ]
            == []
        )


def test_query_validation(ninja_api_client):
    assert (
        ninja_api_client.post("/molecule-search/", json={"smiles": ""}).status_code
        == 422
    )
    assert (
        ninja_api_client.post(
            "/molecule-search/", json={"smiles": "CCO", "threshold": 2}
        ).status_code
        == 422
    )
