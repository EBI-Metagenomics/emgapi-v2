import gzip
import logging
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import httpx
import pytest
from lxml import etree

from activate_django_first import EMG_CONFIG

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis, Biome
from kvstore.models import KeyValueStore
from workflows.flows.data_dumps.ebi_search.flows.ebi_search_dump import (
    ebi_search_dump_flow,
)
from workflows.flows.data_dumps.ebi_search.utils.checkpoints import (
    EBI_SEARCH_DUMP_STATE_KEY,
    EBISearchDumpState,
)
from workflows.flows.data_dumps.ebi_search.utils.documents import project_entry
from workflows.flows.data_dumps.ebi_search.utils.functional_annotations import (
    DOWNLOAD_RETRY,
    _functional_download_kind,
    _http_client,
    _identifiers_from_url,
)


def _response(url: str, body: bytes = b"", status_code: int = 200):
    return httpx.Response(
        status_code,
        content=body,
        request=httpx.Request("GET", url),
    )


def _download(
    path: str,
    short_description: str,
    download_group: str = "all",
    download_type: DownloadType = DownloadType.FUNCTIONAL_ANALYSIS,
) -> DownloadFile:
    return DownloadFile(
        path=path,
        alias=PurePosixPath(path).name,
        file_type=DownloadFileType.TSV,
        download_type=download_type,
        download_group=download_group,
        short_description=short_description,
        long_description=short_description,
    )


class StubClient(httpx.Client):
    def __init__(self):
        self.urls = []
        super().__init__(transport=httpx.MockTransport(self._response))

    def _response(self, request):
        url = str(request.url)
        self.urls.append(url)
        if "go_summary" in url:
            contents = b"go term\tcategory\tcount\nGO:0008150\tbiological_process\t4\n"
        elif "interpro_summary" in url:
            contents = (
                b"interpro_accession\tdescription\tcount\nIPR000001\tKringle\t2\n"
            )
        elif "proteins2rhea" in url:
            contents = b"protein\tstart\tend\tRHEA:12345\n"
        else:
            raise AssertionError(f"Unexpected URL {url}")
        return _response(url, gzip.compress(contents))


def _parse(path: Path):
    return etree.parse(str(path)).getroot()


@pytest.mark.django_db(transaction=True)
def test_ebi_search_dump_flow_initial_and_private_incremental(
    monkeypatch,
    prefect_harness,
    tmp_path,
    assembly_with_analyses,
    mgnify_study_full_metadata,
    mgnify_sample_lots_of_metadata,
):
    analysis = assembly_with_analyses[0]
    study = mgnify_study_full_metadata
    sample = mgnify_sample_lots_of_metadata
    study.biome = Biome.objects.get(biome_name="Engineered")
    study.metadata["secondary_study_accession"] = "ERP106708"
    study.metadata["study_name"] = 'Soil & water <study> "one"'
    study.save()
    sample.ena_accessions.append("ERS123456")
    sample.save()
    analysis.assembly.ena_accessions = ["ERZ857107"]
    analysis.assembly.save()
    analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = True
    analysis.experiment_type = Analysis.ExperimentTypes.ASSEMBLY
    analysis.pipeline_version = Analysis.PipelineVersions.v6
    analysis.external_results_dir = "PRJNA398/PRJNA398089/ERZ857107/V6/assembly"
    analysis.results_dir = "/internal/results/must-not-be-used"
    analysis.downloads = [
        download.model_dump(exclude={"parent_identifier"})
        for download in [
            _download(
                "functional-annotation/ERZ857107_go_summary.tsv.gz",
                "GO Term counts",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.go_slims",
            ),
            _download(
                "functional-annotation/ERZ857107_interpro_summary.tsv.gz",
                "InterPro Identifier counts",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.interpro",
            ),
            _download(
                "functional-annotation/ERZ857107_proteins2rhea.tsv.gz",
                "Rhea reaction counts",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.rhea_reactions",
            ),
            _download(
                "functional-annotation/ERZ857107_interproscan.tsv.gz",
                "InterProScan results",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.interpro",
            ),
        ]
    ]
    analysis.annotations = {
        Analysis.TAXONOMIES: {
            "ssu": [
                {
                    "organism": "sk__Bacteria;k__;p__Bacillota;c__Bacilli",
                    "count": 12,
                }
            ]
        }
    }
    analysis.save()
    analysis.refresh_from_db()
    assert analysis.is_ready

    stub_client = StubClient()
    monkeypatch.setattr(
        "workflows.flows.data_dumps.ebi_search.tasks.dump_analyses._http_client",
        lambda: stub_client,
    )
    output_dir = tmp_path / "live"
    monkeypatch.setattr(EMG_CONFIG.ebi_search, "output_dir", str(output_dir))
    monkeypatch.setattr(EMG_CONFIG.ebi_search, "analysis_chunk_size", 1)
    monkeypatch.setattr(
        EMG_CONFIG.service_urls,
        "transfer_services_url_root",
        "http://transfer-services/results/",
    )

    result = ebi_search_dump_flow(initial=True)

    assert result["projects"] == {"additions": 1, "deletions": 0}
    assert result["analyses"] == {"additions": 1, "deletions": 0}
    projects = _parse(output_dir / "initial/projects/latest/projects.xml")
    runs = _parse(output_dir / "initial/runs/latest/analyses_0001.xml")
    assert projects.findtext("name") == "EMG_Project"
    assert projects.findtext("entry_count") == "1"
    assert projects.find("entries/entry").get("id") == study.accession
    assert projects.findtext("entries/entry/name") == 'Soil & water <study> "one"'
    assert (
        projects.findtext(
            "entries/entry/additional_fields/field[@name='secondary_acc']"
        )
        == "ERP106708"
    )
    assert projects.xpath(
        "entries/entry/cross_references/ref[@dbname='metagenomics_analyses']/@dbkey"
    ) == [f"{analysis.accession}_6.0"]
    assert projects.xpath(
        "entries/entry/additional_fields/field[@name='experiment_type']/text()"
    ) == ["assembly"]
    assert projects.xpath(
        "entries/entry/additional_fields/field[@name='pipeline_version']/text()"
    ) == ["6.0"]

    run_entry = runs.find("entries/entry")
    assert run_entry.get("id") == f"{analysis.accession}_6.0"
    assert len(run_entry.xpath("additional_fields/field[@name='project_name']")) == 2
    assert run_entry.xpath(
        "additional_fields/hierarchical_field[@name='organism']/*/text()"
    ) == ["Bacteria", "Bacillota", "Bacilli"]
    references = {
        (ref.get("dbname"), ref.get("dbkey"))
        for ref in run_entry.findall("cross_references/ref")
    }
    assert ("ena_project", "PRJNA398089") in references
    assert ("biosamples", "SAMN07793787") in references
    assert ("sra-sample", "ERS123456") in references
    assert ("analysis", "ERZ857107") in references
    assert ("go", "GO:0008150") in references
    assert ("interpro", "IPR000001") in references
    assert ("rhea", "RHEA:12345") in references
    assert len(stub_client.urls) == 3
    assert all("/internal/results/" not in url for url in stub_client.urls)

    study.is_private = True
    study.save()
    incremental_result = ebi_search_dump_flow()

    assert incremental_result["since"] == result["until"]
    assert incremental_result["projects"] == {"additions": 0, "deletions": 1}
    assert incremental_result["analyses"] == {"additions": 0, "deletions": 1}
    incremental_until = datetime.fromisoformat(incremental_result["until"])
    incremental = output_dir / "incremental" / incremental_until.date().isoformat()
    project_deletes = _parse(incremental / "projects/projects-deletes.xml")
    analysis_deletes = _parse(incremental / "runs/analyses-deletes.xml")
    assert project_deletes.find("entries/entry").get("id") == study.accession
    assert analysis_deletes.find("entries/entry").get("id") == (
        f"{analysis.accession}_6.0"
    )
    assert _parse(incremental / "projects/projects.xml").findtext("entry_count") == "0"
    assert _parse(incremental / "runs/analyses_0001.xml").findtext("entry_count") == "0"
    state = KeyValueStore.get_model(EBI_SEARCH_DUMP_STATE_KEY, EBISearchDumpState)
    assert state.last_dump_date == incremental_until


def test_project_entry_has_distinct_analysis_types_and_pipeline_versions():
    analyses = [
        Analysis(
            accession="MGYA00000001",
            experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
            pipeline_version=Analysis.PipelineVersions.v6,
        ),
        Analysis(
            accession="MGYA00000002",
            experiment_type=Analysis.ExperimentTypes.METAGENOMIC,
            pipeline_version=Analysis.PipelineVersions.v5,
        ),
        Analysis(
            accession="MGYA00000003",
            experiment_type=Analysis.ExperimentTypes.ASSEMBLY,
            pipeline_version=Analysis.PipelineVersions.v6,
        ),
    ]
    now = datetime.now(UTC)
    study = SimpleNamespace(
        accession="MGYS00000001",
        title="Study",
        metadata_preferring_inferred={},
        ena_accessions=[],
        ebi_search_analyses=analyses,
        biome=None,
        biome_id=None,
        created_at=now,
        updated_at=now,
    )

    entry = project_entry(study, {})

    assert [
        item.value for item in entry.additional_fields if item.name == "experiment_type"
    ] == ["assembly", "metagenomic"]
    assert [
        item.value
        for item in entry.additional_fields
        if item.name == "pipeline_version"
    ] == ["5.0", "6.0"]


def test_functional_summary_selection_does_not_fetch_interproscan():
    assert (
        _functional_download_kind(_download("opaque-a.csv", "InterPro summary"))
        == "interpro"
    )
    assert (
        _functional_download_kind(_download("opaque-b.tsv", "InterProScan results"))
        is None
    )
    assert (
        _functional_download_kind(
            _download(
                "opaque-c.tsv",
                "anything",
                f"{Analysis.FUNCTIONAL_ANNOTATION}.go_slims",
            )
        )
        == "go"
    )
    assert (
        _functional_download_kind(_download("opaque-d.tsv", "Rhea reaction counts"))
        == "rhea"
    )
    assert (
        _functional_download_kind(_download("opaque-v5.csv", "Complete GO annotation"))
        == "go"
    )
    assert (
        _functional_download_kind(
            _download(
                "opaque-e.tsv",
                "InterPro summary",
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
            )
        )
        is None
    )


def test_missing_functional_file_is_a_warning(caplog):
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: _response(str(request.url), status_code=404)
        )
    )
    with caplog.at_level(logging.WARNING):
        identifiers = _identifiers_from_url(
            client,
            "http://example.test/missing.tsv.gz",
            "missing.tsv.gz",
            "go",
            "MGYA00000001",
            logging.getLogger(__name__),
        )
    assert identifiers == set()
    assert "Skipping missing go file for MGYA00000001" in caplog.text


def test_http_client_configuration():
    client = _http_client()
    try:
        assert client.follow_redirects
        assert client.timeout.connect == 10
        assert client.timeout.read == 120
    finally:
        client.close()


def test_http_client_retries_transient_status(monkeypatch):
    responses = iter(
        [
            _response("http://example.test/go.tsv", status_code=503),
            _response("http://example.test/go.tsv", body=b"GO:0008150"),
        ]
    )
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: next(responses))
    )
    monkeypatch.setattr(type(DOWNLOAD_RETRY), "sleep", lambda *_: None)

    assert _identifiers_from_url(
        client,
        "http://example.test/go.tsv",
        "go.tsv",
        "go",
        "MGYA00000001",
        logging.getLogger(__name__),
    ) == {"GO:0008150"}
