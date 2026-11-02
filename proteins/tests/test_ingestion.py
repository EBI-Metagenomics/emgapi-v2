import shutil
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from analyses.models import Analysis
from proteins.accession.inputs import InvalidInput
from proteins.ingestion import analyses
from proteins.models import (
    AssemblyProteins,
    GeneCaller,
    StagedOccurrence,
    UnsupportedPipelineVersion,
)
from proteins.routers import MGYP
from proteins.tests.conftest import FIXTURES, make_analysis, make_study, query
from workflows.flows.proteins import ingest_assembly_proteins as flows

pytestmark = [
    pytest.mark.django_db(databases=["default", MGYP], transaction=True),
    pytest.mark.usefixtures("mgyp_as_accession"),
]


@pytest.fixture
def results(tmp_path):
    """Lays out ERZ101's predicted genes as an assembly analysis's results of an ERZ."""

    def make(erz):
        directory = tmp_path / erz
        (directory / "cds").mkdir(parents=True)
        (directory / "qc").mkdir()
        for fixture, path in (
            ("faa.gz", f"cds/{erz}_predicted_cds.faa.gz"),
            ("gff.gz", f"cds/{erz}_predicted_cds.gff.gz"),
            ("fasta.gz", f"qc/{erz}_filtered_contigs.fasta.gz"),
        ):
            shutil.copy(FIXTURES / f"ERZ101.{fixture}", directory / path)
        return directory

    return make


def records():
    return sorted(
        AssemblyProteins.objects.values_list(
            "assembly_accession", "pipeline_version", "analysis_accession"
        )
    )


def test_lists_only_pending_analyses_of_versions_with_rules(results):
    a, b = make_study("PRJ1"), make_study("PRJ2")
    pending = make_analysis(a, "ERZ1", results_dir=results("ERZ1"))
    make_analysis(a, "ERZ2", state=Analysis.AnalysisStates.ANALYSIS_COMPLETED)
    make_analysis(a, "ERZ3", version="V5")
    make_analysis(a, "ERZ4", version="V4.1")
    recorded = make_analysis(a, "ERZ5", results_dir=results("ERZ5"))
    analyses.ingest_analysis(recorded.accession)
    # The lowest id is taken, though its study comes later.
    first = make_analysis(b, "ERZ6", version="V6.1")
    make_analysis(a, "ERZ6", version="V6.1")

    assert analyses.find_pending_analyses() == [pending, first]
    assert analyses.find_pending_analyses(a.accession) == [pending]
    assert analyses.find_pending_analyses(b.accession) == [first]


def test_chunks_hold_analyses_of_one_study():
    listed = [
        SimpleNamespace(study_id=study, accession=f"MGYA{n}")
        for n, study in enumerate([1, 1, 1, 2])
    ]
    assert analyses.chunk_by_study(listed, 2) == [
        ["MGYA0", "MGYA1"],
        ["MGYA2"],
        ["MGYA3"],
    ]


@pytest.mark.usefixtures("prefect_harness")
def test_listing_flow_starts_one_chunk_flow_for_each_chunk(monkeypatch):
    a, b = make_study("PRJ1"), make_study("PRJ2")
    listed = [make_analysis(a, f"ERZ{n}") for n in range(3)] + [
        make_analysis(b, "ERZ3")
    ]
    monkeypatch.setattr(flows.EMG_CONFIG.proteins, "ingestion_chunk_size", 2)
    with patch.object(flows, "run_deployment") as run_deployment:
        assert flows.ingest_assembly_proteins() == 3
    assert [
        call.kwargs["parameters"]["analysis_accessions"]
        for call in run_deployment.call_args_list
    ] == [
        [listed[0].accession, listed[1].accession],
        [listed[2].accession],
        [listed[3].accession],
    ]


@pytest.mark.usefixtures("prefect_harness")
def test_chunk_ingests_the_others_then_fails_naming_the_failed_analysis(results):
    study = make_study("PRJ1")
    good = make_analysis(study, "ERZ1", results_dir=results("ERZ1"))
    missing = make_analysis(study, "ERZ2", results_dir=results("ERZ2").parent / "x")
    other = make_analysis(study, "ERZ3", results_dir=results("ERZ3"))

    with pytest.raises(
        flows.IngestionFailed, match=f"1 of 3 analyses: {missing.accession}$"
    ):
        flows.ingest_assembly_proteins_chunk(
            [good.accession, missing.accession, other.accession]
        )
    assert records() == [
        ("ERZ1", "6.0", good.accession),
        ("ERZ3", "6.0", other.accession),
    ]
    assert analyses.find_pending_analyses() == [missing]


@pytest.mark.usefixtures("prefect_harness")
def test_a_listing_after_a_chunk_commits_takes_none_of_its_analyses(results):
    study = make_study("PRJ1")
    chunk = [
        make_analysis(study, f"ERZ{n}", results_dir=results(f"ERZ{n}"))
        for n in range(2)
    ]
    results_ = flows.ingest_assembly_proteins_chunk([a.accession for a in chunk])
    assert [r.skipped for r in results_] == [False, False]
    assert analyses.find_pending_analyses() == []


@pytest.mark.usefixtures("prefect_harness")
def test_by_hand_one_mgya_or_every_analysis_of_one_erz(results):
    study = make_study("PRJ1")
    v6 = make_analysis(study, "ERZ1", results_dir=results("ERZ1"))
    v6_1 = make_analysis(study, "ERZ1", version="V6.1", results_dir=v6.results_dir)
    other = make_analysis(study, "ERZ2", results_dir=results("ERZ2"))

    flows.ingest_analysis_proteins(other.accession)
    assert records() == [("ERZ2", "6.0", other.accession)]
    flows.ingest_analysis_proteins("ERZ1")
    assert records() == [
        ("ERZ1", "6.0", v6.accession),
        ("ERZ1", "6.1", v6_1.accession),
        ("ERZ2", "6.0", other.accession),
    ]


def test_genes_are_staged_with_the_gene_callers_of_the_version(results):
    analysis = make_analysis(make_study("PRJ1"), "ERZ1", results_dir=results("ERZ1"))
    analyses.ingest_analysis(analysis.accession)
    assert set(
        StagedOccurrence.objects.values_list("gene_caller__name", flat=True)
    ) == {"Pyrodigal", "FragGeneScanRS"}


def test_genes_of_a_caller_the_version_does_not_have_fail(results, monkeypatch):
    analysis = make_analysis(
        make_study("PRJ1"), "ERZ1", version="V6.1", results_dir=results("ERZ1")
    )
    monkeypatch.setitem(
        GeneCaller.BY_PIPELINE_VERSION,
        (AssemblyProteins.Source.ASSEMBLY_ANALYSIS, "6.1"),
        {"Pyrodigal": True},
    )
    with pytest.raises(InvalidInput, match="FragGeneScanRS"):
        analyses.ingest_analysis(analysis.accession)
    assert records() == []


def test_an_analysis_of_a_version_without_rules_fails(results):
    analysis = make_analysis(
        make_study("PRJ1"), "ERZ1", version="V5", results_dir=results("ERZ1")
    )
    with pytest.raises((NotImplementedError, UnsupportedPipelineVersion)):
        analyses.ingest_analysis(analysis.accession)
    assert records() == []


def test_an_analysis_without_its_files_fails(tmp_path):
    analysis = make_analysis(make_study("PRJ1"), "ERZ1", results_dir=tmp_path)
    with pytest.raises(FileNotFoundError):
        analyses.ingest_analysis(analysis.accession)
    assert records() == []
    assert query("SELECT count(*) FROM staged_protein") == [(0,)]
