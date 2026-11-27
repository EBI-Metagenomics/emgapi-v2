"""Tests for the Rhea task and existing-result backfill."""

from unittest.mock import Mock, PropertyMock, patch

import pytest

from analyses.models import AnalysisMoleculeIndex
from workflows.flows.analysis.assembly.flows.backfill_rhea import backfill_rhea
from workflows.flows.analysis.assembly.tasks.import_rhea import import_rhea_annotations


def test_rhea_task_delegates_to_atomic_import(tmp_path):
    with patch(
        "workflows.flows.analysis.assembly.tasks.import_rhea.import_rhea",
        return_value=3,
    ) as importer:
        assert import_rhea_annotations.fn(12, tmp_path / "rhea.tsv.gz") == 3
        importer.assert_called_once_with(12, tmp_path / "rhea.tsv.gz")


@pytest.mark.django_db
def test_backfill_uses_public_downloads(assembly_analysis):
    analysis = assembly_analysis
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    namespace = "workflows.flows.analysis.assembly.flows.backfill_rhea"
    download = Mock(download_group="functional_annotation.rhea_reactions")
    response = Mock()
    response.iter_bytes.return_value = [b"compressed fixture"]
    with (
        patch(namespace + ".get_run_logger"),
        patch(
            "analyses.models.Analysis.downloads_as_objects",
            new_callable=PropertyMock,
            return_value=[download],
        ),
        patch(
            namespace + ".MGnifyAnalysisDownloadFile.from_parent",
            return_value=[Mock(url="https://example.org/rhea.tsv.gz")],
        ),
        patch(namespace + ".httpx.stream") as stream,
        patch(namespace + ".import_rhea_annotations", return_value=1) as importer,
    ):
        stream.return_value.__enter__.return_value = response
        backfill_rhea.fn(study_accession=analysis.study.accession)
        assert importer.call_args.args[0] == analysis.id
        assert importer.call_args.args[1].name == "proteins2rhea.tsv.gz"


@pytest.mark.django_db
def test_backfill_ignores_private_analyses(assembly_analysis):
    analysis = assembly_analysis
    analysis.is_private = True
    analysis.save()
    namespace = "workflows.flows.analysis.assembly.flows.backfill_rhea"
    with (
        patch(namespace + ".get_run_logger"),
        patch(namespace + ".httpx.stream") as stream,
    ):
        backfill_rhea.fn()
        stream.assert_not_called()


@pytest.mark.django_db
def test_backfill_skips_successful_empty_index(assembly_analysis):
    analysis = assembly_analysis
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    AnalysisMoleculeIndex.objects.create(analysis=analysis, chebi_ids=[])
    namespace = "workflows.flows.analysis.assembly.flows.backfill_rhea"
    with (
        patch(namespace + ".get_run_logger"),
        patch(namespace + ".httpx.stream") as stream,
    ):
        backfill_rhea.fn()
        stream.assert_not_called()
