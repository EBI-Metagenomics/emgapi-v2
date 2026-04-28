from typing import Optional
from pathlib import Path

import pytest

import analyses.models
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs
import workflows.flows.analyse_study_tasks.amplicon.import_primer_identification_for_imported_analyses as flow_module


class FakeAnalysis:
    def __init__(
        self,
        id: int,
        accession: str,
        *,
        amplicon: bool = True,
        have_ann: bool = True,
        results_dir: Optional[str] = ".",
    ):
        self.id = id
        self.accession = accession
        # Expose the same enums on the instance as the real model provides
        self.ExperimentTypes = analyses.models.Analysis.ExperimentTypes
        self.AnalysisStates = analyses.models.Analysis.AnalysisStates
        self.experiment_type = self.ExperimentTypes.AMPLICON if amplicon else "NOT_AMP"
        # status is dict-like; AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED must be True to proceed
        self.status = {
            self.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED: bool(have_ann)
        }
        self.results_dir = results_dir

    def refresh_from_db(self):
        return None


class FakeQuery:
    """A very small stand-in for Django's queryset/manager used in this flow."""

    def __init__(self, items: list[FakeAnalysis]):
        # Keep items sorted by id for deterministic behaviour
        self._items = list(sorted(items, key=lambda a: a.id))

    # Queryset chaining API used by the flow
    def select_related(self, *_args, **_kwargs):
        return self

    def filter(self, *, experiment_type=None, **_kwargs):
        if experiment_type is None:
            return self
        kept = [a for a in self._items if a.experiment_type == experiment_type]
        return FakeQuery(kept)

    def filter_by_statuses(self, statuses):
        kept = [
            a for a in self._items if all(a.status.get(st, False) for st in statuses)
        ]
        return FakeQuery(kept)

    def order_by(self, field_name: str):
        reverse = field_name.startswith("-")
        key = field_name.lstrip("-")
        if key == "id":
            sorted_items = sorted(self._items, key=lambda a: a.id, reverse=reverse)
        else:
            sorted_items = list(self._items)
        return FakeQuery(sorted_items)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return FakeQuery(self._items[item])
        raise TypeError("FakeQuery only supports slicing for __getitem__")

    def __iter__(self):
        return iter(self._items)

    def count(self):
        return len(self._items)

    # Manager-like API used by the task function
    def get(self, *, id: int):
        for a in self._items:
            if a.id == id:
                return a
        raise analyses.models.Analysis.DoesNotExist()  # type: ignore[attr-defined]


@pytest.fixture
def fake_objects():
    # Provide a small set of analyses: 2 valid, 1 filtered due to non-amplicon, 1 missing annotations, 1 missing results_dir
    items = [
        FakeAnalysis(
            2, "MGYA00000002", amplicon=True, have_ann=True, results_dir="/tmp/r2"
        ),
        FakeAnalysis(
            1, "MGYA00000001", amplicon=True, have_ann=True, results_dir="/tmp/r1"
        ),
        FakeAnalysis(
            3, "MGYA00000003", amplicon=False, have_ann=True, results_dir="/tmp/r3"
        ),
        FakeAnalysis(
            4, "MGYA00000004", amplicon=True, have_ann=False, results_dir="/tmp/r4"
        ),
        FakeAnalysis(5, "MGYA00000005", amplicon=True, have_ann=True, results_dir=None),
    ]
    return FakeQuery(items)


@pytest.fixture
def patch_analysis_objects(monkeypatch, fake_objects):
    # Patch the Analysis.objects to our fake chainable queryset/manager
    monkeypatch.setattr(
        analyses.models.Analysis, "objects", fake_objects, raising=False
    )


@pytest.fixture
def patch_import_fn(monkeypatch):
    calls = {"count": 0, "args": []}

    def _fake_import_fn(
        analysis, dir_for_analysis: Path, allow_non_exist: bool = False
    ):
        calls["count"] += 1
        calls["args"].append(
            (analysis.accession, str(dir_for_analysis), allow_non_exist)
        )
        # Do nothing, pretend success
        return None

    # Patch the symbol used inside the flow module
    monkeypatch.setattr(
        flow_module, "import_primer_identification", _fake_import_fn, raising=True
    )
    return calls


def test_flow_processes_only_valid_amplicon_analyses_and_logs(
    monkeypatch, patch_analysis_objects, patch_import_fn
):
    # Run without max_count to process all valid items (2 will be processed successfully; 3 others skipped)
    logged = run_flow_and_capture_logs(
        flow_module.import_primer_identification_for_imported_analyses
    )

    logs = logged.logs
    # It should report finding eligible analyses (amplicon + annotations imported), which are 3 here (ids 1,2,5)
    assert "Found 3 amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED" in logs

    # Success messages for the two valid accessions in id order (ordered by id)
    assert "[MGYA00000001] primer-identification import done" in logs
    assert "[MGYA00000002] primer-identification import done" in logs

    # Skip due to missing results_dir should appear
    assert (
        "[MGYA00000005] skipped/failed: results_dir is not set for this analysis"
        in logs
    )

    # Final summary should reflect 2 successes and 1 failure/skip among eligible ones
    assert "Successes=2, Failures/Skipped=1" in logs

    # Import function should have been called exactly for the two valid analyses
    assert patch_import_fn["count"] == 2
    # And it should receive allow_non_exist=True as coded in the task
    assert all(arg[2] is True for arg in patch_import_fn["args"])  # type: ignore[index]


def test_flow_respects_max_count(monkeypatch, fake_objects):
    # Restrict to first 1 analysis after ordering by id
    monkeypatch.setattr(
        analyses.models.Analysis, "objects", fake_objects, raising=False
    )

    logged = run_flow_and_capture_logs(
        flow_module.import_primer_identification_for_imported_analyses, max_count=1
    )
    logs = logged.logs

    # Should find and process only one eligible analysis
    assert "Found 1 amplicon analyses with ANALYSIS_ANNOTATIONS_IMPORTED" in logs
    assert "[MGYA00000001] primer-identification import done" in logs
    # And not mention the second accession
    assert "MGYA00000002" not in logs or logs.count("MGYA00000002") == 0
