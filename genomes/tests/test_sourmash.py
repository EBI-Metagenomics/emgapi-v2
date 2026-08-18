import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from genomes import sourmash


class _FakeMinhash:
    def __init__(self, *, ksize=31, scaled=1000, hash_count=1):
        self.ksize = ksize
        self.scaled = scaled
        self._hash_count = hash_count

    def __len__(self):
        return self._hash_count


class _FakeQuery:
    def __init__(self, *, ksize=31, scaled=1000, hash_count=1):
        self.minhash = _FakeMinhash(
            ksize=ksize,
            scaled=scaled,
            hash_count=hash_count,
        )


class _FakeDatabase:
    def __init__(self, siglist):
        self._siglist = siglist

    def counter_gather(self, _query, _threshold_bp):
        return SimpleNamespace(siglist=self._siglist)


class _FakeGatherResult:
    def __init__(
        self,
        *,
        filename,
        intersect_bp=3158000,
        f_unique_weighted=1.0,
        f_match=1.0,
        gatherresultdict=None,
    ):
        self.intersect_bp = intersect_bp
        self.f_unique_weighted = f_unique_weighted
        self.f_match = f_match
        self.match = SimpleNamespace(filename=filename)
        self.gatherresultdict = gatherresultdict or {
            "intersect_bp": intersect_bp,
            "filename": filename,
            "md5": "abc123",
        }


@pytest.fixture(autouse=True)
def clear_name_map_cache():
    sourmash._load_name_map.cache_clear()
    yield
    sourmash._load_name_map.cache_clear()


@pytest.fixture
def fake_sourmash_runtime(monkeypatch):
    state = {
        "query": _FakeQuery(),
        "databases": [],
        "results": [],
        "query_call": None,
        "database_call": None,
        "gather_call": None,
    }

    sourmash_pkg = ModuleType("sourmash")
    commands_mod = ModuleType("sourmash.commands")
    search_mod = ModuleType("sourmash.search")
    args_mod = ModuleType("sourmash.sourmash_args")

    class FakeSaveSignaturesToLocation:
        def __init__(self, _destination):
            self.siglist = []

        def open(self):
            return None

        def add_many(self, signatures):
            self.siglist.extend(signatures)

        def close(self):
            return None

        def __len__(self):
            return len(self.siglist)

    class FakeGatherDatabases:
        def __init__(self, query, counters, threshold_bp, ignore_abundance):
            state["gather_call"] = {
                "query": query,
                "counters": counters,
                "threshold_bp": threshold_bp,
                "ignore_abundance": ignore_abundance,
            }

        def __iter__(self):
            return iter(state["results"])

    def fake_load_query_signature(path, *, ksize, select_moltype):
        state["query_call"] = {
            "path": path,
            "ksize": ksize,
            "select_moltype": select_moltype,
        }
        return state["query"]

    def fake_load_dbs_and_sigs(paths, query, _keep_sigs, cache_size=None):
        state["database_call"] = {
            "paths": paths,
            "query": query,
            "cache_size": cache_size,
        }
        return state["databases"]

    commands_mod.SaveSignaturesToLocation = FakeSaveSignaturesToLocation
    search_mod.GatherDatabases = FakeGatherDatabases
    search_mod.format_bp = lambda bp: f"{bp} bp"
    args_mod.FileOutputCSV = lambda path: open(path, "w", newline="", encoding="utf-8")
    args_mod.get_moltype = lambda _query: "DNA"
    args_mod.load_dbs_and_sigs = fake_load_dbs_and_sigs
    args_mod.load_query_signature = fake_load_query_signature

    sourmash_pkg.commands = commands_mod
    sourmash_pkg.search = search_mod
    sourmash_pkg.sourmash_args = args_mod

    monkeypatch.setitem(sys.modules, "sourmash", sourmash_pkg)
    monkeypatch.setitem(sys.modules, "sourmash.commands", commands_mod)
    monkeypatch.setitem(sys.modules, "sourmash.search", search_mod)
    monkeypatch.setitem(sys.modules, "sourmash.sourmash_args", args_mod)

    return state


def test_load_name_map_empty_path_returns_empty_mapping():
    assert sourmash._load_name_map("") == {}


def test_load_name_map_missing_file_logs_warning_and_returns_empty(caplog, tmp_path):
    missing_path = tmp_path / "missing-name-map.json"

    with caplog.at_level("WARNING"):
        result = sourmash._load_name_map(str(missing_path))

    assert result == {}
    assert "does not exist" in caplog.text


def test_load_name_map_reads_json_file(tmp_path):
    name_map_path = tmp_path / "name-map.json"
    expected = {"GUT_GENOME": "MGYG000000001"}
    name_map_path.write_text(json.dumps(expected), encoding="utf-8")

    assert sourmash._load_name_map(str(name_map_path)) == expected


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("/tmp/query.sig", "query"),
        ("MGYG000000001.fna.gz", "MGYG000000001"),
        ("nested/path/genome.fa.gz", "genome"),
        ("already_clean_name", "already_clean_name"),
    ],
)
def test_strip_known_suffixes(filename, expected):
    assert sourmash._strip_known_suffixes(filename) == expected


def test_get_accession_from_filename_uses_name_map_for_gut_prefix(tmp_path):
    name_map_path = tmp_path / "name-map.json"
    name_map_path.write_text(
        json.dumps({"GUT_SPECIAL": "MGYG000000001"}),
        encoding="utf-8",
    )

    accession = sourmash.get_accession_from_filename(
        "nested/GUT_SPECIAL.fna.gz",
        name_map_path=str(name_map_path),
    )

    assert accession == "MGYG000000001"


def test_run_sourmash_gather_raises_for_missing_query_file(
    tmp_path, fake_sourmash_runtime
):
    artifact_path = tmp_path / "genomes_index.sbt.json"
    artifact_path.write_text("{}", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="query signature does not exist"):
        sourmash.run_sourmash_gather(
            query_path=str(tmp_path / "missing.sig"),
            original_filename="query.sig",
            catalogue_id="human-gut-v2-0",
            artifact_path=str(artifact_path),
            result_path=str(tmp_path / "results" / "result.csv"),
        )


def test_run_sourmash_gather_raises_for_missing_search_artifact(
    tmp_path, fake_sourmash_runtime
):
    query_path = tmp_path / "query.sig"
    query_path.write_text("{}", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="search artifact does not exist"):
        sourmash.run_sourmash_gather(
            query_path=str(query_path),
            original_filename="query.sig",
            catalogue_id="human-gut-v2-0",
            artifact_path=str(tmp_path / "missing.sbt.json"),
            result_path=str(tmp_path / "results" / "result.csv"),
        )


def test_run_sourmash_gather_requires_scaled_query(tmp_path, fake_sourmash_runtime):
    query_path = tmp_path / "query.sig"
    artifact_path = tmp_path / "genomes_index.sbt.json"
    query_path.write_text("{}", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")
    fake_sourmash_runtime["query"] = _FakeQuery(scaled=0, hash_count=10)

    with pytest.raises(ValueError, match="created with --scaled"):
        sourmash.run_sourmash_gather(
            query_path=str(query_path),
            original_filename="query.sig",
            catalogue_id="human-gut-v2-0",
            artifact_path=str(artifact_path),
            result_path=str(tmp_path / "results" / "result.csv"),
        )


def test_run_sourmash_gather_requires_query_hashes(tmp_path, fake_sourmash_runtime):
    query_path = tmp_path / "query.sig"
    artifact_path = tmp_path / "genomes_index.sbt.json"
    query_path.write_text("{}", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")
    fake_sourmash_runtime["query"] = _FakeQuery(scaled=1000, hash_count=0)

    with pytest.raises(ValueError, match="does not contain any hashes"):
        sourmash.run_sourmash_gather(
            query_path=str(query_path),
            original_filename="query.sig",
            catalogue_id="human-gut-v2-0",
            artifact_path=str(artifact_path),
            result_path=str(tmp_path / "results" / "result.csv"),
        )


def test_run_sourmash_gather_requires_available_databases(
    tmp_path, fake_sourmash_runtime
):
    query_path = tmp_path / "query.sig"
    artifact_path = tmp_path / "genomes_index.sbt.json"
    query_path.write_text("{}", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")
    fake_sourmash_runtime["query"] = _FakeQuery(scaled=1000, hash_count=10)
    fake_sourmash_runtime["databases"] = []

    with pytest.raises(ValueError, match="No sourmash databases were available"):
        sourmash.run_sourmash_gather(
            query_path=str(query_path),
            original_filename="query.sig",
            catalogue_id="human-gut-v2-0",
            artifact_path=str(artifact_path),
            result_path=str(tmp_path / "results" / "result.csv"),
        )


def test_run_sourmash_gather_returns_no_results_when_gather_finds_nothing(
    tmp_path, fake_sourmash_runtime
):
    query_path = tmp_path / "query.sig"
    artifact_path = tmp_path / "genomes_index.sbt.json"
    query_path.write_text("{}", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")
    fake_sourmash_runtime["query"] = _FakeQuery(scaled=1000, hash_count=10)
    fake_sourmash_runtime["databases"] = [_FakeDatabase(["sig-a"])]
    fake_sourmash_runtime["results"] = []

    summary = sourmash.run_sourmash_gather(
        query_path=str(query_path),
        original_filename="query.sig",
        catalogue_id="human-gut-v2-0",
        artifact_path=str(artifact_path),
        result_path=str(tmp_path / "results" / "result.csv"),
    )

    assert summary == {
        "status": "NO_RESULTS",
        "catalog": "human-gut-v2-0",
        "query_filename": "query.sig",
        "md5_name": "query.sig",
    }


def test_run_sourmash_gather_writes_csv_and_returns_first_match(
    tmp_path, fake_sourmash_runtime
):
    query_path = tmp_path / "query.sig"
    artifact_path = tmp_path / "genomes_index.sbt.json"
    name_map_path = tmp_path / "name-map.json"
    result_path = tmp_path / "results" / "job-1" / "result.csv"
    query_path.write_text("{}", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")
    name_map_path.write_text(
        json.dumps({"GUT_MATCH": "MGYG000000001"}),
        encoding="utf-8",
    )

    fake_sourmash_runtime["query"] = _FakeQuery(scaled=1000, hash_count=10)
    fake_sourmash_runtime["databases"] = [
        _FakeDatabase(["sig-a"]),
        _FakeDatabase(["sig-b"]),
    ]
    fake_sourmash_runtime["results"] = [
        _FakeGatherResult(
            filename="/tmp/GUT_MATCH.fna.gz",
            intersect_bp=3158000,
            f_unique_weighted=0.75,
            f_match=0.5,
            gatherresultdict={
                "intersect_bp": 3158000,
                "filename": "/tmp/GUT_MATCH.fna.gz",
                "md5": "abc123",
            },
        ),
        _FakeGatherResult(
            filename="/tmp/MGYG000000999.fna.gz",
            intersect_bp=1000000,
            f_unique_weighted=0.2,
            f_match=0.1,
            gatherresultdict={
                "intersect_bp": 1000000,
                "filename": "/tmp/MGYG000000999.fna.gz",
                "md5": "def456",
            },
        ),
    ]

    summary = sourmash.run_sourmash_gather(
        query_path=str(query_path),
        original_filename="query.sig",
        catalogue_id="human-gut-v2-0",
        artifact_path=str(artifact_path),
        result_path=str(result_path),
        threshold_bp=12345,
        ignore_abundance=True,
        name_map_path=str(name_map_path),
    )

    assert summary == {
        "overlap": "3158000 bp",
        "p_query": "75.0%",
        "p_match": "50.0%",
        "match": "MGYG000000001",
        "catalog": "human-gut-v2-0",
        "query_filename": "query.sig",
        "md5_name": "query.sig",
        "matches": 2,
    }
    assert fake_sourmash_runtime["query_call"] == {
        "path": str(query_path),
        "ksize": 31,
        "select_moltype": "DNA",
    }
    assert fake_sourmash_runtime["database_call"]["paths"] == [str(artifact_path)]
    assert fake_sourmash_runtime["gather_call"]["threshold_bp"] == 12345
    assert fake_sourmash_runtime["gather_call"]["ignore_abundance"] is True
    assert result_path.exists()
    csv_text = result_path.read_text(encoding="utf-8")
    assert "intersect_bp" in csv_text
    assert "3158000" in csv_text
