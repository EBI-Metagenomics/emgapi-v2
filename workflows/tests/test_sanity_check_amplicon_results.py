import pytest

from workflows.flows.analyse_study_tasks.amplicon.sanity_check_amplicon_results import (
    _dada2_taxonomy_summary_failure_reason,
)

RUN_ID = "SRR123456"
REGION = "16S-V4"


def _mseq_path(db):
    return db / f"{RUN_ID}_{db.name}.mseq"


def test_header_only_mseq_does_not_require_krona_files(tmp_path):
    db = tmp_path / "DADA2-SILVA"
    db.mkdir()
    _mseq_path(db).write_text("#query\tdbhit\n")

    assert _dada2_taxonomy_summary_failure_reason(db, RUN_ID, [REGION]) is None


def test_mseq_with_data_requires_krona_files(tmp_path):
    db = tmp_path / "DADA2-SILVA"
    db.mkdir()
    _mseq_path(db).write_text("#query\tdbhit\nseq-1\thit-1\n")

    assert _dada2_taxonomy_summary_failure_reason(db, RUN_ID, [REGION]) == (
        f"missing {REGION} file in {db}"
    )

    (db / f"{RUN_ID}_{REGION}.html").touch()
    (db / f"{RUN_ID}_{REGION}_{db.name}_asv_krona_counts.txt").touch()
    assert _dada2_taxonomy_summary_failure_reason(db, RUN_ID, [REGION]) is None


@pytest.mark.parametrize(
    "contents",
    [
        b"",
        b"not-an-mseq-header\n",
        b"\xff\xfe\n",
    ],
    ids=["empty", "malformed-header", "invalid-utf8"],
)
def test_invalid_mseq_fails_sanity_check(tmp_path, contents):
    db = tmp_path / "DADA2-SILVA"
    db.mkdir()
    _mseq_path(db).write_bytes(contents)

    reason = _dada2_taxonomy_summary_failure_reason(db, RUN_ID, [REGION])

    assert reason.startswith(f"invalid mseq in {db}")
