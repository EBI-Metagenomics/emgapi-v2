import csv
import gzip
from pathlib import Path

import pytest

from proteins.accession.contract import (
    InvalidSequence,
    protein_hash,
    validate_sequence,
)


def test_upper_case_sequence_is_unchanged():
    assert validate_sequence("MKVLAAGIX") == b"MKVLAAGIX"


def test_lower_case_is_upper_cased():
    assert validate_sequence("mkVlaagix") == b"MKVLAAGIX"


def test_case_does_not_change_the_hash():
    assert protein_hash("mkvlaagix") == protein_hash("MKVLAAGIX")


@pytest.mark.parametrize(
    "sequence, character, position",
    [
        ("MKV*", "*", 4),  # trailing stop
        ("MK*V", "*", 3),  # internal stop
        ("MK V", " ", 3),
        ("MKV\n", "\n", 4),
        ("MK1V", "1", 3),
        ("MK-V", "-", 3),
        # Non-ASCII letters that upper() would turn into ASCII ones.
        ("MKVß", "ß", 4),  # -> "SS"
        ("ıMKV", "ı", 1),  # -> "I"
        ("MﬁKV", "ﬁ", 2),  # -> "FI"
    ],
)
def test_invalid_sequence_names_first_offending_character(
    sequence, character, position
):
    with pytest.raises(InvalidSequence) as excinfo:
        validate_sequence(sequence)
    assert excinfo.value.character == character
    assert excinfo.value.position == position
    assert f"{character!r} at position {position}" in str(excinfo.value)


def test_only_the_first_offending_character_is_reported():
    with pytest.raises(InvalidSequence) as excinfo:
        validate_sequence("M*K-V")
    assert (excinfo.value.character, excinfo.value.position) == ("*", 2)


def test_empty_sequence_is_rejected():
    with pytest.raises(InvalidSequence, match="empty sequence"):
        validate_sequence("")


def test_invalid_sequence_is_never_hashed():
    with pytest.raises(InvalidSequence):
        protein_hash("MKV*")


def test_reproduces_hashes_stored_in_the_current_database():
    # 10,000 rows of the current `protein` table, from two of its 64 partitions,
    # 1,000 of them containing X. A mismatch would give an existing protein a second MGYP.
    fixture = Path(__file__).parent.parent / "fixtures" / "stored_protein_hashes.tsv.gz"
    with gzip.open(fixture, "rt") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert len(rows) == 10_000
    mismatches = [
        row["sequence_sha256sum"]
        for row in rows
        if protein_hash(row["sequence"]).hex() != row["sequence_sha256sum"]
    ]
    assert mismatches == []
