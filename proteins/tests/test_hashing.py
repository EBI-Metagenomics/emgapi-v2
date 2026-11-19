import csv
import gzip
import hashlib
from pathlib import Path

import pytest

from proteins.accession.hashing import (
    InvalidSequence,
    contig_hash,
    protein_hash,
    validate_sequence,
)


def test_upper_case_sequence_is_unchanged():
    assert validate_sequence("MKVLAAGIX") == b"MKVLAAGIX"


def test_the_hash_is_of_the_sequence_as_it_is():
    assert protein_hash("MKVLAAGIX") == hashlib.sha256(b"MKVLAAGIX").digest()
    assert contig_hash("ACGTN") == hashlib.sha256(b"ACGTN").digest()


@pytest.mark.parametrize(
    "sequence, character, position",
    [
        ("mKV", "m", 1),  # lower case is not upper-cased
        ("MKV*", "*", 4),  # trailing stop
        ("MK*V", "*", 3),  # internal stop
        ("MK V", " ", 3),
        ("MKV\n", "\n", 4),
        ("MK1V", "1", 3),
        ("MK-V", "-", 3),
        # Non-ASCII letters, which upper() would turn into ASCII ones.
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
    assert (
        str(excinfo.value) == f"invalid character {character!r} at position {position}"
    )


def test_only_the_first_offending_character_is_reported():
    with pytest.raises(InvalidSequence, match="'\\*' at position 2"):
        validate_sequence("M*K-V")


def test_empty_sequence_is_rejected():
    with pytest.raises(InvalidSequence, match="empty sequence"):
        validate_sequence("")


def test_invalid_sequence_is_never_hashed():
    with pytest.raises(InvalidSequence):
        protein_hash("MKV*")
    with pytest.raises(InvalidSequence):
        contig_hash("acgt")


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
