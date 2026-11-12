"""Hash contract v1: when two sequences are the same protein, or the same contig of an assembly.

A sequence's identity is the sha256 of the sequence as it is. Every piece of code that hashes
a protein or a contig calls this module, so that no path can hash a sequence differently and
silently allocate a second MGYP for an existing protein.

The contract is a guard: it rejects, it never repairs, because the sequence that is hashed
must be exactly the one MGnify publishes. Lower case, a `*`, whitespace, a digit or a
non-ASCII character is an error to be fixed where the sequence was produced.
"""

import hashlib
import re

# Any character that is not an upper-case ASCII letter. The 26 letters are exactly the
# IUPAC extended protein alphabet: the 20 standard amino acids, the ambiguity codes
# B, J, X, Z, and the rare amino acids U (selenocysteine) and O (pyrrolysine).
# They also hold the IUPAC nucleotide codes, N included.
_INVALID_CHAR = re.compile(r"[^A-Z]")


class InvalidSequence(ValueError):
    """A sequence that cannot be hashed: empty, or not only upper-case ASCII letters."""


def validate_sequence(sequence: str) -> bytes:
    """Return the sequence as ASCII bytes, or raise InvalidSequence."""
    if not sequence:
        raise InvalidSequence("empty sequence")
    if match := _INVALID_CHAR.search(sequence):
        raise InvalidSequence(
            f"invalid character {match.group()!r} at position {match.start() + 1}"
        )
    return sequence.encode("ascii")


def protein_hash(sequence: str) -> bytes:
    """The protein's identity: sha256 of its validated sequence (32 bytes)."""
    return hashlib.sha256(validate_sequence(sequence)).digest()


def contig_hash(sequence: str) -> bytes:
    """The contig's identity within its assembly: sha256 of its validated sequence (32 bytes)."""
    return hashlib.sha256(validate_sequence(sequence)).digest()
