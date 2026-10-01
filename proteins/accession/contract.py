"""Hash contract v1: when two protein sequences are the same protein.

A protein's identity is the sha256 of its upper-cased sequence. Every piece of code that
hashes a protein calls this module, so that no path can hash a sequence differently and
silently allocate a second MGYP for an existing protein.

The contract is a guard: it rejects, it never repairs. Upper-casing is the only change
ever made to a sequence, because the sequence that is hashed must be exactly the one
MGnify publishes. A `*`, whitespace, a digit or a non-ASCII character is an error to be
fixed where the sequence was produced.
"""

import hashlib


class InvalidSequence(ValueError):
    """A sequence that fails the contract, with its first offending character."""

    def __init__(self, sequence: str):
        self.sequence = sequence
        # 1-based position, as for residues; None for an empty sequence.
        self.position, self.character = next(
            (
                (i, c)
                for i, c in enumerate(sequence, 1)
                if not (c.isascii() and c.isalpha())
            ),
            (None, None),
        )
        super().__init__(
            "empty sequence"
            if self.position is None
            else f"invalid character {self.character!r} at position {self.position}"
        )


def validate_sequence(sequence: str) -> bytes:
    """Return the sequence as upper-cased ASCII bytes, or raise InvalidSequence."""
    # isascii() before upper(): upper() maps some non-ASCII letters to ASCII ones
    # ("ß" -> "SS", "ı" -> "I"), which would then pass.
    if not sequence or not sequence.isascii() or not sequence.isalpha():
        raise InvalidSequence(sequence)
    return sequence.upper().encode("ascii")


def protein_hash(sequence: str) -> bytes:
    """The protein's identity: sha256 of its validated sequence, 32 bytes."""
    return hashlib.sha256(validate_sequence(sequence)).digest()
