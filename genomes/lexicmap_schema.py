import re

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SearchQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sequence: str = Field(min_length=1, max_length=100_000)
    max_results: int = Field(100, ge=1, le=1000)
    min_identity: float = Field(70, ge=0, le=100, allow_inf_nan=False)
    min_query_coverage: float = Field(70, ge=0, le=100, allow_inf_nan=False)
    catalogues_filter: list[str] | None = None

    @field_validator("sequence")
    @classmethod
    def validate_sequence(cls, value):
        lines = value.strip().splitlines()
        if lines and lines[0].startswith(">"):
            lines = lines[1:]
        sequence = "".join("".join(lines).split()).upper()
        if not re.fullmatch(r"[ACGTURYSWKMBDHVN]+", sequence):
            raise ValueError(
                "Provide one nucleotide sequence, as raw DNA or single-record FASTA"
            )
        # Match our search command's minimum HSP length, not the upstream
        # recommendation about query lengths with default seed spacing.
        if len(sequence) < 50:
            raise ValueError(
                "This search requires at least 50 bases (minimum alignment length)"
            )
        return sequence


class LexicMapMatch(BaseModel):
    genome: str
    catalogue: str
    query_coverage: float
    identity: float
    bitscore: float
    evalue: float
    sequence_id: str
    query_start: int
    query_end: int
    subject_start: int
    subject_end: int
    strand: str
