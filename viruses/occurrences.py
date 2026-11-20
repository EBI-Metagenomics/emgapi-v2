"""Versioned provenance snapshots for viral cluster representatives and members."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ViralFragmentOccurrence(BaseModel):
    """One detection of a fragment in a genome release or assembly analysis."""

    model_config = ConfigDict(extra="forbid")

    fragment_accession: str = Field(pattern=r"^MGYV\d{10}$")
    nt_sequence_sha256: str = Field(default="", pattern=r"^([a-f0-9]{64})?$")
    original_id: str
    prediction_tool: str = ""
    source_type: Literal["genome", "analysis"]
    source_accession: str = Field(min_length=1, max_length=100)
    source_genome_accession: str | None = None
    catalogue_genome_id: int | None = Field(default=None, gt=0)
    genome_catalogue_id: str | None = None
    analysis_id: int | None = Field(default=None, gt=0)
    analysis_accession: str | None = None
    source_sample: str = ""
    source_study: str = ""
    source_lineage: str = ""

    @model_validator(mode="after")
    def validate_source(self):
        """Prevent mixed genome and analysis references."""
        if self.source_type == "genome" and (
            self.analysis_id or self.analysis_accession
        ):
            raise ValueError("A genome occurrence cannot reference an analysis")
        if self.source_type == "analysis" and (
            self.catalogue_genome_id
            or self.genome_catalogue_id
            or self.source_genome_accession
        ):
            raise ValueError("An analysis occurrence cannot reference a genome")
        return self
