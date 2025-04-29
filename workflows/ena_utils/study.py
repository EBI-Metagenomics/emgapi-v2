from datetime import date
from typing import Optional

from pydantic import Field

from emgapiv2.enum_utils import FutureStrEnum
from workflows.ena_utils.abstract import _ENAQueryConditions


class ENAStudyQuery(_ENAQueryConditions):
    # From: https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result=study 2025/04/28
    # Some are controlled values not yet controlled here
    breed: Optional[str] = Field(None, description="breed")
    broker_name: Optional[str] = Field(None, description="broker name")
    center_name: Optional[str] = Field(None, description="Submitting center")
    cultivar: Optional[str] = Field(
        None,
        description="cultivar (cultivated variety) of plant from which sample was obtained",
    )
    datahub: Optional[str] = Field(None, description="DCC datahub name")
    description: Optional[str] = Field(None, description="brief sequence description")
    first_public: Optional[date] = Field(None, description="date when made public")
    geo_accession: Optional[str] = Field(None, description="GEO accession")
    isolate: Optional[str] = Field(
        None, description="individual isolate from which sample was obtained"
    )
    keywords: Optional[str] = Field(
        None, description="keywords associated with sequence"
    )
    last_updated: Optional[date] = Field(None, description="date when last updated")
    parent_study_accession: Optional[str] = Field(
        None, description="parent study accession"
    )
    project_name: Optional[str] = Field(
        None,
        description="name of the project within which the sequencing was organized",
    )
    scientific_name: Optional[str] = Field(
        None, description="scientific name of an organism"
    )
    secondary_study_accession: Optional[str] = Field(
        None, description="secondary study accession number"
    )
    secondary_study_alias: Optional[str] = Field(None, description="Submitting center")
    secondary_study_center_name: Optional[str] = Field(
        None, description="Submitting center"
    )
    status: Optional[int] = Field(None, description="Status")
    strain: Optional[str] = Field(
        None, description="strain from which sample was obtained"
    )
    study_accession: Optional[str] = Field(None, description="study accession number")
    study_alias: Optional[str] = Field(
        None, description="submitter&#x27;s name for the study"
    )
    study_description: Optional[str] = Field(
        None, description="detailed sequencing study description"
    )
    study_name: Optional[str] = Field(None, description="sequencing study name")
    study_title: Optional[str] = Field(
        None, description="brief sequencing study description"
    )
    submission_tool: Optional[str] = Field(None, description="Submission tool")
    tag: Optional[str] = Field(None, description="Classification Tags")
    tax_division: Optional[str] = Field(None, description="taxonomic division")
    tax_id: Optional[str] = Field(None, description="NCBI taxonomic classification")


class ENAStudyFields(FutureStrEnum):
    # from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=study 2025-04-28
    BREED = "breed"  # breed
    BROKER_NAME = "broker_name"  # broker name
    CENTER_NAME = "center_name"  # Submitting center
    CULTIVAR = "cultivar"  # cultivar (cultivated variety) of plant from which sample was obtained
    DATAHUB = "datahub"  # DCC datahub name
    DESCRIPTION = "description"  # brief sequence description
    FIRST_PUBLIC = "first_public"  # date when made public
    GEO_ACCESSION = "geo_accession"  # GEO accession
    ISOLATE = "isolate"  # individual isolate from which sample was obtained
    KEYWORDS = "keywords"  # keywords associated with sequence
    LAST_UPDATED = "last_updated"  # date when last updated
    PARENT_STUDY_ACCESSION = "parent_study_accession"  # parent study accession
    PROJECT_NAME = (
        "project_name"  # name of the project within which the sequencing was organized
    )
    SCIENTIFIC_NAME = "scientific_name"  # scientific name of an organism
    SECONDARY_STUDY_ACCESSION = (
        "secondary_study_accession"  # secondary study accession number
    )
    SECONDARY_STUDY_ALIAS = "secondary_study_alias"  # Submitting center
    SECONDARY_STUDY_CENTER_NAME = "secondary_study_center_name"  # Submitting center
    STATUS = "status"  # Status
    STRAIN = "strain"  # strain from which sample was obtained
    STUDY_ACCESSION = "study_accession"  # study accession number
    STUDY_ALIAS = "study_alias"  # submitter's name for the study
    STUDY_DESCRIPTION = "study_description"  # detailed sequencing study description
    STUDY_NAME = "study_name"  # sequencing study name
    STUDY_TITLE = "study_title"  # brief sequencing study description
    SUBMISSION_TOOL = "submission_tool"  # Submission tool
    TAG = "tag"  # Classification Tags
    TAX_DIVISION = "tax_division"  # taxonomic division
    TAX_ID = "tax_id"  # NCBI taxonomic classification
    TAX_LINEAGE = "tax_lineage"  # Complete taxonomic lineage for an organism
