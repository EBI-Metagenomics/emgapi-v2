from ninja import Schema
from pydantic import BaseModel, Field, computed_field


class AnnotationTypeDescriptor(BaseModel):
    title: str = Field(..., description="Explanatory version of the annotation type")
    description: str = Field("")


annotation_type_humanize_map = {
    "Sample-Material": AnnotationTypeDescriptor(
        title="Sample material",
        description="Sample from which the microbiome is extracted",
    ),
    "Body-Site": AnnotationTypeDescriptor(
        title="Body site",
        description="Host body region/structure where microbiome is found",
    ),
    "Host": AnnotationTypeDescriptor(
        title="Host", description="The organism where the microbiome is found"
    ),
    "Engineered": AnnotationTypeDescriptor(
        title="Engineered environment",
        description="Microbiome's man - made environment",
    ),
    "Ecoregion": AnnotationTypeDescriptor(
        title="Ecoregion", description="Microbiome's natural environment"
    ),
    "Date": AnnotationTypeDescriptor(title="Date", description="Sampling date"),
    "Place": AnnotationTypeDescriptor(
        title="Place", description="Microbiome's place or geocoordinates"
    ),
    "Site": AnnotationTypeDescriptor(
        title="Site", description="Microbiome's site within place"
    ),
    "State": AnnotationTypeDescriptor(
        title="State", description="Host/Environment state"
    ),
    "Treatment": AnnotationTypeDescriptor(
        title="Treatment", description="Host/Environment treatments"
    ),
    "Kit": AnnotationTypeDescriptor(
        title="Kit", description="Nucleic acid extraction-kit"
    ),
    "Gene": AnnotationTypeDescriptor(
        title="Gene",
        description="Target gene(s) (e.g. hypervariable regions of 16s/18s rRNA gene)",
    ),
    "Primer": AnnotationTypeDescriptor(title="Primer", description="PCR primers used"),
    "LS": AnnotationTypeDescriptor(
        title="Library strategy", description="e.g. amplicon, whole metagenome"
    ),
    "LCM": AnnotationTypeDescriptor(
        title="Library construction method", description="e.g. paired-end, single-end"
    ),
    "Sequencing": AnnotationTypeDescriptor(
        title="Sequencing platform", description="e.g. Illumina"
    ),
}

sample_processing_annotation_types = ["Sequencing", "LS", "LCM", "Kit", "Primer"]


class EuropePmcAnnotationTag(BaseModel):
    name: str
    uri: str


class EuropePmcAnnotationMention(BaseModel):
    exact: str
    id: str | None = Field(None)
    postfix: str | None = Field(None)
    prefix: str | None = Field(None)
    provider: str = Field("Metagenomic")
    type: str
    tags: list[EuropePmcAnnotationTag] = Field(...)
    section: str | None = Field(None)

    @computed_field
    @property
    def icase_text(self) -> str:
        return self.exact.lower()

    class Config:
        exclude = {"icase_text"}


class EuropePmcAnnotation(BaseModel):
    annotation_text: str
    mentions: list[EuropePmcAnnotationMention]


class EuropePmcAnnotationGroup(BaseModel):
    annotation_type: str
    title: str
    description: str
    annotations: list[EuropePmcAnnotation]


class EuropePmcAnnotationResponse(BaseModel):
    sample_processing: list[EuropePmcAnnotationGroup]
    other: list[EuropePmcAnnotationGroup]


class PublicationAnnotations(Schema, EuropePmcAnnotationResponse):
    """Compatibility response for publication Europe PMC annotations."""

    pass
