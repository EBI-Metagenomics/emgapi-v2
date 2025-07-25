import itertools

import httpx
from django.http import Http404
from pydantic import BaseModel, Field, computed_field

from activate_django_first import EMG_CONFIG

ANNOTATIONS = "annotations"
TYPE = "type"


class AnnotationTypeDescriptor(BaseModel):
    title: str = Field(
        ...,
        description="Explanatory version of the annotation type",
        examples=["Sample material", "Body site"],
    )
    description: str = Field(
        "",
        description="Detailed description of the annotation type",
        examples=[
            "Sample from which the microbiome is extracted",
            "Host body region/structure where microbiome is found",
        ],
    )


# based on http://blog.europepmc.org/2020/11/europe-pmc-publications-metagenomics-annotations.html
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


# sample processing annotations tend to be more accurate than others.
sample_processing_annotation_types = ["Sequencing", "LS", "LCM", "Kit", "Primer"]


class EuropePmcAnnotationTag(BaseModel):
    name: str
    uri: str


class EuropePmcAnnotationMention(BaseModel):
    exact: str = Field(
        ...,
        description="The exact text of the annotation in the text",
        examples=["16S rRNA gene"],
    )
    id: str | None = Field(None)
    postfix: str | None = Field(
        None,
        description="The text immediately following the annotation",
        examples=[" and found it."],
    )
    prefix: str | None = Field(
        None,
        description="The text immediately preceding the annotation",
        examples=["We sequenced the "],
    )
    provider: str = Field(
        "Metagenomic",
        description="The provider of the annotation",
        examples=["Metagenomics"],
    )
    type: str = Field(
        ...,
        description="The type of the annotation",
        examples=list(annotation_type_humanize_map.keys()),
    )
    tags: list[EuropePmcAnnotationTag] = Field(
        ...,
        description="A list of tags that associate the annotation with an ontology term",
    )
    section: str | None = Field(
        None,
        description="The section of the text where the annotation occurs",
        examples=["Methods"],
    )

    @computed_field
    @property
    def icase_text(self) -> str:
        return self.exact.lower()

    class Config:
        exclude = {"icase_text"}


class EuropePmcAnnotation(BaseModel):
    annotation_text: str = Field(
        ..., description="Text of the annotation", examples=["16S rRNA gene"]
    )
    mentions: list[EuropePmcAnnotationMention] = Field(
        ...,
        description="List of occurrence where the annotation is mentioned in the publication",
    )


class EuropePmcAnnotationGroup(BaseModel):
    annotation_type: str = Field(
        ...,
        description="Type (i.e. the concept) of the annotation",
        examples=list(annotation_type_humanize_map.keys()),
    )
    title: str = Field(
        ...,
        description="Explanatory version of the annotation type",
        examples=[a.title for a in annotation_type_humanize_map.values()],
    )
    description: str = Field(
        ...,
        description="Detailed description of the annotation type",
        examples=[a.description for a in annotation_type_humanize_map.values()],
    )
    annotations: list[EuropePmcAnnotation] = Field(
        ..., description="List of annotations of the given type"
    )


class EuropePmcAnnotationResponse(BaseModel):
    sample_processing: list[EuropePmcAnnotationGroup] = Field(
        ..., description="List of sample processing annotations"
    )
    other: list[EuropePmcAnnotationGroup] = Field(
        ..., description="List of other annotations"
    )


def get_epmc_publication_annotations(pubmed_id: int) -> EuropePmcAnnotationResponse:
    """
    Fetch EMERALD-provided Europe PMC metagenomics annotations for a publication and group them by type and text.
    :param pubmed_id: the publication identified in pubmed
    :return: grouped and sorted annotations
    """
    epmc = httpx.get(
        EMG_CONFIG.europe_pmc.annotations_endpoint,
        params={
            "articleIds": f"MED:{pubmed_id}",
            "provider": EMG_CONFIG.europe_pmc.annotations_provider,
        },
    )
    try:
        assert epmc.status_code == 200
        annotations = epmc.json()[0][ANNOTATIONS]
    except (AssertionError, KeyError, IndexError):
        raise Http404

    # Group by annotation type, and within type by icase annotation text
    # Sort within each level by the number of annotations inside.
    annotations.sort(key=lambda annotation: annotation.get(TYPE))

    grouped_annotations: list[EuropePmcAnnotationGroup] = []
    for anno_type, annots_of_type in itertools.groupby(
        annotations, key=lambda annotation: annotation.get("type", "Other")
    ):
        annotation_mentions_in_group = []
        for annot in annots_of_type:
            annotation_mentions_in_group.append(EuropePmcAnnotationMention(**annot))
        grouped_annotations.append(
            EuropePmcAnnotationGroup(
                annotations=[
                    EuropePmcAnnotation(annotation_text=text, mentions=mentions)
                    for text, mentions in itertools.groupby(
                        annotation_mentions_in_group,
                        key=lambda mention: mention.icase_text,
                    )
                ],
                annotation_type=anno_type,
                description=annotation_type_humanize_map.get(
                    anno_type, AnnotationTypeDescriptor(title=anno_type)
                ).description
                or "",
                title=annotation_type_humanize_map.get(
                    anno_type, AnnotationTypeDescriptor(title=anno_type)
                ).title
                or "",
            )
        )

    grouped_annotations.sort(key=lambda group: len(group.annotations), reverse=True)

    # Split off special sample processing annotation groups
    sample_processing_annotations = []
    other_annotations = []

    for group in grouped_annotations:
        if group.annotation_type in sample_processing_annotation_types:
            sample_processing_annotations.append(group)
        else:
            other_annotations.append(group)

    return EuropePmcAnnotationResponse(
        sample_processing=sample_processing_annotations, other=other_annotations
    )


# QUERY_POSSIBLE = 'query_possible'
# STUDY_HAS_ANNOTATIONS = 'study_has_annotations'
#
#
# def get_epmc_publication_annotations_existence_for_sample(sample, study_query_limit=8, publication_query_limit=8):
#     """
#     Determine whether any of the studies related to a sample have publications which are annotated by Europe PMC.
#     :param sample: a MGnify Sample model instance.
#     :param study_query_limit: how many studies may be queried per sample (if more are present, do not fetch)
#     :param publication_query_limit: how many publications per study may be queries (limit set by EPMC API)
#     :return: {'query_possible': bool, 'studies': {<study_id>: {'query_possible': bool, 'has_annotations': bool}}}
#     """
#     response = {QUERY_POSSIBLE: False, STUDY_HAS_ANNOTATIONS: {}}
#     studies = sample.studies.annotate(num_pubs=Count('publications'))
#
#     if not studies.exists():
#         response[QUERY_POSSIBLE] = True
#         return response
#
#     if studies.count() > study_query_limit or \
#             studies.aggregate(max_pubs=Max('num_pubs'))['max_pubs'] > publication_query_limit:
#         return response
#
#     for study in studies.all():
#         pubmeds = [f'MED:{pub.pubmed_id}' for pub in study.publications.all()]
#         epmc = requests.get(settings.EUROPE_PMC['annotations_endpoint'], params={
#             'articleIds': ','.join(pubmeds),
#             'provider': settings.EUROPE_PMC['annotations_provider']
#         })
#
#         try:
#             assert epmc.status_code == 200
#             has_annotations = any([len(pub[ANNOTATIONS]) > 0 for pub in epmc.json()])
#         except (AssertionError, KeyError, IndexError):
#             response[STUDY_HAS_ANNOTATIONS][study.accession] = False
#         else:
#             response[STUDY_HAS_ANNOTATIONS][study.accession] = has_annotations
#         response[QUERY_POSSIBLE] = True
#
#     return response
#
#
# def get_contextual_data_clearing_house_metadata(sample):
#     """
#     Fetch sample metadata from the Elixir Contextual Data Clearing House, if present.
#     :param sample: a MGnify Sample model instance.
#     :return: curations from the CDCH.
#     """
#     endpoint = settings.ELIXIR_CDCH["sample_metadata_endpoint"]
#     response = requests.get(f'{endpoint}{sample.accession}')
#     try:
#         assert response.status_code == 200
#         curations = response.json().get('curations', [])
#     except AssertionError:
#         if response.status_code not in [200, 404]:
#             logging.warning(
#                 f'Non-OK response code from CDCH. '
#                 f'Endpoint: {endpoint}. '
#                 f'Status: {response.status_code}. '
#                 f'Sample: {sample.accession}'
#             )
#         return []
#     except JSONDecodeError:
#         return []
#     return curations
