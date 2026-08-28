import itertools
from datetime import datetime
from typing import Iterable

import httpx
from django.db import transaction
from django.db.models import Exists, OuterRef

from activate_django_first import EMG_CONFIG

from analyses.models import Publication
from curations.models import (
    EuropePmcAnnotation,
    EuropePmcAnnotationGroup,
    EuropePmcAnnotationMention,
    EuropePmcAnnotationTag,
    EuropePmcPublicationCuration,
)
from curations.schemas import (
    AnnotationTypeDescriptor,
    EuropePmcAnnotationResponse,
    annotation_type_humanize_map,
    sample_processing_annotation_types,
)
from curations.schemas import (
    EuropePmcAnnotation as EuropePmcAnnotationSchema,
)
from curations.schemas import (
    EuropePmcAnnotationGroup as EuropePmcAnnotationGroupSchema,
)
from curations.schemas import (
    EuropePmcAnnotationMention as EuropePmcAnnotationMentionSchema,
)

EUROPE_PMC_PROVIDER = "europe_pmc"
ANNOTATIONS = "annotations"
TYPE = "type"


class EuropePmcProviderError(RuntimeError):
    """Raised when Europe PMC cannot provide a valid annotation payload."""


def fetch_epmc_publication_annotations(pubmed_id: int) -> dict:
    """Fetch the raw Europe PMC annotation payload for a publication."""
    try:
        response = httpx.get(
            EMG_CONFIG.europe_pmc.annotations_endpoint,
            params={
                "articleIds": f"MED:{pubmed_id}",
                "provider": EMG_CONFIG.europe_pmc.annotations_provider,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise EuropePmcProviderError(
            f"Unable to fetch Europe PMC annotations for publication {pubmed_id}"
        ) from exc

    if (
        not isinstance(payload, list)
        or not payload
        or not isinstance(payload[0], dict)
        or not isinstance(payload[0].get(ANNOTATIONS), list)
    ):
        raise EuropePmcProviderError(
            f"Europe PMC returned no valid annotations for publication {pubmed_id}"
        )
    return payload[0]


def group_epmc_publication_annotations(payload: dict) -> EuropePmcAnnotationResponse:
    """Group EMERALD-provided Europe PMC annotations by type and text."""
    annotations = sorted(
        payload[ANNOTATIONS], key=lambda annotation: annotation.get(TYPE)
    )
    grouped_annotations: list[EuropePmcAnnotationGroupSchema] = []
    for anno_type, annots_of_type in itertools.groupby(
        annotations, key=lambda annotation: annotation.get("type", "Other")
    ):
        mentions = sorted(
            (EuropePmcAnnotationMentionSchema(**annot) for annot in annots_of_type),
            key=lambda mention: mention.icase_text,
        )
        grouped_annotations.append(
            EuropePmcAnnotationGroupSchema(
                annotations=[
                    EuropePmcAnnotationSchema(
                        annotation_text=text, mentions=group_mentions
                    )
                    for text, group_mentions in itertools.groupby(
                        mentions, key=lambda mention: mention.icase_text
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
    sample_processing_annotations = [
        group
        for group in grouped_annotations
        if group.annotation_type in sample_processing_annotation_types
    ]
    return EuropePmcAnnotationResponse(
        sample_processing=sample_processing_annotations,
        other=[
            group
            for group in grouped_annotations
            if group.annotation_type not in sample_processing_annotation_types
        ],
    )


@transaction.atomic
def record_publication_annotations(
    publication: Publication, payload: dict
) -> EuropePmcPublicationCuration:
    """Persist one complete Europe PMC annotation snapshot transactionally."""
    grouped = group_epmc_publication_annotations(payload)
    curation, _ = EuropePmcPublicationCuration.objects.update_or_create(
        publication=publication,
        defaults={
            "provider": EUROPE_PMC_PROVIDER,
            "source_version": "",
            "configuration": {"articleIds": f"MED:{publication.pubmed_id}"},
            "raw_result": payload,
            "status": EuropePmcPublicationCuration.Status.SUGGESTED,
            "curator": None,
        },
    )
    curation.groups.all().delete()

    for category, groups in (
        ("sample_processing", grouped.sample_processing),
        ("other", grouped.other),
    ):
        for group in groups:
            group_record = EuropePmcAnnotationGroup.objects.create(
                curation=curation,
                annotation_type=group.annotation_type,
                category=category,
            )
            for annotation in group.annotations:
                annotation_record = EuropePmcAnnotation.objects.create(
                    group=group_record,
                    annotation_text=annotation.annotation_text,
                )
                for mention in annotation.mentions:
                    mention_record = EuropePmcAnnotationMention.objects.create(
                        annotation=annotation_record,
                        exact=mention.exact,
                        external_id=mention.id or "",
                        postfix=mention.postfix or "",
                        prefix=mention.prefix or "",
                        provider=mention.provider,
                        annotation_type=mention.type,
                        section=mention.section or "",
                    )
                    EuropePmcAnnotationTag.objects.bulk_create(
                        [
                            EuropePmcAnnotationTag(
                                mention=mention_record,
                                name=tag.name,
                                uri=tag.uri,
                            )
                            for tag in mention.tags
                        ]
                    )
    return curation


def publications_requiring_sync(
    publication_ids: Iterable[int] | None = None,
    stale_after: datetime | None = None,
):
    """Return publications with no snapshot or an older snapshot."""
    queryset = Publication.objects.all()
    if publication_ids is not None:
        queryset = queryset.filter(pubmed_id__in=publication_ids)
    if stale_after is None:
        return queryset.filter(europe_pmc_curations__isnull=True).distinct()
    snapshots = EuropePmcPublicationCuration.objects.filter(
        publication=OuterRef("pk"), updated_at__gte=stale_after
    )
    return queryset.filter(~Exists(snapshots))


def publication_annotations_response(
    curation: EuropePmcPublicationCuration,
) -> EuropePmcAnnotationResponse:
    """Serialize a persisted snapshot using the legacy response shape."""
    categorized = {"sample_processing": [], "other": []}
    for group in curation.groups.all():
        annotations = []
        for annotation in group.annotations.all():
            mentions = [
                {
                    "exact": mention.exact,
                    "id": mention.external_id or None,
                    "postfix": mention.postfix or None,
                    "prefix": mention.prefix or None,
                    "provider": mention.provider,
                    "type": mention.annotation_type,
                    "tags": [
                        {"name": tag.name, "uri": tag.uri} for tag in mention.tags.all()
                    ],
                    "section": mention.section or None,
                }
                for mention in annotation.mentions.all()
            ]
            annotations.append(
                {"annotation_text": annotation.annotation_text, "mentions": mentions}
            )
        descriptor = annotation_type_humanize_map.get(group.annotation_type)
        categorized[group.category].append(
            {
                "annotation_type": group.annotation_type,
                "title": descriptor.title if descriptor else group.annotation_type,
                "description": descriptor.description if descriptor else "",
                "annotations": annotations,
            }
        )

    return EuropePmcAnnotationResponse(
        sample_processing=sorted(
            categorized["sample_processing"],
            key=lambda group: len(group["annotations"]),
            reverse=True,
        ),
        other=sorted(
            categorized["other"],
            key=lambda group: len(group["annotations"]),
            reverse=True,
        ),
    )
