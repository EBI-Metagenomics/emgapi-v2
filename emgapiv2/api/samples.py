from typing import List

from django.http import Http404
from ninja.pagination import RouterPaginated

import analyses.models
from analyses.models import Sample
from analyses.schemas import MGnifySample, MGnifySampleDetail
from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link

router = RouterPaginated()


@router.get(
    "/{accession}",
    response=MGnifySampleDetail,
    summary="Get the detail of a single sample analysed by MGnify",
    description="MGnify samples inherit directly from samples (or BioSamples) in ENA.",
    operation_id="get_mgnify_sample",
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_mgnify_study",
            related_object_name="study",
            self_object_name="sample",
            related_id_in_response="accession",
            from_list_to_detail=True,
            from_list_at_path="studies/",
        )
    ),
)
def get_mgnify_sample(request, accession: str):
    try:
        sample = analyses.models.Sample.public_objects.get_by_accession(accession)
    except (Sample.DoesNotExist, Sample.MultipleObjectsReturned):
        raise Http404(f"No sample available for accession {accession}")
    return sample


@router.get(
    "/",
    response=List[MGnifySample],
    summary="List all samples analysed by MGnify",
    description="MGnify studies inherit directly from samples (or BioSamples) in ENA.",
    operation_id="list_mgnify_samples",
)
def list_mgnify_samples(request):
    qs = analyses.models.Sample.public_objects.all().prefetch_related("studies")
    return qs
