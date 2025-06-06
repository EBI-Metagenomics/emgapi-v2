from typing import List

from django.shortcuts import get_object_or_404
from ninja.errors import AuthenticationError
from ninja.pagination import RouterPaginated

import analyses.models
from analyses.schemas import (
    MGnifyAnalysis,
    MGnifyStudy,
    MGnifyStudyDetail,
    MGnifyAnalysisDetail,
)
from emgapiv2.api import ApiSections
from emgapiv2.api.auth import DjangoSuperUserAuth, WebinJWTAuth
from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link

router = RouterPaginated(auth=[WebinJWTAuth(), DjangoSuperUserAuth()])


@router.get(
    "/analyses/",
    response=List[MGnifyAnalysis],
    tags=[ApiSections.PRIVATE_DATA.value],
    summary="List all private analyses (MGYAs) available from MGnify",
    operation_id="list_private_mgnify_analyses",
)
def list_private_mgnify_analyses(request):
    auth = request.auth
    qs = analyses.models.Analysis.objects

    if auth and auth.is_superuser:
        qs = qs.all()
    elif auth and (webin_id := auth.username):
        qs = qs.filter(webin_submitter=webin_id)
    else:
        qs = qs.none()

    return qs


@router.get(
    "/studies/",
    response=List[MGnifyStudy],
    tags=[ApiSections.PRIVATE_DATA.value],
    summary="List all private studies available from MGnify",
    operation_id="list_private_mgnify_studies",
)
def list_private_mgnify_studies(request):
    auth = request.auth
    qs = analyses.models.Study.objects

    if auth and auth.is_superuser:
        qs = qs.all()
    elif auth and (webin_id := auth.username):
        qs = qs.filter(webin_submitter=webin_id)
    else:
        qs = qs.none()

    return qs


@router.get(
    "/studies/{accession}",
    response=MGnifyStudyDetail,
    summary="Get the detail of a single private study analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
    operation_id="get_private_mgnify_study",
)
def get_private_mgnify_study(request, accession: str):
    auth = request.auth
    if auth and auth.is_superuser:
        return get_object_or_404(analyses.models.Study.objects, accession=accession)
    elif auth:
        return get_object_or_404(
            analyses.models.Study.objects,
            accession=accession,
            webin_submitter=auth.username,
        )

    raise AuthenticationError


@router.get(
    "/analyses/{accession}",
    response=MGnifyAnalysisDetail,
    summary="Get private MGnify analysis by accession",
    description="MGnify analyses are accessioned with an MYGA-prefixed identifier "
    "and correspond to an individual Run or Assembly analysed by a Pipeline.",
    operation_id="get_private_mgnify_analysis",
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_private_mgnify_study",
            related_object_name="study",
            self_object_name="analysis",
            related_id_in_response="study_accession",
        )
    ),
)
def get_private_mgnify_analysis(request, accession: str):
    auth = request.auth
    if auth and auth.is_superuser:
        return get_object_or_404(
            analyses.models.Analysis.objects.select_related(
                "run", "assembly", "study", "sample"
            ),
            accession=accession,
        )
    elif auth:
        return get_object_or_404(
            analyses.models.Analysis.objects.select_related(
                "run", "assembly", "study", "sample"
            ),
            accession=accession,
            webin_submitter=auth.username,
        )

    raise AuthenticationError
