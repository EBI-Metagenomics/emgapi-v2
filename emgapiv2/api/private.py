from typing import List

from ninja.pagination import RouterPaginated

import analyses.models
from analyses.schemas import MGnifyAnalysis, MGnifyStudy
from emgapiv2.api import ApiSections
from emgapiv2.api.auth import DjangoSuperUserAuth, WebinJWTAuth

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
