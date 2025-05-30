from typing import List

from ninja.pagination import RouterPaginated
from ninja.security import django_auth_superuser

import analyses.models
from analyses.schemas import MGnifyAnalysis, MGnifyStudy
from emgapiv2.api import ApiSections

router = RouterPaginated()


@router.get(
    "/analyses/",
    response=List[MGnifyAnalysis],
    tags=[ApiSections.ANALYSES.value],
    summary="List all private analyses (MGYAs) available from MGnify",
    operation_id="list_private_mgnify_analyses",
    auth=django_auth_superuser,
)
def list_private_mgnify_analyses():
    qs = analyses.models.Analysis.public_objects()
    return qs


@router.get(
    "/studies/",
    response=List[MGnifyStudy],
    summary="List all private studies available from MGnify",
    operation_id="list_private_mgnify_studies",
    auth=django_auth_superuser,
)
def list_private_mgnify_studies():
    qs = analyses.models.Study.objects()
    return qs
