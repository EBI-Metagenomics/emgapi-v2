from ninja_extra import api_controller, http_get
from ninja_extra.pagination import paginate
from ninja_extra.schemas import NinjaPaginationResponseSchema

import analyses.models
from analyses.schemas import (
    MGnifyAnalysisDetail,
    MGnifyAnalysisWithAnnotations,
    MGnifyFunctionalAnalysisAnnotationType,
    MGnifyAnalysisTypedAnnotation,
)
from emgapiv2.api import perms
from emgapiv2.api.auth import WebinJWTAuth, NoAuth, DjangoSuperUserAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController
from emgapiv2.api.schema_utils import (
    make_links_section,
    make_related_detail_link,
    ApiSections,
)


@api_controller("analyses", tags=[ApiSections.ANALYSES])
class AnalysisController(UnauthorisedIsUnfoundController):
    @http_get(
        "/{accession}",
        response=MGnifyAnalysisDetail,
        summary="Get MGnify analysis by accession",
        description="MGnify analyses are accessioned with an MYGA-prefixed identifier "
        "and correspond to an individual Run or Assembly analysed by a Pipeline.",
        operation_id="get_mgnify_analysis",
        openapi_extra=make_links_section(
            make_related_detail_link(
                related_detail_operation_id="get_mgnify_study",
                related_object_name="study",
                self_object_name="analysis",
                related_id_in_response="study_accession",
            )
        ),
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    def get_mgnify_analysis(self, accession: str):
        return self.get_object_or_exception(
            analyses.models.Analysis.objects.select_related(
                "run", "assembly", "study", "sample"
            ),
            accession=accession,
        )

    @http_get(
        "/",
        response=NinjaPaginationResponseSchema[MGnifyAnalysisDetail],
        summary="List all analyses (MGYAs) available from MGnify",
        description="Each analysis is the result of a Pipeline execution on a reads dataset "
        "(either a raw read-run, or an assembly).",
        operation_id="list_mgnify_analyses",
    )
    @paginate
    def list_mgnify_analyses(self):
        qs = analyses.models.Analysis.public_objects.select_related(
            "study", "sample", "run", "assembly"
        )
        return qs

    @http_get(
        "/{accession}/annotations",
        response=MGnifyAnalysisWithAnnotations,
        summary="Get MGnify analysis by accession, with annotations and downloadable files",
        description="MGnify analyses have annotations (taxonomic and functional assignments), "
        "and downloadable files (outputs from the pipeline execution).",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    def get_mgnify_analysis_with_annotations(self, accession: str):
        return self.get_object_or_exception(
            analyses.models.Analysis.objects_and_annotations.select_related(
                "study", "sample", "run", "assembly"
            ),
            accession=accession,
        )

    @http_get(
        "/{accession}/annotations/{annotation_type}",
        response=NinjaPaginationResponseSchema[MGnifyAnalysisTypedAnnotation],
        summary="Get a named set of annotations for a MGnify analysis by accession.",
        description="List the annotations of a given type for a MGnify analysis referred to by its accession.",
        auth=[WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()],
        permissions=[
            perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
        ],
    )
    @paginate()
    def get_mgnify_analysis_with_annotations_of_type(
        self,
        accession: str,
        annotation_type: MGnifyFunctionalAnalysisAnnotationType,
    ):
        # TODO: this involves a duplicate DB query, can probably be a single query that is then perm checked
        self.get_object_or_exception(
            analyses.models.Analysis.objects, accession=accession
        )
        annotations = (
            analyses.models.Analysis.objects.filter(accession=accession)
            .values_list(f"annotations__{annotation_type.value}", flat=True)
            .first()
        )
        return annotations or []  # None -> []
