import logging
from textwrap import dedent
from typing import List, Optional
from urllib.parse import urljoin

from django.http import Http404
from django.shortcuts import get_object_or_404
from ninja import NinjaAPI
from ninja.pagination import RouterPaginated
from ninja.security import django_auth_superuser

import analyses.models
from analyses.schemas import (
    MGnifyAnalysis,
    MGnifyAnalysisDetail,
    MGnifyAnalysisTypedAnnotation,
    MGnifyAnalysisWithAnnotations,
    MGnifyAssemblyAnalysisRequest,
    MGnifyAssemblyAnalysisRequestCreate,
    MGnifyFunctionalAnalysisAnnotationType,
    MGnifyStudy,
    EMG_CONFIG,
    MGnifyStudyDetail,
)
from emgapiv2.schema_utils import (
    ApiSections,
    OpenApiKeywords,
    make_links_section,
    make_related_detail_link,
)

logger = logging.getLogger(__name__)

api = NinjaAPI(
    title="MGnify API",
    description="The API for [MGnify](https://www.ebi.ac.uk/metagenomics), "
    "EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.",
    urls_namespace="api",
    csrf=True,
    version="2.0-alpha",
    default_router=RouterPaginated(),
    openapi_extra={
        "tags": [
            {
                OpenApiKeywords.NAME.value: ApiSections.STUDIES.value,
                OpenApiKeywords.DESCRIPTION.value: dedent(
                    """
                MGnify studies are based on ENA studies/projects, and are collections of samples, runs, assemblies,
                and analyses associated with a certain set of experiments.
                """
                ),
            },
            {
                OpenApiKeywords.NAME.value: ApiSections.SAMPLES.value,
                OpenApiKeywords.DESCRIPTION.value: dedent(
                    """
                MGnify samples are based on ENA/BioSamples samples, and represent individual biological samples.
                """
                ),
            },
            {
                OpenApiKeywords.NAME.value: ApiSections.ANALYSES.value,
                OpenApiKeywords.DESCRIPTION.value: dedent(
                    """
                MGnify analyses are runs of a standard pipeline on an individual sequencing run or assembly.
                They can include collections of taxonomic and functional annotations.
                """
                ),
            },
            {
                OpenApiKeywords.NAME.value: ApiSections.REQUESTS.value,
                OpenApiKeywords.DESCRIPTION.value: dedent(
                    """
                Requests are user-initiated processes for MGnify to assemble and/or analyse the samples in a study.
                """
                ),
            },
        ]
    },
)


#################################################################
#                                                               #
#                           STUDIES                             #
#                                                               #
#################################################################


@api.get(
    "/studies/{accession}",
    response=MGnifyStudyDetail,
    tags=[ApiSections.STUDIES.value],
    summary="Get the detail of a single study analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
    operation_id="get_mgnify_study",
    by_alias=True,
)
def get_mgnify_study(request, accession: str):
    study = get_object_or_404(analyses.models.Study.public_objects, accession=accession)
    return study


@api.get(
    "/studies",
    response=List[MGnifyStudy],
    tags=[ApiSections.STUDIES.value],
    summary="List all studies analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
    operation_id="list_mgnify_studies",
)
def list_mgnify_studies(request):
    qs = analyses.models.Study.public_objects.all()
    return qs


#################################################################
#                                                               #
#                           ANALYSES                            #
#                                                               #
#################################################################


@api.get(
    "/analyses/{accession}",
    response=MGnifyAnalysisDetail,
    summary="Get MGnify analysis by accession",
    description="MGnify analyses are accessioned with an MYGA-prefixed identifier "
    "and correspond to an individual Run or Assembly analysed by a Pipeline.",
    tags=[ApiSections.ANALYSES.value],
    openapi_extra=make_links_section(
        make_related_detail_link(
            related_detail_operation_id="get_mgnify_study",
            related_object_name="study",
            self_object_name="analysis",
            related_id_in_response="study_accession",
        )
    ),
)
def get_mgnify_analysis(request, accession: str):
    analysis = get_object_or_404(
        analyses.models.Analysis.public_objects.select_related("run"),
        accession=accession,
    )

    run_accession = analysis.run.first_accession if analysis.run else None
    study_accession = analysis.study.accession if analysis.study else None
    sample_accession = analysis.sample.ena_sample.accession if analysis.sample else None
    assembly_accession = (
        analysis.assembly.first_accession if analysis.assembly else None
    )
    experiment_type = analysis.run.experiment_type if analysis.run else None
    raw_run = analysis.raw_run

    response = {
        "accession": analysis.accession,
        "run_accession": run_accession,
        "downloads_as_objects": analysis.downloads_as_objects,
        "study_accession": study_accession,
        "sample_accession": sample_accession,
        "assembly_accession": assembly_accession,
        "experiment_type": experiment_type,
        "raw_run": raw_run,
        "pipeline_version": analysis.pipeline_version,
        "quality_control": analysis.quality_control,
        "results_dir": urljoin(
            EMG_CONFIG.service_urls.transfer_services_url_root, analysis.results_dir
        ),
        "metadata": analysis.metadata,
    }

    return response


@api.get(
    "/analyses/{accession}/annotations",
    response=MGnifyAnalysisWithAnnotations,
    summary="Get MGnify analysis by accession, with annotations and downloadable files",
    description="MGnify analyses have annotations (taxonomic and functional assignments), "
    "and downloadable files (outputs from the pipeline execution).",
    tags=[ApiSections.ANALYSES.value],
)
def get_mgnify_analysis_with_annotations(request, accession: str):
    analysis = get_object_or_404(
        analyses.models.Analysis.public_objects_and_annotations, accession=accession
    )
    run_accession = analysis.run.first_accession if analysis.run else None
    study_accession = analysis.study.accession if analysis.study else None
    sample_accession = analysis.sample.ena_sample.accession if analysis.sample else None
    assembly_accession = (
        analysis.assembly.first_accession if analysis.assembly else None
    )
    experiment_type = analysis.run.experiment_type if analysis.run else None
    raw_run = analysis.raw_run

    return {
        "accession": analysis.accession,
        "study_accession": study_accession,
        "run_accession": run_accession,
        "sample_accession": sample_accession,
        "experiment_type": experiment_type,
        "pipeline_version": analysis.pipeline_version,
        "downloads_as_objects": analysis.downloads_as_objects,
        "raw_run": raw_run,
        "quality_control": analysis.quality_control,
        "assembly_accession": assembly_accession,
        "annotations": analysis.annotations,
    }


@api.get(
    "/analyses/{accession}/annotations/{annotation_type}",
    response=List[MGnifyAnalysisTypedAnnotation],
    tags=[ApiSections.ANALYSES.value],
)
def get_mgnify_analysis_with_annotations_of_type(
    request,
    accession: str,
    annotation_type: MGnifyFunctionalAnalysisAnnotationType,
    limit: Optional[int] = None,
):
    try:
        annotations = (
            analyses.models.Analysis.public_objects.filter(accession=accession)
            .values_list(f"annotations__{annotation_type.value}", flat=True)
            .first()
        )
    except analyses.models.Analysis.DoesNotExist:
        raise Http404("No analysis found")

    if limit and annotations:
        return annotations[:limit]
    return annotations or []  # None -> []


@api.get(
    "/analyses",
    response=List[MGnifyAnalysis],
    tags=[ApiSections.ANALYSES.value],
    summary="List all analyses (MGYAs) available from MGnify",
    description="Each analysis is the result of a Pipeline execution on a reads dataset "
    "(either a raw read-run, or an assembly).",
    operation_id="list_mgnify_analyses",
)
def list_mgnify_analyses(request):
    qs = analyses.models.Analysis.public_objects.all()
    return qs


#################################################################
#                                                               #
#                          My Data                             #
#                                                               #
#################################################################


@api.get(
    "/my-data/analyses",
    response=List[MGnifyAnalysis],
    tags=[ApiSections.ANALYSES.value],
    summary="List all private analyses (MGYAs) available from MGnify",
    operation_id="list_private_mgnify_analyses",
    auth=django_auth_superuser,
)
def list_private_mgnify_analyses():
    qs = analyses.models.Analysis.public_objects()
    return qs


@api.get(
    "/my-data/studies",
    response=List[MGnifyStudy],
    tags=[ApiSections.STUDIES.value],
    summary="List all private studies available from MGnify",
    operation_id="list_private_mgnify_studies",
    auth=django_auth_superuser,
)
def list_private_mgnify_studies():
    qs = analyses.models.Study.objects()
    return qs


#################################################################
#                                                               #
#                          REQUESTS                             #
#                                                               #
#################################################################


@api.get(
    "/analysis_requests",
    response=List[MGnifyAssemblyAnalysisRequest],
    tags=[ApiSections.REQUESTS.value],
)
def list_assembly_analysis_requests(request):
    qs = analyses.models.AssemblyAnalysisRequest.objects.all()
    # TODO: perms
    return qs


@api.get(
    "/analysis_requests/{analysis_request_id}",
    response=MGnifyAssemblyAnalysisRequest,
    tags=[ApiSections.REQUESTS.value],
)
def get_assembly_analysis_requests(request, analysis_request_id: int):
    return analyses.models.AssemblyAnalysisRequest.objects.get(id=analysis_request_id)
    # TODO: perms


@api.post(
    "/analysis_requests",
    response=MGnifyAssemblyAnalysisRequest,
    tags=[ApiSections.REQUESTS.value],
)
def create_assembly_analysis_request(
    request, payload: MGnifyAssemblyAnalysisRequestCreate
):
    assembly_analysis_request = analyses.models.AssemblyAnalysisRequest.objects.create(
        **payload.dict()
    )
    return assembly_analysis_request


# @api.post("/analyse")
# def create_study_analysis_intent(request, payload: StudyAnalysisIntent):
#     run_deployment(
#         "Fetch Study and Samples from ENA/ena_fetch_study_deployment",
#         timeout=0,
#         parameters={"accession": payload.study_accession},
#     )
#     return
