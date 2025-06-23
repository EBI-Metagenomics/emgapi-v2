from typing import List, Optional
from django.http import Http404
from django.shortcuts import get_object_or_404
from ninja.pagination import RouterPaginated

import analyses.models
from analyses.schemas import (
    MGnifyAnalysisDetail,
    MGnifyAnalysisWithAnnotations,
    MGnifyAnalysisTypedAnnotation,
    MGnifyFunctionalAnalysisAnnotationType,
    MGnifyAnalysis,
)
from emgapiv2.api.schema_utils import make_links_section, make_related_detail_link

router = RouterPaginated()


@router.get(
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
)
def get_mgnify_analysis(request, accession: str):
    analysis = get_object_or_404(
        analyses.models.Analysis.public_objects.select_related(
            "run", "assembly", "study", "sample"
        ),
        accession=accession,
    )
    return analysis


@router.get(
    "/{accession}/annotations",
    response=MGnifyAnalysisWithAnnotations,
    summary="Get MGnify analysis by accession, with annotations and downloadable files",
    description="MGnify analyses have annotations (taxonomic and functional assignments), "
    "and downloadable files (outputs from the pipeline execution).",
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


@router.get(
    "/{accession}/annotations/{annotation_type}",
    response=List[MGnifyAnalysisTypedAnnotation],
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


@router.get(
    "/",
    response=List[MGnifyAnalysis],
    summary="List all analyses (MGYAs) available from MGnify",
    description="Each analysis is the result of a Pipeline execution on a reads dataset "
    "(either a raw read-run, or an assembly).",
    operation_id="list_mgnify_analyses",
)
def list_mgnify_analyses(request):
    qs = analyses.models.Analysis.public_objects.select_related(
        "study", "sample", "run", "assembly"
    )
    return qs
