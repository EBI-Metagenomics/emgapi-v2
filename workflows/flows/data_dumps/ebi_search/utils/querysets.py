from datetime import datetime

from django.db.models import Prefetch, Q, QuerySet

from analyses.models import Analysis, Study
from emgapiv2.model_utils import during


def changed_studies(since: datetime, until: datetime) -> QuerySet[Study]:
    # Reindex studies when they or their analyses change
    return Study.objects.filter(
        during(since, until) | during(since, until, relationship_path="analyses")
    ).distinct()


def changed_analyses(since: datetime, until: datetime) -> QuerySet[Analysis]:
    # Reindex analyses when they or any ENA object they relate to change
    return Analysis.objects.filter(
        during(since, until)
        | during(since, until, relationship_path="study")
        | during(since, until, relationship_path="sample")
        | during(since, until, relationship_path="run")
        | during(since, until, relationship_path="assembly")
    ).distinct()


def analyses_to_delete(since: datetime, until: datetime) -> QuerySet[Analysis]:
    related_change = (
        during(since, until, relationship_path="study")
        | during(since, until, relationship_path="sample")
        | during(since, until, relationship_path="run")
        | during(since, until, relationship_path="assembly")
    )
    return Analysis.objects.filter(
        during(since, until) | (Q(is_ready=True) & related_change)
    ).distinct()


def _public_study_filter() -> Q:
    return Q(is_private=False, is_suppressed=False)


def _public_analysis_filter() -> Q:
    return Q(
        is_private=False,
        is_suppressed=False,
        is_ready=True,
        study__is_private=False,
        study__is_suppressed=False,
    )


def study_additions(
    initial: bool, since: datetime | None, until: datetime
) -> QuerySet[Study]:
    studies = (
        Study.objects.filter(_public_study_filter())
        if initial
        else changed_studies(since, until).filter(_public_study_filter())
    )
    public_analyses = Analysis.objects.filter(_public_analysis_filter()).only(
        "accession", "experiment_type", "pipeline_version", "study_id"
    )
    return (
        studies.select_related("biome")
        .prefetch_related(
            Prefetch(
                "analyses", queryset=public_analyses, to_attr="ebi_search_analyses"
            )
        )
        .order_by("accession")
    )


def analysis_additions(
    initial: bool, since: datetime | None, until: datetime
) -> QuerySet[Analysis]:
    analyses = (
        Analysis.objects.filter(_public_analysis_filter())
        if initial
        else changed_analyses(since, until).filter(_public_analysis_filter())
    )
    return (
        analyses.defer(None)
        .select_related("study", "study__biome", "sample", "run", "assembly")
        .order_by("accession", "pipeline_version")
    )
