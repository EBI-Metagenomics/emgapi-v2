"""HTTP API schemas and endpoints for biome predictions."""

from datetime import datetime
from typing import Literal

from django.http import HttpRequest
from ninja import Query, Schema
from ninja_extra import api_controller, http_get, paginate
from ninja_extra.exceptions import NotFound
from ninja_extra.schemas import NinjaPaginationResponseSchema
from pydantic import Field

from analyses.models import Sample, Study
from emgapiv2.api import perms
from emgapiv2.api.auth import DjangoSuperUserAuth, NoAuth, WebinJWTAuth
from emgapiv2.api.perms import UnauthorisedIsUnfoundController

from .models import BiomeSampleBiomePrediction, BiomeStudyBiomePrediction


class PredictionSchema(Schema):
    """Serialized representation of a study or sample prediction."""

    study_accession: str | None = None
    sample_accession: str | None = None
    predicted_biome: str | None = None
    raw_predicted_lineage: str
    confidence: float | None = None
    status: str
    mapped: bool
    method: str
    source: str
    source_version: str
    configuration: dict
    predicted_at: datetime
    analysis_accessions: list[str] = Field(default_factory=list)

    @staticmethod
    def resolve_study_accession(obj):
        """Resolve the owning study accession for direct or inherited records."""
        if hasattr(obj, "study"):
            return obj.study.accession
        studies = list(obj.sample.studies.all())
        return studies[0].accession if studies else None

    @staticmethod
    def resolve_sample_accession(obj):
        """Resolve the sample accession when the record is sample-scoped."""
        inherited_sample = getattr(obj, "inherited_sample", None)
        if inherited_sample is not None:
            return inherited_sample.first_accession
        return obj.sample.first_accession if hasattr(obj, "sample") else None

    @staticmethod
    def resolve_predicted_biome(obj):
        """Return the mapped biome lineage, when available."""
        return obj.predicted_biome.pretty_lineage if obj.predicted_biome else None

    @staticmethod
    def resolve_mapped(obj):
        """Return whether the prediction lineage mapped to a biome."""
        return obj.is_mapped

    @staticmethod
    def resolve_analysis_accessions(obj):
        """Return accessions of analyses used as prediction evidence."""
        return list(obj.evidence.values_list("accession", flat=True))


class PredictionFilters(Schema):
    """Filters supported by the prediction listing endpoint."""

    status: Literal["suggested", "approved", "rejected"] | None = None
    mapped: bool | None = None
    study_accession: str | None = None
    sample_accession: str | None = None


class InheritedPrediction:
    """Expose a study prediction as the effective prediction for a sample."""

    def __init__(self, prediction, sample):
        """Wrap a prediction while retaining the sample receiving the fallback."""
        self.prediction = prediction
        self.inherited_sample = sample

    def __getattr__(self, name):
        """Delegate prediction fields to the wrapped record."""
        return getattr(self.prediction, name)


@api_controller("biome-predictions", tags=["Biome predictions"])
class PredictionController(UnauthorisedIsUnfoundController):
    """Serve visibility-filtered study and sample biome predictions."""

    auth = [WebinJWTAuth(), DjangoSuperUserAuth(), NoAuth()]
    permissions = [
        perms.IsPublic | perms.IsWebinOwner | perms.IsAdminUserWithObjectPerms
    ]

    @http_get(
        "/studies/{accession}",
        response=PredictionSchema,
        auth=auth,
        permissions=permissions,
    )
    def study_prediction(self, accession: str):
        """Return the prediction for a study accession."""
        prediction = self.get_object_or_exception(
            BiomeStudyBiomePrediction.objects.select_related("study", "predicted_biome")
            .prefetch_related("evidence")
            .filter(study__in=Study.objects_not_suppressed.all()),
            study__accession=accession,
        )
        self.check_object_permissions(prediction.study)
        return prediction

    @http_get(
        "/samples/{accession}",
        response=PredictionSchema,
        auth=auth,
        permissions=permissions,
    )
    def sample_prediction(self, accession: str):
        """Return a direct sample prediction or its study-level fallback."""
        try:
            sample = Sample.objects_not_suppressed.get_by_accession(accession)
        except (Sample.DoesNotExist, Sample.MultipleObjectsReturned) as exc:
            raise NotFound(f"No sample found with accession {accession}") from exc

        prediction = BiomeSampleBiomePrediction.objects.effective_for_sample(sample)
        if isinstance(prediction, BiomeSampleBiomePrediction):
            self.check_object_permissions(prediction.sample)
            return prediction
        if prediction is None:
            raise NotFound(f"No biome prediction available for sample {accession}")
        self.check_object_permissions(sample)
        self.check_object_permissions(prediction.study)
        return InheritedPrediction(prediction, sample)

    @http_get("/", response=NinjaPaginationResponseSchema[PredictionSchema])
    @paginate()
    def list_predictions(
        self, request: HttpRequest, filters: PredictionFilters = Query(...)
    ):
        """List visible predictions after applying the requested filters."""
        study_qs = (
            BiomeStudyBiomePrediction.objects.select_related("study", "predicted_biome")
            .prefetch_related("evidence")
            .filter(study__in=Study.public_objects.all())
        )
        sample_qs = (
            BiomeSampleBiomePrediction.objects.select_related(
                "sample", "predicted_biome"
            )
            .prefetch_related("evidence", "sample__studies")
            .filter(sample__in=Sample.public_objects.all())
        )
        if filters.status is not None:
            study_qs = study_qs.filter(status=filters.status)
            sample_qs = sample_qs.filter(status=filters.status)
        if filters.mapped is not None:
            study_qs = study_qs.filter(predicted_biome__isnull=not filters.mapped)
            sample_qs = sample_qs.filter(predicted_biome__isnull=not filters.mapped)
        if filters.study_accession is not None:
            study_qs = study_qs.filter(study__accession=filters.study_accession)
            sample_qs = sample_qs.filter(
                sample__studies__accession=filters.study_accession
            )
        if filters.sample_accession is not None:
            sample_qs = sample_qs.filter(
                sample__ena_accessions__contains=[filters.sample_accession]
            )
            study_qs = study_qs.none()

        qs = list(study_qs) + list(sample_qs)
        visible = []
        for obj in qs:
            target = obj.study if hasattr(obj, "study") else obj.sample
            try:
                self.check_object_permissions(target)
            except NotFound:
                continue
            visible.append(obj)
        return visible
