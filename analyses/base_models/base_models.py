from __future__ import annotations

from django.conf import settings
from django.db import models

import ena.models


class MGnifyAutomatedModel(models.Model):
    """
    Base class for models that have an autoincrementing ID (perhaps for use as an accession)
    and an `is_ready` bool for when they should appear on website vs. in automation only.
    """

    id = models.AutoField(primary_key=True)
    is_ready = models.BooleanField(default=False)

    class Meta:
        abstract = True

    # TODO: suppression propagation


class SelectRelatedEnaStudyManagerMixin:
    def get_queryset(self):
        return super().get_queryset().select_related("ena_study")


class VisibilityControlledManager(SelectRelatedEnaStudyManagerMixin, models.Manager):
    pass


class VisibilityControlledModel(models.Model):
    """
    Base class for models that should inherit their privacy status (so visibility) from an ENA Study.
    """

    objects = VisibilityControlledManager()

    is_private = models.BooleanField(default=False)
    ena_study = models.ForeignKey(ena.models.Study, on_delete=models.CASCADE)
    webin_submitter = models.CharField(
        null=True, blank=True, max_length=25, db_index=True
    )

    class Meta:
        abstract = True


class GetByENAAccessionManagerMixin:
    async def get_by_accession(self, ena_accession):
        qs = self.get_queryset().filter(ena_accessions__contains=ena_accession)
        if qs.count() > 1:
            raise self.MultipleObjectsReturned()
        elif not qs.exists():
            raise self.ObjectDoesNotExist()
        return qs.first()


class ENADerivedManager(
    SelectRelatedEnaStudyManagerMixin, GetByENAAccessionManagerMixin, models.Manager
):
    pass


class ENADerivedModel(VisibilityControlledModel):
    objects = ENADerivedManager()

    ena_accessions = models.JSONField(default=list, db_index=True, blank=True)
    is_suppressed = models.BooleanField(default=False)

    # TODO – postgres GIN index on accessions?

    @property
    def first_accession(self):
        if len(self.ena_accessions):
            return self.ena_accessions[0]
        return None

    @property
    def ena_browser_url(self):
        return (
            f"{settings.EMG_CONFIG.ena.browser_view_url_prefix}/{self.first_accession}"
        )

    def inherit_accessions_from_related_ena_object(self, related_field_name: str):
        """
        Copy (inherit) accessions from a related ENA object, e.g. ena_study or ena_sample.
        :param: related_field_name: name of the field on this model which defined the relationship

        Example: mgnify_study.inherit_accessions_from_related_ena_object('ena_study')
        Example: mgnify_sample.inherit_accessions_from_related_ena_object('ena_sample')
        """
        all_accessions = self.ena_accessions or []
        related_object = getattr(self, related_field_name)
        if related_object:
            related_primary_accession = getattr(related_object, "accession")
            if related_primary_accession:
                all_accessions.append(related_primary_accession)
            related_additional_accessions = getattr(
                related_object, "additional_accessions"
            )
            if related_additional_accessions:
                try:
                    for accession in list(related_additional_accessions):
                        all_accessions.append(accession)
                except ValueError:
                    pass
        self.ena_accessions = list(set(all_accessions))
        self.save()

    class Meta:
        abstract = True


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class PrivacyFilterManagerMixin:
    """
    Base mixin providing common privacy filtering methods for studies
    """

    def get_queryset(self, include_private=False, private_only=False):
        qs = super().get_queryset()
        if private_only:
            return qs.filter(is_private=True)
        if not include_private:
            return qs.filter(is_private=False)
        return qs

    def private_only(self):
        """
        Returns only private studies
        """
        return self.get_queryset(private_only=True)
