from __future__ import annotations

import logging
from typing import Any, Protocol

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ObjectDoesNotExist
from django.db import models

import ena.models


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
    def get_by_accession(self, ena_accession):
        qs = self.get_queryset().filter(ena_accessions__contains=[ena_accession])
        if qs.count() > 1:
            raise self.model.MultipleObjectsReturned()
        elif not qs.exists():
            raise self.model.DoesNotExist()
        return qs.first()


class UpdateOrCreateByAccessionManagerMixin:
    def update_or_create_by_accession(
        self,
        known_accessions: list[str],
        defaults: dict[str, Any] | None = None,
        create_defaults: dict[str, Any] | None = None,
        include_update_defaults_in_create_defaults: bool = True,
        **kwargs,
    ) -> tuple[ENADerivedModel, bool]:
        """
        Like django update_or_create, but with handling for multiple accessions.
        I.e., will select the model based on ena_accessions field containing any of the given accessions,
        and will set the ena_accessions to the union of provided known_accessions and any existing accessions.
        :param known_accessions: List of accessions to match on and set as ena_accessions.
        :param defaults: Fields to update.
        :param create_defaults: Fields to set if the object is created.
        :param include_update_defaults_in_create_defaults: If true, the defaults dict will be merged with create_defaults for creation.
        :param kwargs: other matchers
        :return: Tuple of object, created.
        """
        defaults = defaults or {}
        if not create_defaults:
            create_defaults = defaults.copy()
            create_defaults["ena_accessions"] = known_accessions

        if include_update_defaults_in_create_defaults:
            create_defaults.update(defaults)

        # Two-step process needed (first get, then update/create).
        # This is because __overlap cannot be used with default django update_or_create.
        try:
            obj = self.get_queryset().get(
                ena_accessions__overlap=known_accessions, **kwargs
            )
        except ObjectDoesNotExist:
            obj = None

        if obj:
            # Update
            logging.debug(f"Updating {obj} with {defaults}")
            for key, value in defaults.items():
                setattr(obj, key, value)
            obj.save()
            created = False
        else:
            # Create
            logging.debug(f"Creating with {create_defaults}")
            obj = self.model(**create_defaults, **kwargs)
            obj.save()
            created = True

        # Ensure all known_accessions are in the object's ena_accessions
        if not set(obj.ena_accessions).issuperset(known_accessions):
            logging.debug(
                f"Setting accessions to union of known and existing accessions: {obj.ena_accessions}, {known_accessions}"
            )
            obj.ena_accessions = list(set(obj.ena_accessions + known_accessions))
            obj.save()

        return obj, created


class ENADerivedManager(
    SelectRelatedEnaStudyManagerMixin,
    GetByENAAccessionManagerMixin,
    UpdateOrCreateByAccessionManagerMixin,
    models.Manager,
): ...


class ENADerivedModel(VisibilityControlledModel):
    objects = ENADerivedManager()

    ena_accessions = ArrayField(
        models.CharField(max_length=20, blank=True),
        db_index=True,
        blank=True,
        default=list,
    )
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


class SuppressionFilterManagerMixin:
    def get_queryset(self):
        qs = super().get_queryset()
        return qs.filter(is_suppressed=False)


class PrivacyFilterManagerMixin(SuppressionFilterManagerMixin):
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


class HasMetadata(Protocol):
    metadata: dict


class InferredMetadataMixin:
    INFERRED = "INFERRED"

    class _MetadataDictPreferringInferred(dict):
        def get(self, __key):
            for potential_key in [
                f"{InferredMetadataMixin.INFERRED}_{__key}",
                f"{InferredMetadataMixin.INFERRED.lower()}_{__key}",
            ]:
                if potential_key in self:
                    logging.debug(
                        f"Using inferred value of {__key}, because {potential_key} found in {self}"
                    )
                    return super().get(potential_key)
            return super().get(__key)

        def __getitem__(self, item):
            return self.get(item)

    @property
    def metadata_preferring_inferred(self: HasMetadata):
        return InferredMetadataMixin._MetadataDictPreferringInferred(self.metadata)
