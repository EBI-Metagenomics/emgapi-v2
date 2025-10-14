import logging

from django.conf import settings
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db import models
from django.db.models import Model, QuerySet
from django.db.models.signals import post_save
from django.dispatch import receiver

# Some models that mirror ENA objects, like Study, Sample, Run etc


logger = logging.getLogger(__name__)


class ENAModel(models.Model):

    accession = models.CharField(primary_key=True, max_length=20)
    fetched_at = models.DateTimeField(auto_now=True)
    additional_accessions = models.JSONField(default=list, blank=True)

    class Meta:
        abstract = True

    @property
    def ena_browser_url(self):
        return f"{settings.EMG_CONFIG.ena.browser_view_url_prefix}/{self.accession}"


class StudyManager(models.Manager):
    def get_ena_study(self, ena_study_accession):
        logger.info(f"Will get ENA study for {ena_study_accession} from DB")
        ena_study = False
        try:
            ena_study = (
                self.get_queryset()
                .filter(
                    models.Q(accession=ena_study_accession)
                    | models.Q(additional_accessions__icontains=ena_study_accession)
                )
                .first()
            )
            logger.debug(f"Got {ena_study}")
        except (MultipleObjectsReturned, ObjectDoesNotExist):
            logger.warning(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB"
            )
        return ena_study


class Study(ENAModel):
    title = models.TextField()

    is_private = models.BooleanField(default=False)
    is_suppressed = models.BooleanField(default=False)
    webin_submitter = models.CharField(
        null=True, blank=True, max_length=25, db_index=True
    )

    class Meta:
        verbose_name_plural = "studies"

    objects: StudyManager = StudyManager()

    def __str__(self):
        return self.accession


@receiver(post_save, sender=Study)
def on_ena_study_saved_update_derived_suppression_and_privacy_states(
    sender, instance: Study, created, **kwargs
):
    """
    (Un)suppress the MGnify ("Analyses" app) objects associated with an ENA Study whenever the ENA Study is updated,
    and update their private/public state.
    Typically, an ENA study might be suppressed if it was submitted with erroneous data.
    At present, suppression is handled study-wide. I.e. data are suppressed if and only if an ENA study is suppressed.
    Likewise, data can be private if an entire ENA study is private.
    After embargo date expires, the study and all associated data become public.
    """
    # TODO: suppression can also take place at non-study level...

    for field in instance._meta.get_fields():
        if field.is_relation and field.auto_created and not field.concrete:
            related_model: Model = field.related_model
            fields_of_related = [
                field.name for field in related_model._meta.get_fields()
            ]
            if "ena_study" in fields_of_related:
                for field_to_propagate in [
                    "is_suppressed",
                    "is_private",
                    "webin_submitter",
                ]:
                    if field_to_propagate not in fields_of_related:
                        logger.warning(
                            f"Model {related_model._meta.model_name} looks like it is derived from ENA Study, but doesn't have an {field_to_propagate} field to update."
                        )
                        continue
                    else:
                        # Related_model is probably one that inherits from (or is compatible with) analyses:ENADerivedModel.
                        # We didn't check explicitly because ENADerivedModel is an abstract model,
                        #  we want to avoid circular imports, and because Analysis works slightly differently
                        #  but is caught by this.

                        related_qs: QuerySet = related_model.objects.get_queryset()

                        related_objects_to_update_status_of = related_qs.filter(
                            ena_study=instance
                        ).exclude(
                            **{
                                field_to_propagate: getattr(
                                    instance, field_to_propagate
                                )
                            }  # optimisation so only select derived objects that are not already up to date
                        )
                        if related_objects_to_update_status_of.exists():
                            logger.info(
                                f"Will update {field_to_propagate} state of "
                                f"{related_objects_to_update_status_of.count()} "
                                f"{related_model._meta.app_label}.{related_model._meta.verbose_name_plural} "
                                f"to {field_to_propagate}={getattr(instance, field_to_propagate)} "
                                f"via {instance.accession}."
                            )
                            for related_object in related_objects_to_update_status_of:
                                setattr(
                                    related_object,
                                    field_to_propagate,
                                    getattr(instance, field_to_propagate),
                                )

                            related_qs.bulk_update(
                                related_objects_to_update_status_of,
                                [field_to_propagate],
                            )


class Sample(ENAModel):
    # TODO: refactor this model away. It causes more complexity than benefit.
    metadata = models.JSONField(default=dict)
    study = models.ForeignKey(Study, on_delete=models.CASCADE, related_name="samples")

    def __str__(self):
        return self.accession


@receiver(post_save, sender=Sample)
def on_ena_sample_saved_update_mgnify_sample_metadata(
    sender, instance: Sample, created, **kwargs
):
    if not instance.analyses_samples.exists():
        logger.warning(f"No analyses.Sample found for ena.Sample {instance}")
        return
    if instance.analyses_samples.count() > 1:
        logger.warning(f"More than one analyses.Sample found for ena.Sample {instance}")
        return
    analyses_sample = instance.analyses_samples.first()
    analyses_sample.metadata.update(instance.metadata)
    # Note: does not remove keys from analyses_sample.metadata – so keys like INFERRED_location would be kept.
    analyses_sample.save()
