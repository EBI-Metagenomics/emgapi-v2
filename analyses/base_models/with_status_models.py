# TODO: refactor the "state" field to a base model here, including helpers like a default state classmethod
# and a mark-status method
from enum import Enum
from typing import List, Union

from django.db.models import Q, QuerySet


class SelectByStatusQueryset(QuerySet):
    def __init__(
        self,
        model=None,
        query=None,
        using=None,
        hints=None,
        status_fieldname: str = "status",
    ):
        self.status_fieldname = status_fieldname
        super().__init__(model, query, using, hints)

    def _build_q_objects(
        self, keys: List[Union[str, Enum]], allow_null: bool, truthy_target: bool
    ):
        filters = []
        for status in keys:
            status_label = status.value if isinstance(status, Enum) else status
            if allow_null:
                filters.append(
                    Q(**{f"{self.status_fieldname}__{status_label}": truthy_target})
                )
            else:
                filters.append(
                    Q(**{f"{self.status_fieldname}__{status_label}": truthy_target})
                    | Q(**{f"{self.status_fieldname}__{status_label}__isnull": True})
                )
        return filters

    def filter_by_statuses(
        self, statuses_to_be_true: List[Union[str, Enum]] = None, strict: bool = True
    ):
        """
        Filter queryset by a combination of statuses in the object's status json field.
        :param statuses_to_be_true: List of keys that should resolve to true values in the model's status_fieldname json field.
        :param strict: If True, objects will be filtered out if the key is not present. If False, null values are also acceptable i.e. only falsey values will be excluded.
        """
        if not statuses_to_be_true:
            return self

        filters = self._build_q_objects(statuses_to_be_true, strict, True)
        return self.filter(*filters)

    def exclude_by_statuses(
        self, statuses_to_exclude: List[Union[str, Enum]] = None, strict: bool = True
    ):
        """
        Filter queryset by excluding a combination of statuses in the object's status json field.
        :param statuses_to_exclude: List of keys that, if they resolve to false values in the model's status_fieldname json field, will exclude that object from the queryset.
        :param strict: If True, objects will only be excluded if the key is present AND truthy. If False, null values are also excluded.
        """
        if not statuses_to_exclude:
            return self

        filters = self._build_q_objects(statuses_to_exclude, not strict, False)
        return self.filter(*filters)


class SelectByStatusManagerMixin:
    """
    A mixin for model managers that provides queryset filtering by the truthiness of keys in a model object's json field.
    Helpful for filtering e.g. analyses.objects.filter(status__is_it_started=True, status__is_it_finished=False).

    Use:
    class MyModelManager(SelectByStatusManagerMixin, models.Manager):...
        STATUS_FIELDNAME = "my_status_field"

    class MyModel(...):
        objects = MyModelManager()

    MyModel.objects.filter_by_statuses(["is_it_started"]).exclude_by_statuses(["is_it_finished"]).
    """

    STATUS_FIELDNAME = "status"

    def get_queryset(self):
        return SelectByStatusQueryset(
            status_fieldname=self.STATUS_FIELDNAME, using=self._db, model=self.model
        )

    def exclude_by_statuses(self, *args, **kwargs):
        return self.get_queryset().exclude_by_statuses(*args, **kwargs)

    def filter_by_statuses(self, *args, **kwargs):
        return self.get_queryset().filter_by_statuses(*args, **kwargs)
