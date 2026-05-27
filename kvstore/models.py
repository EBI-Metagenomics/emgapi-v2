from typing import TypeVar

from django.db import models
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class KeyValueStore(models.Model):
    key = models.CharField(max_length=255, primary_key=True)
    value = models.JSONField()
    updated_at = models.DateTimeField(auto_now=True)

    @classmethod
    def set_model(cls, key: str, value: BaseModel):
        cls.objects.update_or_create(
            key=key,
            defaults={"value": value.model_dump(mode="json")},
        )

    @classmethod
    def get_model(cls, key: str, model: type[T]) -> T:
        obj = cls.objects.get(key=key)
        return model.model_validate(obj.value)

    class Meta:
        verbose_name = "Key-Value Store Entry"
        verbose_name_plural = "Key-Value Store Entries"
