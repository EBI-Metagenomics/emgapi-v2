import json
from typing import Type

from django.core.exceptions import ValidationError as DjValidationError
from django.db import models
from pydantic import BaseModel
from pydantic import ValidationError as PydValidationError


class _PydanticValidatingDict(dict):
    """
    If django field is returning a dict, override item setter so that a field within the dict
    can be updated, with validation.
    """

    def __init__(self, data, schema, strict=False, parent=None):
        self._schema = schema
        self._strict = strict
        self._parent = parent
        super().__init__(data)

    def __setitem__(self, key, value):
        field_data = dict(self)
        field_data[key] = value
        self._schema.model_validate(field_data, strict=self._strict)
        super().__setitem__(key, value)
        if self._parent:
            self._parent._mark_field_as_dirty()

    def update(self, *args, **kwargs):
        field_data = dict(self)
        field_data.update(*args, **kwargs)
        self._schema.model_validate(field_data, strict=self._strict)
        super().update(*args, **kwargs)
        if self._parent:
            self._parent._mark_field_as_dirty()


class _PydanticValidatingList(list):
    """
    If django field is returning a list, override setter and appender to validate updated data.
    """

    def __init__(self, data, schema, strict=False, parent=None):
        self._schema = schema
        self._strict = strict
        self._parent = parent
        super().__init__(data)

    def append(self, value):
        self._schema.model_validate(value, strict=self._strict)
        super().append(value)
        if self._parent:
            self._parent._mark_field_as_dirty()

    def __setitem__(self, index, value):
        self._schema.model_validate(value, strict=self._strict)
        super().__setitem__(index, value)
        if self._parent:
            self._parent._mark_field_as_dirty()


class _PydanticDecoder(json.JSONDecoder):
    def __init__(self, *args, schema=None, is_list: bool = False, **kwargs):
        self.pydantic_model = schema
        self.is_list = is_list
        super().__init__(*args, **kwargs)

    def decode(self, s, **kwargs):
        data = super().decode(s, **kwargs)
        if self.pydantic_model:
            if self.is_list:
                return [self.pydantic_model.model_validate(d) for d in data]
            return self.pydantic_model.model_validate(data)
        return data


class _PydanticEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseModel):
            return obj.model_dump()
        if isinstance(obj, _PydanticValidatingDict):
            return dict(obj)
        if isinstance(obj, _PydanticValidatingList):
            return list(obj)
        return super().default(obj)


class JSONFieldWithSchema(models.JSONField):
    """
    A Django JSONField for a model, except that the JSON content has a validator based on a pydantic schema.

    Usage:

    class MyThing(pydantic.BaseModel):
        name: str = pydantic.Field(..., description="Name of thing")
        length: Optional[int]

    class MyModel(django.db.models.Model):
        id = django.db.models.UUIDField(primary_key=True)
        things = JSONFieldWithSchema(schema=MyThing, is_list=True, strict=True)

    my_object = MyModel.objects.create(things=[{"name": "x-wing", "length": 1}])
    """

    def __init__(
        self,
        schema: Type[BaseModel] = None,
        is_list: bool = False,
        strict: bool = False,
        *args,
        **kwargs,
    ):
        kwargs.pop("decoder", "")
        kwargs.pop("encoder", "")
        self.schema = schema
        self.is_list = is_list
        self.is_strict = strict
        super().__init__(
            *args, decoder=self._make_decoder(), encoder=_PydanticEncoder, **kwargs
        )

    def _make_decoder(self):
        schema = self.schema
        is_list = self.is_list

        class SchemaDecoder(_PydanticDecoder):
            def __init__(inner_self, *args, **kwargs):
                super().__init__(schema=schema, is_list=is_list, *args, **kwargs)

        return SchemaDecoder

    def deconstruct(self):
        """
        Ensure the 'decoder' argument is excluded during migration serialization.
        """
        name, path, args, kwargs = super().deconstruct()

        kwargs.pop("decoder", None)

        kwargs["schema"] = self.schema
        kwargs["is_list"] = self.is_list
        kwargs["strict"] = self.is_strict

        return name, path, args, kwargs

    def validate(self, value, model_instance):
        super().validate(value, model_instance)

        # Use the Pydantic model for additional validation
        try:
            if self.is_list:
                if type(value) is not list:
                    raise DjValidationError("Value is not a list")
                [
                    self.schema.model_validate(item, strict=self.is_strict)
                    for item in value
                ]
            else:
                self.schema.model_validate(value, strict=self.is_strict)
        except PydValidationError as e:
            raise DjValidationError(f"Pydantic validation error: {e}")

        return value

    def to_python(self, value):
        """
        Convert DB values (JSONB) to native python types.
        These are similar to list and dict (the types that a normal jsonfield returns),
        except they handle pydantic validation when updated by dict setters and list appenders.
        """
        value = super().to_python(value)
        if self.schema:
            if self.is_list:
                if isinstance(value, list):
                    return _PydanticValidatingList(
                        value, schema=self.schema, strict=self.is_strict
                    )
            elif isinstance(value, dict):
                return _PydanticValidatingDict(
                    value, schema=self.schema, strict=self.is_strict
                )

        return value

    def value_to_string(self, obj):
        """
        Despite the method name, this is just needed to convert pydantic model instances into native python types.
        Django does the actual stringification itself (or not, in our case, because it is a jsonB field).
        """
        value = self.value_from_object(obj)

        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, _PydanticValidatingDict):
            return dict(value)
        if isinstance(value, _PydanticValidatingList):
            return list(value)

        return value

    def get_prep_value(self, value):
        """
        Similar to stringification, django can't handle non-native types when prepping data for the db/dumpdata/loaddata etc.
        """
        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, _PydanticValidatingDict):
            return dict(value)
        if isinstance(value, _PydanticValidatingList):
            return list(value)
        return super().get_prep_value(value)
