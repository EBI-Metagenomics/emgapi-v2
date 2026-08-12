from datetime import UTC, datetime

import pytest
from django.core.exceptions import ValidationError
from django.db import connection, models
from django.test.utils import isolate_apps
from pydantic import BaseModel

from analyses.models import Publication
from emgapiv2.async_utils import anysync_property
from emgapiv2.dict_utils import add, some
from emgapiv2.enum_utils import FutureStrEnum
from emgapiv2.log_utils import mask_sensitive_data
from emgapiv2.model_utils import (
    during,
    JSONFieldWithSchema,
    SuppressionFollowingForeignKey,
    SuppressionFollowingRelation,
)


# Tests for async utils
class MyThing:
    def __init__(self):
        self.hello_to = "world"

    @property
    def message(self):
        return f"Hello {self.hello_to}"

    @anysync_property
    def any_message(self):
        return f"Hello {self.hello_to}"


def test_async_utils_anysync_property_works_in_sync_context():
    m = MyThing()
    assert m.message == "Hello world"
    assert m.any_message == "Hello world"


@pytest.mark.asyncio
async def test_async_utils_anysync_property_works_in_async_context():
    m = MyThing()
    assert m.message == "Hello world"
    assert await m.any_message == "Hello world"


def test_log_masking():
    script = "./run-command subcommand -flag=okay -password=verysecret"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password=*****"
    )

    script = "./run-command subcommand -flag=okay -password='verysecret'"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password='*****'"
    )

    script = './run-command subcommand -flag=okay -password="verysecret"'
    assert (
        mask_sensitive_data(script)
        == './run-command subcommand -flag=okay -password="*****"'
    )

    script = """
    ./run-command subcommand1 -flag=okay -password=verysecret"
    ./run-command subcommand2 -flag=okay -password=alsoverysecret"
    """
    assert mask_sensitive_data(script) == """
    ./run-command subcommand1 -flag=okay -password=*****
    ./run-command subcommand2 -flag=okay -password=*****
    """

    script = "./run-command subcommand -flag=okay -password verysecret"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password *****"
    )

    script = "./run-command subcommand -flag=okay -password 'verysecret'"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password '*****'"
    )


@pytest.mark.django_db
def test_json_field_with_schema():
    class TestSchema(BaseModel):
        name: str
        length: int

    class TestModel(models.Model):
        my_data = JSONFieldWithSchema(schema=TestSchema)

        class Meta:
            app_label = "test"

    valid_data = {"name": "X-wing", "length": 13}

    # should validate
    instance = TestModel(my_data=valid_data)
    instance.full_clean()

    assert TestSchema.model_validate(instance.my_data).name == "X-wing"
    assert TestSchema.model_validate(instance.my_data).length == 13

    # should support partial update
    instance.my_data["name"] = "Y-wing"
    instance.full_clean()
    assert TestSchema.model_validate(instance.my_data).name == "Y-wing"
    assert TestSchema.model_validate(instance.my_data).length == 13

    # Create invalid data that violates the Pydantic schema
    invalid_data = {"name": "X-wing", "length": "thirteen"}

    # Test saving invalid data
    instance = TestModel(my_data=invalid_data)
    with pytest.raises(ValidationError) as exc_info:
        instance.full_clean()

    # Check the error message
    assert "Pydantic validation error" in str(exc_info.value)

    # As list:
    class TestModel2(models.Model):
        my_data = JSONFieldWithSchema(schema=TestSchema, is_list=True)

        class Meta:
            app_label = "test"

    single_datum = valid_data
    instance = TestModel2(my_data=single_datum)
    with pytest.raises(ValidationError):
        instance.full_clean()

    instance = TestModel2(my_data=[single_datum])
    assert TestSchema.model_validate(instance.my_data[0]).name == "X-wing"


@pytest.mark.django_db
def test_model_utils_during():
    jul_start = datetime(2026, 7, 1, tzinfo=UTC)
    aug_start = datetime(2026, 8, 1, tzinfo=UTC)
    jul_pub = Publication.objects.create(
        pubmed_id=1, title="Published in July", metadata={}
    )
    aug_pub = Publication.objects.create(
        pubmed_id=2, title="Published in August", metadata={}
    )
    Publication.objects.filter(pk=jul_pub.pk).update(updated_at=jul_start)
    Publication.objects.filter(pk=aug_pub.pk).update(updated_at=aug_start)

    assert list(Publication.objects.filter(during(jul_start, aug_start))) == [jul_pub]
@pytest.mark.django_db(transaction=True)
@isolate_apps("emgapiv2")
def test_suppression_following_foreign_key_propagates_through_suppressed_models():
    class SuppressionSource(models.Model):
        is_suppressed = models.BooleanField(default=False)

        class Meta:
            app_label = "emgapiv2"

    class SuppressionIntermediate(models.Model):
        source = SuppressionFollowingForeignKey(
            SuppressionSource, on_delete=models.CASCADE
        )
        is_suppressed = models.BooleanField(default=False)

        class Meta:
            app_label = "emgapiv2"

    class SuppressionLeaf(models.Model):
        intermediate = SuppressionFollowingForeignKey(
            SuppressionIntermediate, on_delete=models.CASCADE
        )
        is_suppressed = models.BooleanField(default=False)

        class Meta:
            app_label = "emgapiv2"

    test_models = (SuppressionSource, SuppressionIntermediate, SuppressionLeaf)
    with connection.schema_editor() as schema_editor:
        for model in test_models:
            schema_editor.create_model(model)

    try:
        source = SuppressionSource.objects.create(is_suppressed=True)
        unsuppressed_intermediate = SuppressionIntermediate.objects.create(
            source=source
        )
        suppressed_intermediate = SuppressionIntermediate.objects.create(source=source)
        leaves = [
            SuppressionLeaf.objects.create(intermediate=unsuppressed_intermediate),
            SuppressionLeaf.objects.create(intermediate=suppressed_intermediate),
        ]
        SuppressionIntermediate.objects.filter(pk=suppressed_intermediate.pk).update(
            is_suppressed=True
        )

        SuppressionFollowingRelation.propagate_from(source)

        assert not SuppressionIntermediate.objects.filter(is_suppressed=False).exists()
        assert not SuppressionLeaf.objects.filter(
            pk__in=[leaf.pk for leaf in leaves], is_suppressed=False
        ).exists()

        source.is_suppressed = False
        source.save(update_fields=["is_suppressed"])

        assert not SuppressionIntermediate.objects.filter(is_suppressed=True).exists()
        assert not SuppressionLeaf.objects.filter(
            pk__in=[leaf.pk for leaf in leaves], is_suppressed=True
        ).exists()
    finally:
        with connection.schema_editor() as schema_editor:
            for model in reversed(test_models):
                schema_editor.delete_model(model)


def test_enum_stringification():
    class MyEnum(FutureStrEnum):
        HELLO = "hello"
        WORLD = "world"

    assert str(MyEnum.HELLO) == "hello"
    assert str(MyEnum.HELLO.value) == "hello"


def test_dict_utils_some():
    assert some({"planet": "world", "message": "hello"}, {"planet", "message"}) == {
        "planet": "world",
        "message": "hello",
    }
    assert some({1: 1, 2: 2, 3: 3}, {1, 2}) == {1: 1, 2: 2}
    assert some({1: 1, 2: 2, 3: 3}, {1, 2, 3, 4}) == {1: 1, 2: 2, 3: 3}
    assert some({1: 1, 2: 2, 3: 3}, {1, 2, 3, 4}, default=None) == {
        1: 1,
        2: 2,
        3: 3,
        4: None,
    }
    assert some({}, {1}) == {}
    assert some({}, {1}, None) == {1: None}


def test_dict_utils_add():
    assert add({1: 1}, {2: 2}) == {1: 1, 2: 2}
    assert add({1: 1}, {1: 2}) == {1: 2}
