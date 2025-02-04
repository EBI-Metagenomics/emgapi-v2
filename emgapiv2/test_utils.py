import pytest
from django.core.exceptions import ValidationError
from django.db import models
from pydantic import BaseModel

from emgapiv2.async_utils import anysync_property
from emgapiv2.enum_utils import FutureStrEnum
from emgapiv2.log_utils import mask_sensitive_data
from emgapiv2.model_utils import JSONFieldWithSchema


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
    assert (
        mask_sensitive_data(script)
        == """
    ./run-command subcommand1 -flag=okay -password=*****
    ./run-command subcommand2 -flag=okay -password=*****
    """
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


def test_enum_stringification():
    class MyEnum(FutureStrEnum):
        HELLO = "hello"
        WORLD = "world"

    assert str(MyEnum.HELLO) == "hello"
    assert str(MyEnum.HELLO.value) == "hello"
