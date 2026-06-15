from typing import Optional

import pytest
from pydantic import BaseModel

from .models import KeyValueStore
from .schemas import CogCategories


class DemoPydanticModel(BaseModel):
    category: str
    description: Optional[str]
    count: int


@pytest.mark.django_db(transaction=True)
def test_fixture_loaded(cog_categories_fixture):
    cog_cats = KeyValueStore.get_model("cog_category_description_map", CogCategories)
    assert len(cog_cats.root) == 26
    assert cog_cats.root["A"] == "RNA processing and modification"


@pytest.mark.django_db(transaction=True)
def test_set_and_get_model():
    key = "test_key"
    data = DemoPydanticModel(
        category="C", count=30, description="A category of constellations"
    )

    # Test set_model
    KeyValueStore.set_model(key, data)

    # Verify object exists in DB
    obj = KeyValueStore.objects.get(key=key)
    assert obj.value["category"] == "C"

    # Test get_model
    retrieved_data = KeyValueStore.get_model(key, DemoPydanticModel)
    assert isinstance(retrieved_data, DemoPydanticModel)
    assert retrieved_data.category == "C"
    assert retrieved_data.count == 30


@pytest.mark.django_db(transaction=True)
def test_update_model():
    key = "update_key"
    data1 = DemoPydanticModel(category="C", count=30, description=None)
    KeyValueStore.set_model(key, data1)

    data2 = DemoPydanticModel(category="C", count=40, description=None)
    KeyValueStore.set_model(key, data2)

    retrieved_data = KeyValueStore.get_model(key, DemoPydanticModel)
    assert retrieved_data.count == 40
