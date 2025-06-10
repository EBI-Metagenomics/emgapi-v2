from enum import Enum

import pytest

from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices


@pytest.fixture
def biome_choices(top_level_biomes):
    return Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})


@pytest.fixture
def user_choices(admin_user):
    return get_users_as_choices()
