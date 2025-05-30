import django
import pytest

django.setup()

import analyses.models as mg_models


@pytest.fixture
def top_level_biomes():
    biomes_list = [
        {"path": "root", "biome_name": "root"},
        {"path": "root.host_associated", "biome_name": "Host-Associated"},
        {"path": "root.engineered", "biome_name": "Engineered"},
        {"path": "root.host_associated.human", "biome_name": "Human"},
    ]
    biomes_objects = []
    for biome in biomes_list:
        print(f"creating biome {biome['path']}")
        biomes_objects.append(
            mg_models.Biome.objects.get_or_create(
                path=biome["path"], biome_name=biome["biome_name"]
            )[0]
        )
    return biomes_objects
