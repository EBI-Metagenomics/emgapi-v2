import django
import pytest

django.setup()

import analyses.models as mg_models

versions = {
    "metaspades": "3.15.5",
    "spades": "3.15.5",
    "megahit": "1.2.9",
    "flye": "2.9.5",
}


@pytest.fixture
def assemblers():
    for name, label in mg_models.Assembler.NAME_CHOICES:
        mg_models.Assembler.objects.get_or_create(name=name, version=versions[name])
    return mg_models.Assembler.objects.all()
