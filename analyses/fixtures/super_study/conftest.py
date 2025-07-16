from pathlib import Path

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile

from analyses.models import SuperStudy, SuperStudyStudy


@pytest.fixture
def super_study(raw_reads_mgnify_study):
    logo_path = Path(__file__).parent / "logo.png"
    with logo_path.open("rb") as f:
        logo = SimpleUploadedFile("logo.png", f.read(), content_type="image/png")
    super_study = SuperStudy.objects.create(
        slug="test-super-study",
        title="Test Super Study",
        description="A test super study for API testing",
        logo=logo,
    )
    SuperStudyStudy.objects.create(
        super_study=super_study,
        study=raw_reads_mgnify_study,
        is_flagship=True,
    )
    return super_study
